"""Consulta periódica (polling) e reconciliação.

Consultor (cenário A; critérios C2 e C15):
  a cada T segundos faz uma rodada de consulta ao provedor, em um de dois modos:
    "incremental": pede só o que veio depois do cursor (P = 1 se couber numa
                   página); o cursor só avança se a rodada inteira der certo;
    "janela":      relê todas as páginas do estado atual (P = ⌈M/m⌉), como o
                   caso real faz com os carrinhos da Yampi.
  Se qualquer requisição da rodada falhar (429, 5xx, rede), a rodada é
  abandonada e nada avança: a próxima recomeça do mesmo ponto. Eventos já
  detectados não são contados de novo (conjunto de identificadores vistos).

Reconciliador (cenário G; critério C3; hipótese H3):
  a cada T_R segundos relê a janela do provedor e aplica cada registro ao
  estado local do receptor. Se o registro estava ausente ou desatualizado
  (porque o webhook se perdeu), o estado é corrigido e o registro conta como
  recuperado. A completude é medida pelo estado: ao final, cada carrinho deve
  ter localmente a mesma versão que tem no provedor.
"""

from __future__ import annotations

import asyncio
import time
from collections import Counter
from collections.abc import Awaitable, Callable
from typing import Any

import httpx

from poc.idempotencia import EstadoPorRegistro, Resultado, ler_data

Dormir = Callable[[float], Awaitable[None]]
AoDetectar = Callable[[str, dict[str, Any], float], None]


class RodadaFalhou(Exception):
    """Alguma requisição da rodada não teve sucesso."""


class _LeitorProvedor:
    """Leitura paginada do provedor, compartilhada pelos dois componentes."""

    def __init__(self, cliente: httpx.AsyncClient, empresa: str,
                 aplicativo: str, por_pagina: int) -> None:
        self.cliente = cliente
        self.empresa = empresa
        self.aplicativo = aplicativo
        self.por_pagina = por_pagina
        self.contadores: Counter[str] = Counter()

    async def _get(self, rota: str, params: dict[str, Any]) -> dict[str, Any]:
        self.contadores["requisicoes"] += 1
        t0 = time.monotonic()
        try:
            r = await self.cliente.get(rota, params=params,
                                       headers={"x-app": self.aplicativo})
            # soma dos tempos de resposta (µs): r̄ = soma / requisições (H1)
            self.contadores["tempo_resposta_total_us"] += int(
                (time.monotonic() - t0) * 1e6)
        except httpx.TransportError as erro:
            self.contadores["falhas_rede"] += 1
            raise RodadaFalhou(str(erro)) from erro
        if r.status_code == 429:
            self.contadores["respostas_429"] += 1
            raise RodadaFalhou("cota excedida")
        if r.status_code != 200:
            self.contadores["respostas_erro"] += 1
            raise RodadaFalhou(f"status {r.status_code}")
        return r.json()

    async def ler_janela(self) -> list[dict[str, Any]]:
        itens: list[dict[str, Any]] = []
        pagina, total = 1, 1
        while pagina <= total:
            dados = await self._get("/v1/carrinhos", {
                "empresa": self.empresa, "pagina": pagina,
                "por_pagina": self.por_pagina})
            itens.extend(dados["itens"])
            total = dados["total_paginas"]
            pagina += 1
        return itens

    async def ler_incremental(self, cursor: int) -> tuple[list[dict[str, Any]], int]:
        itens: list[dict[str, Any]] = []
        while True:
            dados = await self._get("/v1/eventos", {
                "empresa": self.empresa, "cursor": cursor,
                "limite": self.por_pagina})
            itens.extend(dados["itens"])
            cursor = dados["proximo_cursor"]
            if not dados["tem_mais"]:
                return itens, cursor


async def _executar_periodicamente(rodada: Callable[[], Awaitable[None]],
                                   intervalo_s: float, duracao_s: float,
                                   relogio: Callable[[], float],
                                   dormir: Dormir) -> int:
    """Executa 'rodada' em instantes fixos (início + k·intervalo), sem deriva.

    Se a máquina não der conta do ritmo, as rodadas começam atrasadas; o maior
    atraso é devolvido (em ms) para conferir a fidelidade da execução.
    """
    inicio = relogio()
    k = 0
    atraso_max = 0.0
    while True:
        alvo = inicio + k * intervalo_s
        if alvo - inicio >= duracao_s:
            return int(atraso_max * 1000)
        espera = alvo - relogio()
        if espera > 0:
            await dormir(espera)
        else:
            atraso_max = max(atraso_max, -espera)
        await rodada()
        k += 1


class Consultor(_LeitorProvedor):
    def __init__(self, cliente: httpx.AsyncClient, empresa: str,
                 intervalo_s: float, modo: str = "incremental",
                 por_pagina: int = 100, aplicativo: str = "painel",
                 ao_detectar: AoDetectar | None = None,
                 relogio: Callable[[], float] = time.monotonic,
                 dormir: Dormir = asyncio.sleep) -> None:
        if modo not in ("incremental", "janela"):
            raise ValueError("modo deve ser 'incremental' ou 'janela'")
        super().__init__(cliente, empresa, aplicativo, por_pagina)
        self.intervalo_s = intervalo_s
        self.modo = modo
        self.cursor = 0
        self.vistos: set[str] = set()
        self._ao_detectar = ao_detectar
        self._relogio = relogio
        self._dormir = dormir

    async def rodada(self) -> int:
        """Uma rodada de consulta. Devolve quantos eventos novos detectou."""
        try:
            if self.modo == "incremental":
                itens, novo_cursor = await self.ler_incremental(self.cursor)
            else:
                itens, novo_cursor = await self.ler_janela(), self.cursor
        except RodadaFalhou:
            self.contadores["rodadas_falhas"] += 1
            return 0
        self.cursor = novo_cursor  # só depois de a rodada inteira dar certo
        self.contadores["rodadas_ok"] += 1
        agora = self._relogio()
        novos = 0
        for ev in itens:
            if ev["id"] in self.vistos:
                continue
            self.vistos.add(ev["id"])
            novos += 1
            if self._ao_detectar:
                self._ao_detectar(self.empresa, ev, agora)
        self.contadores["detectados"] += novos
        return novos

    async def executar(self, duracao_s: float) -> None:
        async def _rodada() -> None:
            await self.rodada()
        self.contadores["atraso_max_ms"] = await _executar_periodicamente(
            _rodada, self.intervalo_s, duracao_s, self._relogio, self._dormir)


class Reconciliador(_LeitorProvedor):
    def __init__(self, cliente: httpx.AsyncClient, empresa: str,
                 estado: EstadoPorRegistro, intervalo_s: float,
                 por_pagina: int = 100, aplicativo: str = "painel",
                 ao_recuperar: AoDetectar | None = None,
                 relogio: Callable[[], float] = time.monotonic,
                 dormir: Dormir = asyncio.sleep) -> None:
        super().__init__(cliente, empresa, aplicativo, por_pagina)
        self.estado = estado
        self.intervalo_s = intervalo_s
        self._ao_recuperar = ao_recuperar
        self._relogio = relogio
        self._dormir = dormir

    async def reconciliar(self) -> int:
        """Relê a janela e corrige o estado local. Devolve quantos recuperou."""
        try:
            itens = await self.ler_janela()
        except RodadaFalhou:
            self.contadores["rodadas_falhas"] += 1
            return 0
        self.contadores["rodadas_ok"] += 1
        agora = self._relogio()
        recuperados = 0
        for ev in itens:
            dados = ev["data"]
            chave = f"{self.empresa}:{dados['carrinho_id']}"
            local = self.estado.obter(chave)
            if local is not None and local.atualizado_em >= ler_data(dados["updated_at"]):
                continue  # o estado local já está em dia com o provedor
            if self.estado.aplicar(chave, dados["updated_at"], dados) is Resultado.APLICADO:
                recuperados += 1  # estava ausente ou desatualizado
                if self._ao_recuperar:
                    self._ao_recuperar(self.empresa, ev, agora)
        self.contadores["recuperados"] += recuperados
        return recuperados

    async def executar(self, duracao_s: float) -> None:
        async def _rodada() -> None:
            await self.reconciliar()
        self.contadores["atraso_max_ms"] = await _executar_periodicamente(
            _rodada, self.intervalo_s, duracao_s, self._relogio, self._dormir)

"""Provedor simulado (o papel da plataforma de comércio eletrônico).

Três partes, separadas para facilitar teste e reutilização:

- intervalos_poisson: gera os intervalos entre eventos de um processo de
  Poisson, com semente fixa. Eventos em instantes aleatórios em relação ao
  relógio das consultas são a condição para a espera média do polling ser T/2
  (hipótese H1, Seção 3.6).

- Provedor: guarda os eventos de cada empresa e responde às duas formas de
  consulta do experimento:
    * incremental por cursor (só o que é novo; P = 1 quando cabe numa página);
    * releitura de janela paginada (relê todo o histórico recente; P > 1),
      como o caso real faz com os carrinhos da Yampi (termo P da Eq. de custo).
  Cada evento só fica visível D segundos depois de ocorrer (atraso de
  detecção na origem, parcela D da latência).

- CotaPorAplicativo: limita as requisições de um aplicativo a Q por janela de
  tempo e responde 429 com Retry-After quando estoura (critério C15; cenário F).

criar_app_provedor expõe tudo isso por HTTP (FastAPI).
"""

from __future__ import annotations

import math
import random
import threading
import time
from collections import Counter, deque
from collections.abc import Callable, Iterator
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any

from fastapi import FastAPI, Header, Query
from fastapi.responses import JSONResponse

Relogio = Callable[[], float]


def intervalos_poisson(taxa_por_s: float, semente: int) -> Iterator[float]:
    """Intervalos (s) entre eventos de um processo de Poisson de taxa λ."""
    if taxa_por_s <= 0:
        raise ValueError("a taxa deve ser positiva")
    rng = random.Random(semente)
    while True:
        yield rng.expovariate(taxa_por_s)


def iso_utc(instante: float) -> str:
    return datetime.fromtimestamp(instante, timezone.utc).isoformat(
        timespec="milliseconds").replace("+00:00", "Z")


@dataclass
class Evento:
    seq: int                 # posição na sequência da empresa (cursor)
    id: str
    empresa: str
    tipo: str                # "carrinho.abandonado" | "pedido.pago"
    ocorrido_em: float       # relógio monotônico (para medir latência)
    visivel_em: float        # ocorrido_em + D
    dados: dict[str, Any]

    def corpo(self) -> dict[str, Any]:
        """Representação enviada no webhook e devolvida nas consultas."""
        return {"id": self.id, "type": self.tipo, "tenant": self.empresa,
                "data": self.dados}


class Provedor:
    def __init__(self, atraso_deteccao_s: float = 0.0,
                 relogio: Relogio = time.monotonic,
                 relogio_parede: Relogio = time.time,
                 tamanho_janela: int | None = None) -> None:
        """tamanho_janela: quantos carrinhos (os mais recentes) a releitura
        devolve, como a janela de 90 dias do caso; None = todos. Mantém P
        constante ao longo do experimento."""
        self.atraso_deteccao_s = atraso_deteccao_s
        self.tamanho_janela = tamanho_janela
        self._relogio = relogio
        self._parede = relogio_parede
        self._eventos: dict[str, list[Evento]] = {}
        self._trava = threading.Lock()
        self._contador_ids = 0

    def criar_evento(self, empresa: str, tipo: str, carrinho_id: str,
                     status: str, itens: int = 1) -> Evento:
        with self._trava:
            self._contador_ids += 1
            lista = self._eventos.setdefault(empresa, [])
            agora = self._relogio()
            ev = Evento(
                seq=len(lista) + 1,
                id=f"evt_{self._contador_ids:08d}",
                empresa=empresa, tipo=tipo,
                ocorrido_em=agora,
                visivel_em=agora + self.atraso_deteccao_s,
                dados={"carrinho_id": carrinho_id, "status": status,
                       "itens": itens, "updated_at": iso_utc(self._parede())},
            )
            lista.append(ev)
            return ev

    def _visiveis(self, empresa: str) -> list[Evento]:
        agora = self._relogio()
        return [e for e in self._eventos.get(empresa, [])
                if e.visivel_em <= agora]

    def listar_incremental(self, empresa: str, cursor: int = 0,
                           limite: int = 100) -> dict[str, Any]:
        """Eventos visíveis com seq > cursor, em ordem, até 'limite'."""
        with self._trava:
            novos = [e for e in self._visiveis(empresa) if e.seq > cursor]
        pagina = novos[:limite]
        proximo = pagina[-1].seq if pagina else cursor
        return {"itens": [e.corpo() for e in pagina],
                "proximo_cursor": proximo, "tem_mais": len(novos) > limite}

    def listar_janela(self, empresa: str, pagina: int = 1,
                      por_pagina: int = 100) -> dict[str, Any]:
        """Estado atual de cada carrinho visível, paginado (releitura total)."""
        with self._trava:
            atual: dict[str, Evento] = {}
            for e in self._visiveis(empresa):
                atual[e.dados["carrinho_id"]] = e  # o mais recente vence
        registros = sorted(atual.values(), key=lambda e: e.seq)
        if self.tamanho_janela is not None:
            registros = registros[-self.tamanho_janela:]
        total_paginas = max(1, math.ceil(len(registros) / por_pagina))
        inicio = (pagina - 1) * por_pagina
        trecho = registros[inicio:inicio + por_pagina]
        return {"itens": [e.corpo() for e in trecho], "pagina": pagina,
                "total_paginas": total_paginas, "total": len(registros)}

    def estado_atual(self, empresa: str) -> dict[str, dict[str, Any]]:
        """Carrinho -> dados mais recentes, sem janela nem atraso D (gabarito
        usado para medir a completude no cenário híbrido)."""
        with self._trava:
            estado: dict[str, dict[str, Any]] = {}
            for e in self._eventos.get(empresa, []):
                estado[e.dados["carrinho_id"]] = e.dados
            return estado


class CotaPorAplicativo:
    """Até 'limite' requisições por 'janela_s' segundos para cada aplicativo."""

    def __init__(self, limite: int | None, janela_s: float = 60.0,
                 relogio: Relogio = time.monotonic) -> None:
        self.limite = limite  # None: sem cota
        self.janela_s = janela_s
        self._relogio = relogio
        self._uso: dict[str, deque[float]] = {}
        self._trava = threading.Lock()

    def consumir(self, aplicativo: str) -> float | None:
        """Registra uma requisição. Devolve None se permitida, ou os segundos
        até liberar uma vaga (valor do Retry-After) se a cota estourou."""
        if self.limite is None:
            return None
        with self._trava:
            agora = self._relogio()
            fila = self._uso.setdefault(aplicativo, deque())
            while fila and agora - fila[0] >= self.janela_s:
                fila.popleft()
            if len(fila) >= self.limite:
                return self.janela_s - (agora - fila[0])
            fila.append(agora)
            return None


@dataclass
class ConfigProvedor:
    provedor: Provedor = field(default_factory=Provedor)
    cota: CotaPorAplicativo = field(default_factory=lambda: CotaPorAplicativo(None))


def criar_app_provedor(config: ConfigProvedor) -> FastAPI:
    app = FastAPI(title="Provedor simulado (PoC)")
    contadores: Counter[str] = Counter()
    assinaturas: dict[str, dict[str, str]] = {}  # empresa -> {url, segredo}
    app.state.contadores = contadores
    app.state.assinaturas = assinaturas
    prov = config.provedor

    def checar_cota(aplicativo: str) -> JSONResponse | None:
        contadores["requisicoes_consulta"] += 1
        espera = config.cota.consumir(aplicativo)
        if espera is None:
            return None
        contadores["respostas_429"] += 1
        return JSONResponse({"status": "cota_excedida"}, status_code=429,
                            headers={"Retry-After": str(math.ceil(espera))})

    @app.get("/v1/eventos")
    async def eventos(empresa: str, cursor: int = 0,
                      limite: int = Query(100, ge=1, le=100),
                      x_app: str = Header("app-padrao")):
        if (r := checar_cota(x_app)) is not None:
            return r
        return prov.listar_incremental(empresa, cursor, limite)

    @app.get("/v1/carrinhos")
    async def carrinhos(empresa: str, pagina: int = Query(1, ge=1),
                        por_pagina: int = Query(100, ge=1, le=100),
                        x_app: str = Header("app-padrao")):
        if (r := checar_cota(x_app)) is not None:
            return r
        return prov.listar_janela(empresa, pagina, por_pagina)

    @app.post("/v1/assinaturas")
    async def assinar_webhook(dados: dict[str, str]):
        """Cadastra o destino de webhook de uma empresa (usado no módulo 5)."""
        assinaturas[dados["empresa"]] = {"url": dados["url"],
                                         "segredo": dados["segredo"]}
        return {"status": "cadastrado"}

    @app.get("/metricas")
    async def metricas() -> dict[str, int]:
        return dict(contadores)

    return app

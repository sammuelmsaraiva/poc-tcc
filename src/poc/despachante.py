"""Despachante de webhooks (o lado da origem que entrega os eventos).

Para cada evento:
  1. perda induzida: com probabilidade p o evento não é entregue de forma
     alguma, imitando a origem que desistiu ou desativou o destino. É o que
     o cenário G usa para testar se a reconciliação recupera o que se perdeu;
  2. destino desativado (opcional): depois de N falhas definitivas seguidas,
     a origem para de enviar para aquele destino, como a Yampi após 30;
  3. tentativas: cada tentativa é assinada de novo (mesmo webhook-id, horário
     atual) e enviada com prazo de resposta;
       2xx                      -> entregue;
       5xx, 429, prazo, conexão -> nova tentativa, depois de uma espera
                                   exponencial com jitter (ou Retry-After);
       outros 4xx               -> falha definitiva, sem repetir (C5);
  4. esgotadas as tentativas, o evento vai para a lista de falhas definitivas
     (o Dead Letter Channel de Hohpe e Woolf), com o motivo.
"""

from __future__ import annotations

import asyncio
import json
import random
import time
from collections import Counter
from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field

import httpx

from poc.assinatura import cabecalhos
from poc.provedor import Evento

Dormir = Callable[[float], Awaitable[None]]


@dataclass
class PoliticaRetentativa:
    """Espera exponencial: base * 2^n, limitada a teto_s.

    Com jitter=True usa o "jitter completo" descrito por Brooker (2015):
    a espera é sorteada entre 0 e o valor exponencial, o que espalha as
    novas tentativas de muitos clientes que falharam ao mesmo tempo.
    """
    max_tentativas: int = 6
    base_s: float = 1.0
    teto_s: float = 60.0
    jitter: bool = True
    semente: int | None = None
    _rng: random.Random = field(init=False, repr=False)

    def __post_init__(self) -> None:
        if self.max_tentativas < 1:
            raise ValueError("max_tentativas deve ser >= 1")
        self._rng = random.Random(self.semente)

    def espera(self, n_repeticao: int) -> float:
        """Espera antes da repetição de número n (0 = primeira repetição)."""
        limite = min(self.teto_s, self.base_s * (2 ** n_repeticao))
        return self._rng.uniform(0, limite) if self.jitter else limite


@dataclass
class ResultadoEntrega:
    evento_id: str
    empresa: str
    entregue: bool
    tentativas: int
    motivo: str               # "ok", "perda_induzida", "4xx", "esgotou", ...
    status_final: int | None
    inicio: float             # relógio monotônico do primeiro envio
    fim: float                # relógio monotônico da última resposta


@dataclass
class Destino:
    url: str
    segredo: str


class Despachante:
    def __init__(self, cliente: httpx.AsyncClient,
                 politica: PoliticaRetentativa | None = None,
                 prazo_s: float = 5.0,
                 prob_perda: float = 0.0,
                 semente_perda: int | None = None,
                 desativar_apos: int | None = None,
                 dormir: Dormir = asyncio.sleep,
                 relogio: Callable[[], float] = time.monotonic,
                 ao_concluir: Callable[[ResultadoEntrega], None] | None = None,
                 ) -> None:
        if not 0.0 <= prob_perda <= 1.0:
            raise ValueError("prob_perda deve estar entre 0 e 1")
        self.cliente = cliente
        self.politica = politica or PoliticaRetentativa()
        self.prazo_s = prazo_s
        self.prob_perda = prob_perda
        self._rng_perda = random.Random(semente_perda)
        self.desativar_apos = desativar_apos
        self._dormir = dormir
        self._relogio = relogio
        self._ao_concluir = ao_concluir
        self.contadores: Counter[str] = Counter()
        self.falhas_definitivas: list[ResultadoEntrega] = []
        self._falhas_seguidas: Counter[str] = Counter()  # por empresa (destino)
        self.desativados: set[str] = set()

    def _concluir(self, r: ResultadoEntrega) -> ResultadoEntrega:
        if r.entregue:
            self.contadores["entregues"] += 1
            self._falhas_seguidas[r.empresa] = 0
        else:
            self.contadores[f"nao_entregue_{r.motivo}"] += 1
            if r.motivo not in ("perda_induzida", "destino_desativado"):
                self.falhas_definitivas.append(r)
                self._falhas_seguidas[r.empresa] += 1
                if (self.desativar_apos is not None and
                        self._falhas_seguidas[r.empresa] >= self.desativar_apos):
                    self.desativados.add(r.empresa)
        if self._ao_concluir:
            self._ao_concluir(r)
        return r

    async def entregar(self, evento: Evento, destino: Destino) -> ResultadoEntrega:
        agora = self._relogio()
        base = dict(evento_id=evento.id, empresa=evento.empresa, inicio=agora)

        if evento.empresa in self.desativados:
            return self._concluir(ResultadoEntrega(
                entregue=False, tentativas=0, motivo="destino_desativado",
                status_final=None, fim=agora, **base))
        if self.prob_perda and self._rng_perda.random() < self.prob_perda:
            return self._concluir(ResultadoEntrega(
                entregue=False, tentativas=0, motivo="perda_induzida",
                status_final=None, fim=agora, **base))

        corpo = json.dumps(evento.corpo(), separators=(",", ":")).encode()
        status: int | None = None
        motivo = "esgotou"
        for n in range(self.politica.max_tentativas):
            self.contadores["tentativas"] += 1
            h = cabecalhos(destino.segredo, evento.id, corpo)
            h["content-type"] = "application/json"
            espera_servidor: float | None = None
            try:
                # wait_for garante o prazo mesmo em transportes que não aplicam
                # timeout de rede (ex.: ASGI em memória, usado nos testes).
                resp = await asyncio.wait_for(
                    self.cliente.post(destino.url, content=corpo, headers=h,
                                      timeout=self.prazo_s),
                    timeout=self.prazo_s)
                status = resp.status_code
                if 200 <= status < 300:
                    return self._concluir(ResultadoEntrega(
                        entregue=True, tentativas=n + 1, motivo="ok",
                        status_final=status, fim=self._relogio(), **base))
                if 400 <= status < 500 and status != 429:
                    motivo = "4xx"  # erro de quem envia: repetir não adianta
                    break
                if status == 429 and "retry-after" in resp.headers:
                    espera_servidor = float(resp.headers["retry-after"])
                motivo = "esgotou"
            except (httpx.TimeoutException, asyncio.TimeoutError):
                status, motivo = None, "esgotou_prazo"
            except httpx.TransportError:
                status, motivo = None, "esgotou_conexao"
            if n + 1 < self.politica.max_tentativas:
                self.contadores["repeticoes"] += 1
                await self._dormir(espera_servidor if espera_servidor is not None
                                   else self.politica.espera(n))
        return self._concluir(ResultadoEntrega(
            entregue=False, tentativas=n + 1, motivo=motivo,
            status_final=status, fim=self._relogio(), **base))

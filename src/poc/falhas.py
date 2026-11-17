"""Injeção de falhas no lado do receptor (cenário C).

Três tipos de falha, cada um com o seu ponto de injeção:

- InjetorFalhas (middleware ASGI, envolve o app do receptor):
    prob_500  -> responde 500 em uma fração das requisições (semente fixa);
    atraso_s  -> demora para responder; acima do prazo da origem, a entrega
                 estoura o tempo e é repetida. O momento do atraso importa:
        momento_atraso="antes":  espera ANTES de processar; se a origem
                                 desistir, o evento não chega a ser gravado;
        momento_atraso="depois": processa e grava, e só então demora para
                                 responder. A origem acha que falhou e repete,
                                 e o receptor recebe um DUPLICADO. É o caso
                                 típico da entrega "pelo menos uma vez".
    Por isso, no cenário C, a completude é medida pelo que o RECEPTOR
    registrou (tabela conhecimentos), e não pelo "entregue" do despachante,
    que pode ser um falso negativo.
- TransporteComQueda (transporte httpx, do lado de quem chama):
    janelas de tempo em que o destino está "fora do ar": a conexão é recusada
    (httpx.ConnectError), como acontece quando o servidor está parado.
- Falha de gravação: já existe no próprio receptor (Armazenamento(falhar=...)).
"""

from __future__ import annotations

import asyncio
import random
import time
from collections import Counter
from collections.abc import Callable

import httpx


class InjetorFalhas:
    def __init__(self, app, prob_500: float = 0.0, atraso_s: float = 0.0,
                 momento_atraso: str = "antes", semente: int | None = None,
                 rotas: tuple[str, ...] = ("/webhooks/",)) -> None:
        if not 0.0 <= prob_500 <= 1.0:
            raise ValueError("prob_500 deve estar entre 0 e 1")
        if momento_atraso not in ("antes", "depois"):
            raise ValueError("momento_atraso deve ser 'antes' ou 'depois'")
        self.momento_atraso = momento_atraso
        self.app = app
        self.prob_500 = prob_500
        self.atraso_s = atraso_s
        self.rotas = rotas  # só afeta as rotas de webhook (métricas seguem livres)
        self._rng = random.Random(semente)
        self.contadores: Counter[str] = Counter()

    async def __call__(self, scope, receive, send):
        if scope["type"] != "http" or not scope["path"].startswith(self.rotas):
            return await self.app(scope, receive, send)
        if self.atraso_s and self.momento_atraso == "depois":
            # Deixa o app processar por completo, guarda a resposta e só a
            # devolve depois do atraso.
            guardadas = []

            async def guardar(msg):
                guardadas.append(msg)

            await self.app(scope, receive, guardar)
            self.contadores["atrasadas_depois"] += 1
            await asyncio.sleep(self.atraso_s)
            for msg in guardadas:
                await send(msg)
            return
        if self.atraso_s:
            self.contadores["atrasadas"] += 1
            await asyncio.sleep(self.atraso_s)
        if self.prob_500 and self._rng.random() < self.prob_500:
            self.contadores["respostas_500"] += 1
            await send({"type": "http.response.start", "status": 500,
                        "headers": [(b"content-type", b"application/json")]})
            await send({"type": "http.response.body",
                        "body": b'{"status":"falha_injetada"}'})
            return
        return await self.app(scope, receive, send)


class TransporteComQueda(httpx.AsyncBaseTransport):
    """Recusa conexões dentro das janelas [início, fim) de indisponibilidade.

    Os instantes são relativos ao início (primeira chamada do relógio).
    """

    def __init__(self, interno: httpx.AsyncBaseTransport,
                 janelas: list[tuple[float, float]],
                 relogio: Callable[[], float] = time.monotonic) -> None:
        self.interno = interno
        self.janelas = janelas
        self._relogio = relogio
        self._t0 = relogio()
        self.contadores: Counter[str] = Counter()

    def fora_do_ar(self) -> bool:
        t = self._relogio() - self._t0
        return any(inicio <= t < fim for inicio, fim in self.janelas)

    async def handle_async_request(self, request: httpx.Request) -> httpx.Response:
        if self.fora_do_ar():
            self.contadores["conexoes_recusadas"] += 1
            raise httpx.ConnectError("destino fora do ar (injetado)", request=request)
        return await self.interno.handle_async_request(request)

    async def aclose(self) -> None:
        await self.interno.aclose()

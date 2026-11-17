"""Receptor de webhooks (o papel do painel de gestão no experimento).

Rota principal: POST /webhooks/{empresa}

Ordem do tratamento de cada requisição (cada passo ligado a um critério):
  1. empresa desconhecida -> 404;
  2. assinatura conferida com o segredo DAQUELA empresa (C10, C11, C14); se
     não conferir, a resposta é o MESMO 404 do passo 1, para que ninguém
     descubra quais empresas existem testando endereços (C14; OWASP API1:2023).
     O motivo real fica só nos contadores de /metricas;
  3. evento repetido -> 200 "duplicado", para a origem parar de repetir (C6);
  4. evento gravado; falha na gravação -> 500, para a origem repetir, e o
     identificador é "esquecido" para que a nova tentativa seja aceita;
  5. evento posto na fila e resposta 200 imediata; o processamento pesado
     (aplicar ao estado, com ou sem verificação de ordem) vem depois (C4, C7).
"""

from __future__ import annotations

import asyncio
import contextlib
import json
import sqlite3
import threading
import time
from collections import Counter
from collections.abc import Callable
from dataclasses import dataclass, field
from typing import Any

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse
from starlette.requests import ClientDisconnect

from poc.assinatura import TOLERANCIA_PADRAO_S, AssinaturaInvalida, verificar
from poc.idempotencia import EstadoPorRegistro, RegistroIdempotencia, Resultado


class FalhaGravacao(Exception):
    """Falha (real ou injetada) ao gravar o evento recebido."""


class Armazenamento:
    """Grava cada evento aceito em SQLite (corpo cru, para auditoria)."""

    def __init__(self, caminho: str = ":memory:",
                 falhar: Callable[[], bool] = lambda: False) -> None:
        self._con = sqlite3.connect(caminho, check_same_thread=False)
        self._con.execute(
            "CREATE TABLE IF NOT EXISTS eventos ("
            " empresa TEXT, msg_id TEXT, recebido_em REAL, corpo BLOB)")
        self._trava = threading.Lock()
        self._falhar = falhar  # gancho para o injetor de falhas (cenário C)

    def gravar(self, empresa: str, msg_id: str, corpo: bytes) -> None:
        if self._falhar():
            raise FalhaGravacao("falha de gravação injetada")
        with self._trava, self._con:
            self._con.execute(
                "INSERT INTO eventos VALUES (?, ?, ?, ?)",
                (empresa, msg_id, time.time(), corpo))

    def total(self) -> int:
        with self._trava:
            return self._con.execute("SELECT COUNT(*) FROM eventos").fetchone()[0]


@dataclass
class ConfigReceptor:
    segredos: dict[str, str]                       # empresa -> segredo whsec_
    tolerancia_s: float | None = TOLERANCIA_PADRAO_S  # None: não confere idade
    retencao_s: float | None = None                # None: IDs nunca expiram
    capacidade: int | None = None
    verificar_ordem: bool = True
    idempotencia: bool = True   # False só nos cenários D e E ("sem proteção")
    relogio: Callable[[], float] = time.monotonic  # para a retenção de IDs
    relogio_parede: Callable[[], float] = time.time  # para a tolerância
    # Chamado após processar cada evento (instrumentação, módulo 7).
    ao_processar: Callable[[str, dict[str, Any], Resultado], None] | None = None
    armazenamento: Armazenamento = field(default_factory=Armazenamento)


def criar_app(config: ConfigReceptor) -> FastAPI:
    registros = {e: RegistroIdempotencia(config.retencao_s, config.capacidade,
                                         config.relogio)
                 for e in config.segredos}  # um registro por empresa
    estado = EstadoPorRegistro(verificar_ordem=config.verificar_ordem)
    contadores: Counter[str] = Counter()

    @contextlib.asynccontextmanager
    async def ciclo_de_vida(app: FastAPI):
        app.state.fila = asyncio.Queue()
        tarefa = asyncio.create_task(_processar(app.state.fila))
        yield
        tarefa.cancel()

    app = FastAPI(title="Receptor de webhooks (PoC)", lifespan=ciclo_de_vida)
    app.state.contadores = contadores
    app.state.estado = estado

    async def _processar(fila: asyncio.Queue) -> None:
        while True:
            empresa, evento = await fila.get()
            try:
                dados = evento.get("data", {})
                chave = dados.get("carrinho_id") or dados.get("pedido_id")
                resultado = estado.aplicar(f"{empresa}:{chave}",
                                           dados["updated_at"], dados)
                contadores[resultado.value] += 1
                if config.ao_processar:
                    config.ao_processar(empresa, evento, resultado)
            except (KeyError, ValueError):
                contadores["processamento_invalido"] += 1
            finally:
                fila.task_done()

    def responder(status: int, motivo: str) -> JSONResponse:
        return JSONResponse({"status": motivo}, status_code=status)

    @app.post("/webhooks/{empresa}")
    async def receber(empresa: str, request: Request) -> JSONResponse:
        contadores["recebidos"] += 1
        segredo = config.segredos.get(empresa)
        if segredo is None:
            contadores["empresa_desconhecida"] += 1
            return responder(404, "nao_encontrado")

        try:
            corpo = await request.body()  # corpo cru, exatamente como chegou
        except ClientDisconnect:  # a origem desistiu antes de enviar o corpo
            contadores["origem_desistiu"] += 1
            return responder(499, "origem_desistiu")
        try:
            msg_id = verificar(segredo, request.headers, corpo,
                               tolerancia_s=config.tolerancia_s,
                               agora=config.relogio_parede())
        except AssinaturaInvalida:
            contadores["rejeitados_assinatura"] += 1
            return responder(404, "nao_encontrado")  # idêntico ao passo 1

        try:
            evento = json.loads(corpo)
        except ValueError:
            contadores["corpo_invalido"] += 1
            return responder(400, "corpo_invalido")
        if evento.get("tenant", empresa) != empresa:
            contadores["empresa_divergente"] += 1
            return responder(400, "empresa_divergente")

        registro = registros[empresa]
        if config.idempotencia and not registro.registrar_se_novo(msg_id):
            contadores["duplicados"] += 1
            return responder(200, "duplicado")

        try:
            config.armazenamento.gravar(empresa, msg_id, corpo)
        except (FalhaGravacao, sqlite3.Error):
            if config.idempotencia:
                registro.esquecer(msg_id)  # a nova tentativa precisa ser aceita
            contadores["falhas_gravacao"] += 1
            return responder(500, "falha_gravacao")

        await request.app.state.fila.put((empresa, evento))
        contadores["aceitos"] += 1
        return responder(200, "aceito")

    @app.get("/saude")
    async def saude() -> dict[str, str]:
        return {"status": "ok"}

    @app.get("/metricas")
    async def metricas() -> dict[str, int]:
        return dict(contadores)

    return app

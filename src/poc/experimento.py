"""Executor dos cenários do experimento (Seção 3.6, Quadro de cenários).

Cada cenário sobe servidores HTTP REAIS (uvicorn) na própria máquina — o
provedor e o receptor — e conversa com eles por TCP local, para que os tempos
medidos incluam a pilha de rede. Tudo roda no sistema operacional da máquina,
sem contêiner (os relógios de dentro e de fora de uma VM não se comparam).

Uso (pasta do repositório):
    uv run python -m poc.experimento --cenario h1 --repeticoes 5
    uv run python -m poc.experimento --cenario todos --rapido   # ensaio curto

--rapido divide todos os tempos por 50 e usa 20 eventos: serve para conferir
que tudo funciona antes da execução longa; seus números NÃO entram no TCC.

Cenários e hipóteses:
    h1          latência polling × webhook (T, D)                    H1
    custo       requisições × λT, P = 1 e P = 10 (duração fixa)      H2 (a)
    escala      N empresas, cota por aplicativo (duração fixa)       H2 (b, c)
    falhas      queda, 500, lento antes/depois                       C4, C5
    duplicidade entrega dupla com e sem idempotência                 H4 (c)
    seguranca   reenvio a 5 s e 20 s, adulteração, outra empresa     H4 (a, b, d)
    hibrido     webhook com perda p + reconciliação a cada T_R       H3
"""

from __future__ import annotations

import argparse
import asyncio
import base64
import contextlib
import json
import math
import os
import random
import sys
import time
from collections import Counter
from collections.abc import AsyncIterator, Callable
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any

import httpx
import uvicorn

from poc.assinatura import cabecalhos
from poc.consultor import Consultor, Reconciliador
from poc.despachante import Despachante, Destino, PoliticaRetentativa
from poc.falhas import InjetorFalhas, TransporteComQueda
from poc.instrumentacao import Registro, versao_git
from poc.provedor import (ConfigProvedor, CotaPorAplicativo, Evento, Provedor,
                          criar_app_provedor, intervalos_poisson)
from poc.receptor import ConfigReceptor, criar_app


# ------------------------------------------------------------------ infra ----

def novo_segredo(rng: random.Random) -> str:
    return "whsec_" + base64.b64encode(rng.randbytes(32)).decode()


@contextlib.asynccontextmanager
async def servidor(app) -> AsyncIterator[str]:
    """Sobe o app num uvicorn em porta livre de 127.0.0.1; devolve a URL base."""
    cfg = uvicorn.Config(app, host="127.0.0.1", port=0, log_level="warning",
                         access_log=False, lifespan="on")
    srv = uvicorn.Server(cfg)
    tarefa = asyncio.create_task(srv.serve())
    while not srv.started:
        if tarefa.done():
            tarefa.result()  # propaga o erro de inicialização
        await asyncio.sleep(0.01)
    porta = srv.servers[0].sockets[0].getsockname()[1]
    try:
        yield f"http://127.0.0.1:{porta}"
    finally:
        srv.should_exit = True
        await tarefa


@dataclass
class Execucao:
    """Contexto comum a todos os cenários de uma rodada."""
    registro: Registro
    cenario: str
    repeticao: int
    semente: int
    fator_tempo: float = 1.0   # 1/50 no modo --rapido

    def t(self, segundos: float) -> float:
        return segundos * self.fator_tempo

    def iniciar(self, parametros: dict[str, Any]) -> random.Random:
        self.registro.iniciar_execucao(
            self.cenario, self.repeticao, self.semente,
            {**parametros, "fator_tempo": self.fator_tempo},
            versao_codigo=versao_git(Path(__file__).resolve().parents[2]))
        return random.Random(self.semente)


async def gerar_eventos(prov: Provedor, empresas: list[str], taxa_por_empresa: float,
                        semente: int, ao_criar: Callable[[Evento], None],
                        n_eventos: int | None = None,
                        duracao_s: float | None = None) -> int:
    """Processo de Poisson: N empresas de taxa λ cada equivalem a um processo de
    taxa N·λ em que cada evento vai para uma empresa sorteada. Um evento por
    carrinho (carrinhos novos), condição usada na medida de completude."""
    rng = random.Random(semente + 1)
    inicio = time.monotonic()
    criados = 0
    for intervalo in intervalos_poisson(taxa_por_empresa * len(empresas), semente):
        if n_eventos is not None and criados >= n_eventos:
            break
        await asyncio.sleep(intervalo)
        if duracao_s is not None and time.monotonic() - inicio >= duracao_s:
            break
        empresa = rng.choice(empresas)
        ev = prov.criar_evento(empresa, "carrinho.abandonado", f"c_{criados:06d}",
                               "abandonado", itens=rng.randint(1, 5))
        ao_criar(ev)
        criados += 1
    return criados


class Despachos:
    """Agenda a entrega por webhook de cada evento quando ele fica visível (+D)."""

    def __init__(self, despachante: Despachante, destinos: dict[str, Destino]):
        self.despachante = despachante
        self.destinos = destinos
        self.tarefas: set[asyncio.Task] = set()

    def agendar(self, ev: Evento, vezes: int = 1) -> None:
        async def _entregar():
            espera = ev.visivel_em - time.monotonic()
            if espera > 0:
                await asyncio.sleep(espera)
            for _ in range(vezes):
                await self.despachante.entregar(ev, self.destinos[ev.empresa])
        tarefa = asyncio.create_task(_entregar())
        self.tarefas.add(tarefa)
        tarefa.add_done_callback(self.tarefas.discard)

    async def aguardar(self) -> None:
        while self.tarefas:
            await asyncio.gather(*list(self.tarefas))


def contar_conhecidos(reg: Registro, mecanismo: str) -> int:
    return len(reg.latencias(mecanismo))


async def esperar_ate(condicao: Callable[[], bool], limite_s: float) -> bool:
    fim = time.monotonic() + limite_s
    while time.monotonic() < fim:
        if condicao():
            return True
        await asyncio.sleep(0.05)
    return condicao()


def preencher_janela(prov: Provedor, empresa: str, quantidade: int) -> None:
    """Carrinhos já existentes antes da medição: com a janela do provedor
    limitada a P·m registros, a releitura custa P páginas do início ao fim."""
    for i in range(quantidade):
        prov.criar_evento(empresa, "carrinho.abandonado", f"h_{i:06d}", "abandonado")


# -------------------------------------------------------------- cenários ----

async def cenario_h1(ex: Execucao, T: float, D: float, n_eventos: int = 200,
                     lambda_T: float = 5.0) -> dict[str, Any]:
    rng = ex.iniciar({"T": T, "D": D, "n_eventos": n_eventos, "lambda_T": lambda_T})
    Tt, Dt = ex.t(T), ex.t(D)
    prov = Provedor(atraso_deteccao_s=Dt)
    segredo = novo_segredo(rng)
    receptor = criar_app(ConfigReceptor(segredos={"loja-a": segredo},
                                        ao_processar=ex.registro.gancho_receptor()))
    async with servidor(criar_app_provedor(ConfigProvedor(provedor=prov))) as url_p, \
            servidor(receptor) as url_r, \
            httpx.AsyncClient(base_url=url_p) as cli_p, httpx.AsyncClient() as cli_r:
        despachos = Despachos(Despachante(cli_r, ao_concluir=ex.registro.entrega),
                              {"loja-a": Destino(f"{url_r}/webhooks/loja-a", segredo)})
        consultor = Consultor(cli_p, "loja-a", Tt, ao_detectar=ex.registro.gancho_consultor())
        tarefa_polling = asyncio.create_task(consultor.executar(duracao_s=math.inf))

        def criado(ev: Evento) -> None:
            ex.registro.evento_criado(ev)
            despachos.agendar(ev)

        await gerar_eventos(prov, ["loja-a"], lambda_T / Tt, ex.semente, criado,
                            n_eventos=n_eventos)
        await despachos.aguardar()
        completo = await esperar_ate(
            lambda: contar_conhecidos(ex.registro, "polling") >= n_eventos
            and contar_conhecidos(ex.registro, "webhook") >= n_eventos,
            limite_s=Tt + Dt + 10)
        tarefa_polling.cancel()
        ex.registro.contadores("consultor", consultor.contadores)
        ex.registro.contadores("receptor", receptor.state.contadores)
    return {"completo": completo,
            "polling": contar_conhecidos(ex.registro, "polling"),
            "webhook": contar_conhecidos(ex.registro, "webhook")}


async def cenario_custo(ex: Execucao, lambda_T: float, P: int, T: float = 5.0,
                        duracao: float = 1000.0, por_pagina: int = 100) -> dict[str, Any]:
    rng = ex.iniciar({"lambda_T": lambda_T, "P": P, "T": T, "duracao": duracao})
    Tt, dur = ex.t(T), ex.t(duracao)
    prov = Provedor(tamanho_janela=P * por_pagina)
    if P > 1:
        preencher_janela(prov, "loja-a", P * por_pagina)
    segredo = novo_segredo(rng)
    receptor = criar_app(ConfigReceptor(segredos={"loja-a": segredo}))
    async with servidor(criar_app_provedor(ConfigProvedor(provedor=prov))) as url_p, \
            servidor(receptor) as url_r, \
            httpx.AsyncClient(base_url=url_p) as cli_p, httpx.AsyncClient() as cli_r:
        despachante = Despachante(cli_r, ao_concluir=ex.registro.entrega)
        despachos = Despachos(despachante,
                              {"loja-a": Destino(f"{url_r}/webhooks/loja-a", segredo)})
        modo = "incremental" if P == 1 else "janela"
        consultor = Consultor(cli_p, "loja-a", Tt, modo=modo, por_pagina=por_pagina)
        tarefa_polling = asyncio.create_task(consultor.executar(duracao_s=dur))

        def criado(ev: Evento) -> None:
            ex.registro.evento_criado(ev)
            despachos.agendar(ev)

        eventos = await gerar_eventos(prov, ["loja-a"], lambda_T / Tt, ex.semente,
                                      criado, duracao_s=dur)
        await tarefa_polling
        await despachos.aguardar()
        ex.registro.contadores("consultor", consultor.contadores)
        ex.registro.contadores("despachante", despachante.contadores)
    return {"eventos": eventos,
            "req_polling": consultor.contadores["requisicoes"],
            "req_webhook": despachante.contadores["tentativas"]}


async def cenario_escala(ex: Execucao, N: int, P: int, T: float = 5.0,
                         lambda_T: float = 1.0, cota_por_min: int = 500,
                         duracao: float = 1000.0, por_pagina: int = 100) -> dict[str, Any]:
    rng = ex.iniciar({"N": N, "P": P, "T": T, "lambda_T": lambda_T,
                      "cota_por_min": cota_por_min, "duracao": duracao})
    Tt, dur = ex.t(T), ex.t(duracao)
    empresas = [f"loja-{i:03d}" for i in range(N)]
    prov = Provedor(tamanho_janela=P * por_pagina)
    if P > 1:
        for e in empresas:
            preencher_janela(prov, e, P * por_pagina)
    # A cota é por aplicativo e por minuto REAL; no modo rápido a janela da cota
    # encolhe junto com os demais tempos, preservando a razão N·P/T diante de Q.
    cota = CotaPorAplicativo(cota_por_min, janela_s=ex.t(60.0))
    segredos = {e: novo_segredo(rng) for e in empresas}
    receptor = criar_app(ConfigReceptor(segredos=segredos))
    async with servidor(criar_app_provedor(ConfigProvedor(provedor=prov, cota=cota))) as url_p, \
            servidor(receptor) as url_r, \
            httpx.AsyncClient(base_url=url_p, limits=httpx.Limits(max_connections=200)) as cli_p, \
            httpx.AsyncClient(limits=httpx.Limits(max_connections=200)) as cli_r:
        despachante = Despachante(cli_r)
        despachos = Despachos(despachante, {e: Destino(f"{url_r}/webhooks/{e}", s)
                                            for e, s in segredos.items()})
        modo = "incremental" if P == 1 else "janela"
        consultores = [Consultor(cli_p, e, Tt, modo=modo, por_pagina=por_pagina,
                                 aplicativo="painel") for e in empresas]
        tarefas = [asyncio.create_task(c.executar(duracao_s=dur)) for c in consultores]

        def criado(ev: Evento) -> None:
            despachos.agendar(ev)

        await gerar_eventos(prov, empresas, lambda_T / Tt, ex.semente, criado,
                            duracao_s=dur)
        await asyncio.gather(*tarefas)
        await despachos.aguardar()
        total = sum((Counter({k: v for k, v in c.contadores.items()
                              if k != "atraso_max_ms"}) for c in consultores),
                    start=Counter())
        ex.registro.contadores("consultores", total)
        ex.registro.contadores("despachante", despachante.contadores)
    # O webhook não passa pela API de consulta, logo não consome a cota: as
    # respostas 429 vistas pelo despachante (se houvesse) estariam aqui.
    # atraso_max_ms: se as rodadas começaram muito depois do horário, a máquina
    # não sustentou a carga e a contagem de 429 subestima o efeito da cota.
    atraso = max(c.contadores["atraso_max_ms"] for c in consultores)
    ex.registro.contadores("fidelidade", {"atraso_max_ms": atraso})
    return {"req_polling": total["requisicoes"], "polling_429": total["respostas_429"],
            "rodadas_falhas": total["rodadas_falhas"], "atraso_max_ms": atraso,
            "fiel": atraso < ex.t(T) * 1000 * 0.2,
            "webhook_429": despachante.contadores["respostas_429"]}


async def cenario_falhas(ex: Execucao, variante: str, n_eventos: int = 200,
                         taxa_por_s: float = 2.0) -> dict[str, Any]:
    rng = ex.iniciar({"variante": variante, "n_eventos": n_eventos, "taxa_por_s": taxa_por_s})
    segredo = novo_segredo(rng)
    receptor = criar_app(ConfigReceptor(segredos={"loja-a": segredo},
                                        ao_processar=ex.registro.gancho_receptor()))
    prazo = ex.t(5.0)
    app_r = receptor
    if variante == "500":
        app_r = InjetorFalhas(receptor, prob_500=0.2, semente=ex.semente)
    elif variante in ("lento_antes", "lento_depois"):
        app_r = InjetorFalhas(receptor, atraso_s=prazo * 1.2,
                              momento_atraso=variante.split("_")[1], semente=ex.semente)
    elif variante != "queda":
        raise ValueError(f"variante desconhecida: {variante}")
    prov = Provedor()
    async with servidor(app_r) as url_r:
        transporte: httpx.AsyncBaseTransport = httpx.AsyncHTTPTransport()
        if variante == "queda":
            transporte = TransporteComQueda(transporte, janelas=[(0.0, ex.t(20.0))])
        async with httpx.AsyncClient(transport=transporte) as cli_r:
            politica = PoliticaRetentativa(base_s=ex.t(1.0), teto_s=ex.t(60.0),
                                           semente=ex.semente)
            despachante = Despachante(cli_r, politica, prazo_s=prazo,
                                      ao_concluir=ex.registro.entrega)
            despachos = Despachos(despachante,
                                  {"loja-a": Destino(f"{url_r}/webhooks/loja-a", segredo)})

            def criado(ev: Evento) -> None:
                ex.registro.evento_criado(ev)
                despachos.agendar(ev)

            await gerar_eventos(prov, ["loja-a"], taxa_por_s / ex.fator_tempo,
                                ex.semente, criado, n_eventos=n_eventos)
            await despachos.aguardar()
            await asyncio.sleep(ex.t(1.0))  # deixa a fila do receptor esvaziar
        ex.registro.contadores("despachante", despachante.contadores)
        ex.registro.contadores("receptor", receptor.state.contadores)
    return {"entregues_origem": despachante.contadores["entregues"],
            "conhecidos_receptor": contar_conhecidos(ex.registro, "webhook"),
            "duplicados": receptor.state.contadores["duplicados"]}


async def cenario_duplicidade(ex: Execucao, idempotencia: bool, n_eventos: int = 200,
                              taxa_por_s: float = 2.0) -> dict[str, Any]:
    rng = ex.iniciar({"idempotencia": idempotencia, "n_eventos": n_eventos})
    segredo = novo_segredo(rng)
    receptor = criar_app(ConfigReceptor(segredos={"loja-a": segredo},
                                        idempotencia=idempotencia))
    prov = Provedor()
    async with servidor(receptor) as url_r, httpx.AsyncClient() as cli_r:
        despachante = Despachante(cli_r)
        despachos = Despachos(despachante,
                              {"loja-a": Destino(f"{url_r}/webhooks/loja-a", segredo)})
        await gerar_eventos(prov, ["loja-a"], taxa_por_s / ex.fator_tempo, ex.semente,
                            lambda ev: despachos.agendar(ev, vezes=2), n_eventos=n_eventos)
        await despachos.aguardar()
        await asyncio.sleep(ex.t(1.0))
        ex.registro.contadores("receptor", receptor.state.contadores)
    aceitos = receptor.state.contadores["aceitos"]
    return {"eventos": n_eventos, "aceitos": aceitos,
            "processamentos_duplicados": aceitos - n_eventos}


CONFIGS_SEGURANCA = {
    # nome: (idempotencia, retencao_s, tolerancia_s)  -- tempos antes da escala
    "sem_protecao": (False, None, None),
    "idempotencia_retencao_12s": (True, 12.0, None),
    "idempotencia_sem_expiracao": (True, None, None),
    "idempotencia_e_tolerancia_10s": (True, 12.0, 10.0),
}


async def cenario_seguranca(ex: Execucao, config: str, verificar_ordem: bool = True,
                            n: int = 100) -> dict[str, Any]:
    # Este cenário usa SEMPRE tempos reais (≈ 21 s por configuração), mesmo no
    # modo rápido: o horário assinado tem resolução de 1 s, e tolerâncias
    # menores que isso não seriam medidas corretamente.
    ex = Execucao(ex.registro, ex.cenario, ex.repeticao, ex.semente, 1.0)
    rng = ex.iniciar({"config": config, "verificar_ordem": verificar_ordem, "n": n})
    idem, retencao, tolerancia = CONFIGS_SEGURANCA[config]
    segredos = {"loja-a": novo_segredo(rng), "loja-b": novo_segredo(rng)}
    receptor = criar_app(ConfigReceptor(
        segredos=segredos, idempotencia=idem, verificar_ordem=verificar_ordem,
        retencao_s=ex.t(retencao) if retencao else None,
        tolerancia_s=ex.t(tolerancia) if tolerancia else None))
    url_a = "/webhooks/loja-a"
    resultados: dict[str, int] = {}

    def corpo(i: int, status: str, quando: str) -> bytes:
        return json.dumps({"id": f"evt_{status}_{i}", "type": f"carrinho.{status}",
                           "tenant": "loja-a",
                           "data": {"carrinho_id": f"c_{i}", "status": status,
                                    "itens": 1, "updated_at": quando}},
                          separators=(",", ":")).encode()

    async with servidor(receptor) as url_r, httpx.AsyncClient(base_url=url_r) as cli:
        async def postar(c: bytes, h: dict[str, str]) -> str:
            r = await cli.post(url_a, content=c, headers=h)
            return r.json()["status"] if r.status_code in (200, 404) else str(r.status_code)

        t1 = datetime.now().astimezone().isoformat()
        originais = []
        for i in range(n):
            c = corpo(i, "abandonado", t1)
            h = cabecalhos(segredos["loja-a"], f"evt_abandonado_{i}", c)
            originais.append((c, h))
        await asyncio.gather(*(postar(c, h) for c, h in originais))
        await asyncio.sleep(0.01)
        t2 = datetime.now().astimezone().isoformat()
        await asyncio.gather(*(postar(corpo(i, "pago", t2),
                                      cabecalhos(segredos["loja-a"], f"evt_pago_{i}",
                                                 corpo(i, "pago", t2)))
                               for i in range(n)))
        inicio = time.monotonic()
        for rotulo, instante in (("reenvio_5s", 5.0), ("reenvio_20s", 20.0)):
            await asyncio.sleep(max(0.0, inicio + ex.t(instante) - time.monotonic()))
            st = await asyncio.gather(*(postar(c, h) for c, h in originais))
            resultados[f"{rotulo}_aceitos"] = st.count("aceito")
        # Ataques que a assinatura deve barrar (todos devem dar 404 idêntico):
        c0, h0 = originais[0]
        adulterados = [postar(c.replace(b'"itens":1', b'"itens":9'),
                              cabecalhos(segredos["loja-a"], f"x_{i}", c)) for i, (c, _) in
                       enumerate(originais)]
        outra = [postar(corpo(i, "abandonado", t1),
                        cabecalhos(segredos["loja-b"], f"y_{i}", corpo(i, "abandonado", t1)))
                 for i in range(n)]
        invalida = [postar(c, {**h, "webhook-signature": "v1,AAAA"}) for c, h in originais]
        for rotulo, lote in (("adulterados", adulterados), ("outra_empresa", outra),
                             ("assinatura_invalida", invalida)):
            st = await asyncio.gather(*lote)
            resultados[f"{rotulo}_rejeitados"] = st.count("nao_encontrado")
        await receptor.state.fila.join()
        ex.registro.contadores("receptor", receptor.state.contadores)
    resultados["regressoes"] = receptor.state.contadores["regressao"]
    return resultados


async def cenario_hibrido(ex: Execucao, p: float, P: int, T: float = 5.0,
                          T_R: float = 50.0, lambda_T: float = 1.0,
                          n_eventos: int = 200, por_pagina: int = 100) -> dict[str, Any]:
    rng = ex.iniciar({"p": p, "P": P, "T": T, "T_R": T_R, "lambda_T": lambda_T,
                      "n_eventos": n_eventos})
    Tt, TRt = ex.t(T), ex.t(T_R)
    prov = Provedor(tamanho_janela=P * por_pagina)
    segredo = novo_segredo(rng)
    receptor = criar_app(ConfigReceptor(segredos={"loja-a": segredo},
                                        ao_processar=ex.registro.gancho_receptor()))
    if P > 1:  # histórico que o receptor já conhece (só pesa no custo da releitura)
        preencher_janela(prov, "loja-a", P * por_pagina)
        for carrinho, d in prov.estado_atual("loja-a").items():
            receptor.state.estado.aplicar(f"loja-a:{carrinho}", d["updated_at"], d)
    async with servidor(criar_app_provedor(ConfigProvedor(provedor=prov))) as url_p, \
            servidor(receptor) as url_r, \
            httpx.AsyncClient(base_url=url_p) as cli_p, httpx.AsyncClient() as cli_r:
        despachante = Despachante(cli_r, prob_perda=p, semente_perda=ex.semente,
                                  ao_concluir=ex.registro.entrega)
        despachos = Despachos(despachante,
                              {"loja-a": Destino(f"{url_r}/webhooks/loja-a", segredo)})
        reconciliador = Reconciliador(
            cli_p, "loja-a", receptor.state.estado, TRt, por_pagina=por_pagina,
            ao_recuperar=ex.registro.gancho_consultor("reconciliacao"))
        tarefa_rec = asyncio.create_task(reconciliador.executar(duracao_s=math.inf))

        def criado(ev: Evento) -> None:
            ex.registro.evento_criado(ev)
            despachos.agendar(ev)

        await gerar_eventos(prov, ["loja-a"], lambda_T / Tt, ex.semente, criado,
                            n_eventos=n_eventos)
        await despachos.aguardar()
        await receptor.state.fila.join()
        await asyncio.sleep(TRt + ex.t(1.0))  # garante uma reconciliação após o último
        tarefa_rec.cancel()
        ex.registro.contadores("despachante", despachante.contadores)
        ex.registro.contadores("reconciliador", reconciliador.contadores)
    faltando = 0
    for carrinho, dados in prov.estado_atual("loja-a").items():
        local = receptor.state.estado.obter(f"loja-a:{carrinho}")
        faltando += local is None or local.dados != dados
    ex_id = ex.registro.execucao
    perdidos_recuperados = ex.registro.consultar(
        "SELECT COUNT(*) FROM entregas en JOIN conhecimentos c"
        " ON c.execucao = en.execucao AND c.evento_id = en.evento_id"
        " WHERE en.execucao = ? AND en.motivo = 'perda_induzida'"
        " AND c.mecanismo = 'reconciliacao'", (ex_id,))[0][0]
    # A reconciliação às vezes alcança um evento cujo webhook ainda estava a
    # caminho: ele conta como corrigido pela reconciliação, mas não era perda.
    antecipados = reconciliador.contadores["recuperados"] - perdidos_recuperados
    return {"perdidos": despachante.contadores["nao_entregue_perda_induzida"],
            "perdidos_recuperados": perdidos_recuperados,
            "reconciliacao_antes_do_webhook": antecipados,
            "faltando_no_fim": faltando,
            "req_webhook": despachante.contadores["tentativas"],
            "req_reconciliacao": reconciliador.contadores["requisicoes"]}


# ------------------------------------------------------------------ grade ----

GRADE: dict[str, list[tuple[Callable, dict[str, Any]]]] = {
    "h1": [(cenario_h1, {"T": T, "D": D}) for T in (5, 30, 60) for D in (0, 15)],
    "custo": ([(cenario_custo, {"lambda_T": l, "P": 1}) for l in (0.1, 0.5, 1, 2, 10)] +
              [(cenario_custo, {"lambda_T": l, "P": 10}) for l in (1, 2, 5, 10, 20, 50)]),
    "escala": [(cenario_escala, {"N": N, "P": P}) for P in (1, 10) for N in (1, 10, 50, 100)],
    "falhas": [(cenario_falhas, {"variante": v})
               for v in ("queda", "500", "lento_antes", "lento_depois")],
    "duplicidade": [(cenario_duplicidade, {"idempotencia": i}) for i in (False, True)],
    "seguranca": ([(cenario_seguranca, {"config": c}) for c in CONFIGS_SEGURANCA] +
                  [(cenario_seguranca, {"config": "idempotencia_retencao_12s",
                                        "verificar_ordem": False})]),
    "hibrido": [(cenario_hibrido, {"p": p, "P": P}) for P in (1, 10) for p in (0.0, 0.05, 0.2)],
}

RAPIDO = {"n_eventos": 20, "n": 20, "duracao": 1000.0}


async def executar(cenarios: list[str], repeticoes: list[int], saida: Path,
                   rapido: bool, semente_base: int) -> None:
    """Roda as repetições pedidas de cada cenário.

    A semente depende só de (cenário, configuração, repetição), não de quais
    outras repetições rodam junto: rodar a repetição 3 sozinha numa pasta
    própria gera exatamente o mesmo que rodá-la dentro da série completa.
    Cada configuração deixa uma linha em log_execucao.csv com início e fim
    (horário local), para preencher as durações efetivas da Seção 3.6.
    """
    saida.mkdir(parents=True, exist_ok=True)
    registro = Registro(str(saida / "experimento.sqlite"))
    log = saida / "log_execucao.csv"
    novo_log = not log.exists()
    fator = 1 / 50 if rapido else 1.0
    with log.open("a", encoding="utf-8") as f_log:
        if novo_log:
            f_log.write("cenario,parametros,repeticao,semente,inicio,fim,duracao_s\n")
        for nome in cenarios:
            for indice, (funcao, params) in enumerate(GRADE[nome]):
                for rep in repeticoes:
                    semente = (semente_base + 10_000 * rep + 100 * indice
                               + list(GRADE).index(nome))
                    ex = Execucao(registro, nome, rep, semente, fator)
                    extra = {}
                    if rapido:
                        sig = funcao.__code__.co_varnames[:funcao.__code__.co_argcount]
                        extra = {k: v for k, v in RAPIDO.items()
                                 if k in sig and k not in params}
                        if "duracao" in extra:
                            extra["duracao"] = 1000.0  # escalado -> 20 s
                    inicio_parede = datetime.now().astimezone()
                    inicio = time.monotonic()
                    resultado = await funcao(ex, **params, **extra)
                    duracao = time.monotonic() - inicio
                    # guarda o resumo numérico do cenário junto com os contadores
                    registro.contadores("resultado", {
                        k: int(v) for k, v in resultado.items()
                        if isinstance(v, (int, float, bool))})
                    fim_parede = datetime.now().astimezone()
                    f_log.write(f'{nome},"{json.dumps(params)}",{rep},{semente},'
                                f"{inicio_parede.isoformat(timespec='seconds')},"
                                f"{fim_parede.isoformat(timespec='seconds')},"
                                f"{duracao:.1f}\n")
                    f_log.flush()
                    print(f"[{fim_parede:%H:%M:%S}] [{nome}] {params} rep {rep}: "
                          f"{resultado} ({duracao:.1f} s)", flush=True)
    registro.exportar_csv(saida)


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--cenario", choices=[*GRADE, "todos"], required=True)
    grupo = ap.add_mutually_exclusive_group()
    grupo.add_argument("--repeticoes", type=int, default=5,
                       help="roda as repetições 1..N em série")
    grupo.add_argument("--repeticao", type=int,
                       help="roda só a repetição indicada (para execução em paralelo,"
                            " cada uma com a sua pasta de saída)")
    ap.add_argument("--saida", type=Path,
                    default=Path("resultados") / datetime.now().strftime("%Y%m%d-%H%M%S"))
    ap.add_argument("--rapido", action="store_true",
                    help="tempos ÷ 50 e 20 eventos: só para conferir que tudo roda")
    ap.add_argument("--semente-base", type=int, default=2026)
    ap.add_argument("--permitir-sem-commit", action="store_true",
                    help="só para ensaios: aceita rodar sem commit ou com alterações")
    a = ap.parse_args()
    versao = versao_git(Path(__file__).resolve().parents[2])
    if not a.permitir_sem_commit and not a.rapido and (
            versao == "sem-commit" or versao.endswith("+modificado")):
        sys.exit(f"Versão do código: {versao}. Faça o commit antes da execução "
                 "(ou use --permitir-sem-commit num ensaio).")
    cenarios = list(GRADE) if a.cenario == "todos" else [a.cenario]
    reps = [a.repeticao] if a.repeticao else list(range(1, a.repeticoes + 1))
    asyncio.run(executar(cenarios, reps, a.saida, a.rapido, a.semente_base))


if __name__ == "__main__":
    main()

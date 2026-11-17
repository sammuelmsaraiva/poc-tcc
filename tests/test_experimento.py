"""Testes do executor com servidores HTTP reais (uvicorn em 127.0.0.1).

Versões mínimas dos cenários (tempos ÷ 50, poucos eventos): conferem que cada
cenário roda de ponta a ponta e produz resultados coerentes. Os números do
experimento de verdade saem da execução longa, não daqui.
"""

import pytest

from poc.experimento import (Execucao, cenario_duplicidade, cenario_falhas,
                             cenario_h1, cenario_hibrido, servidor)
from poc.instrumentacao import Registro
from poc.receptor import ConfigReceptor, criar_app

pytestmark = pytest.mark.anyio

RAPIDO = 1 / 50


@pytest.fixture
def anyio_backend():
    return "asyncio"


def execucao(cenario: str) -> Execucao:
    return Execucao(Registro(), cenario, repeticao=1, semente=123, fator_tempo=RAPIDO)


async def test_servidor_real_sobe_e_responde():
    import httpx
    app = criar_app(ConfigReceptor(segredos={}))
    async with servidor(app) as url, httpx.AsyncClient() as cli:
        assert url.startswith("http://127.0.0.1:")
        assert (await cli.get(f"{url}/saude")).json() == {"status": "ok"}


async def test_h1_webhook_mais_rapido_e_polling_perto_do_modelo():
    ex = execucao("h1")
    r = await cenario_h1(ex, T=30, D=0, n_eventos=20)
    assert r["completo"]
    polling = ex.registro.latencias("polling")
    webhook = ex.registro.latencias("webhook")
    media_p = sum(polling) / len(polling)
    T = 30 * RAPIDO
    assert max(webhook) < min(T / 2, media_p)        # webhook bem mais rápido
    assert media_p == pytest.approx(T / 2, rel=0.5)   # 20 eventos: margem larga


async def test_hibrido_reconciliacao_completa_o_estado():
    ex = execucao("hibrido")
    r = await cenario_hibrido(ex, p=0.3, P=1, n_eventos=30)
    assert r["perdidos"] > 0
    assert r["perdidos_recuperados"] == r["perdidos"]   # nenhuma perda ficou para trás
    assert r["reconciliacao_antes_do_webhook"] >= 0
    assert r["faltando_no_fim"] == 0                    # completude pelo estado


async def test_duplicidade_com_e_sem_idempotencia():
    sem = await cenario_duplicidade(execucao("duplicidade"), idempotencia=False, n_eventos=10)
    com = await cenario_duplicidade(execucao("duplicidade"), idempotencia=True, n_eventos=10)
    assert sem["processamentos_duplicados"] == 10
    assert com["processamentos_duplicados"] == 0


async def test_falha_lenta_depois_gera_duplicados_no_servidor_real():
    """Confirma, com uvicorn real, o comportamento previsto na revisão do
    módulo 7: a origem desiste, o receptor já gravou, a repetição é duplicada."""
    ex = execucao("falhas")
    r = await cenario_falhas(ex, variante="lento_depois", n_eventos=5)
    assert r["conhecidos_receptor"] == 5   # completude medida no receptor
    assert r["entregues_origem"] < 5       # a origem acha que falhou
    assert r["duplicados"] > 0

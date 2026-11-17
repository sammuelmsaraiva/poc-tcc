"""Testes do despachante (C5, C8; cenários C e G)."""

import base64
import json
import os

import httpx
import pytest

from poc.assinatura import verificar
from poc.despachante import Despachante, Destino, PoliticaRetentativa
from poc.provedor import Provedor
from poc.receptor import ConfigReceptor, criar_app

pytestmark = pytest.mark.anyio


@pytest.fixture
def anyio_backend():
    return "asyncio"


SEGREDO = "whsec_" + base64.b64encode(os.urandom(32)).decode()
DESTINO = Destino(url="http://receptor/webhooks/loja-a", segredo=SEGREDO)


def novo_evento(prov=None, carrinho="c_1", status="abandonado"):
    prov = prov or Provedor()
    tipo = "pedido.pago" if status == "pago" else "carrinho.abandonado"
    return prov.criar_evento("loja-a", tipo, carrinho, status)


class Sono:
    """Substitui asyncio.sleep: registra as esperas sem esperar de verdade."""

    def __init__(self):
        self.esperas = []

    async def __call__(self, s):
        self.esperas.append(s)


def cliente_com(respostas):
    """Cliente HTTP falso que devolve as respostas da lista, em ordem."""
    pedidos = []
    fila = list(respostas)

    def tratar(req: httpx.Request):
        pedidos.append(req)
        r = fila.pop(0) if len(fila) > 1 else fila[0]
        if isinstance(r, Exception):
            raise r
        return r

    return httpx.AsyncClient(transport=httpx.MockTransport(tratar)), pedidos


# ------------------------- Política de retentativa --------------------------

async def test_espera_sem_jitter_dobra_ate_o_teto():
    p = PoliticaRetentativa(base_s=1, teto_s=10, jitter=False)
    assert [p.espera(n) for n in range(6)] == [1, 2, 4, 8, 10, 10]


async def test_espera_com_jitter_fica_entre_zero_e_o_limite():
    p = PoliticaRetentativa(base_s=1, teto_s=60, jitter=True, semente=3)
    esperas = [p.espera(4) for _ in range(200)]  # limite = 16 s
    assert all(0 <= e <= 16 for e in esperas)
    assert len(set(esperas)) > 150  # espalhadas, não todas iguais


async def test_jitter_e_reprodutivel_com_a_mesma_semente():
    a = PoliticaRetentativa(semente=9)
    b = PoliticaRetentativa(semente=9)
    assert [a.espera(3) for _ in range(5)] == [b.espera(3) for _ in range(5)]


# ------------------------------- Entrega ------------------------------------

async def test_entrega_na_primeira_tentativa():
    cli, pedidos = cliente_com([httpx.Response(200)])
    d = Despachante(cli, dormir=Sono())
    r = await d.entregar(novo_evento(), DESTINO)
    assert r.entregue and r.tentativas == 1 and len(pedidos) == 1


async def test_5xx_repete_ate_conseguir_com_esperas_da_politica():
    cli, _ = cliente_com([httpx.Response(500), httpx.Response(503), httpx.Response(200)])
    sono = Sono()
    d = Despachante(cli, PoliticaRetentativa(jitter=False), dormir=sono)
    r = await d.entregar(novo_evento(), DESTINO)
    assert r.entregue and r.tentativas == 3
    assert sono.esperas == [1, 2]


async def test_4xx_nao_repete_e_vai_para_falhas_definitivas():
    cli, pedidos = cliente_com([httpx.Response(404)])
    d = Despachante(cli, dormir=Sono())
    r = await d.entregar(novo_evento(), DESTINO)
    assert not r.entregue and r.motivo == "4xx" and len(pedidos) == 1
    assert d.falhas_definitivas == [r]


async def test_prazo_estourado_repete_e_esgota():
    cli, pedidos = cliente_com([httpx.ReadTimeout("lento")])
    d = Despachante(cli, PoliticaRetentativa(max_tentativas=4), dormir=Sono())
    r = await d.entregar(novo_evento(), DESTINO)
    assert not r.entregue and r.motivo == "esgotou_prazo" and len(pedidos) == 4


async def test_destino_fora_do_ar_conta_como_falha_de_conexao():
    cli, _ = cliente_com([httpx.ConnectError("recusado")])
    d = Despachante(cli, PoliticaRetentativa(max_tentativas=2), dormir=Sono())
    r = await d.entregar(novo_evento(), DESTINO)
    assert r.motivo == "esgotou_conexao" and r.tentativas == 2


async def test_429_respeita_retry_after():
    cli, _ = cliente_com([httpx.Response(429, headers={"Retry-After": "7"}),
                          httpx.Response(200)])
    sono = Sono()
    d = Despachante(cli, dormir=sono)
    r = await d.entregar(novo_evento(), DESTINO)
    assert r.entregue and sono.esperas == [7.0]


async def test_cada_tentativa_e_assinada_de_novo_com_o_mesmo_id():
    cli, pedidos = cliente_com([httpx.Response(500), httpx.Response(200)])
    d = Despachante(cli, dormir=Sono())
    ev = novo_evento()
    await d.entregar(ev, DESTINO)
    ids = {p.headers["webhook-id"] for p in pedidos}
    assert ids == {ev.id}
    for p in pedidos:  # todas as tentativas têm assinatura válida
        assert verificar(SEGREDO, p.headers, p.content) == ev.id


# ---------------------- Perda induzida e desativação ------------------------

async def test_perda_induzida_total_nao_envia_nada():
    cli, pedidos = cliente_com([httpx.Response(200)])
    d = Despachante(cli, prob_perda=1.0, dormir=Sono())
    r = await d.entregar(novo_evento(), DESTINO)
    assert not r.entregue and r.motivo == "perda_induzida" and pedidos == []
    assert d.falhas_definitivas == []  # perda induzida não é falha de entrega


async def test_perda_induzida_tem_frequencia_proxima_de_p():
    cli, _ = cliente_com([httpx.Response(200)])
    d = Despachante(cli, prob_perda=0.2, semente_perda=11, dormir=Sono())
    prov = Provedor()
    for i in range(2000):
        await d.entregar(novo_evento(prov, carrinho=f"c_{i}"), DESTINO)
    perdidos = d.contadores["nao_entregue_perda_induzida"]
    assert perdidos / 2000 == pytest.approx(0.2, abs=0.03)


async def test_destino_desativado_apos_n_falhas_seguidas():
    cli, pedidos = cliente_com([httpx.Response(404)])
    d = Despachante(cli, desativar_apos=3, dormir=Sono())
    prov = Provedor()
    for i in range(5):
        await d.entregar(novo_evento(prov, carrinho=f"c_{i}"), DESTINO)
    assert len(pedidos) == 3  # o 4º e o 5º nem são enviados
    assert d.contadores["nao_entregue_destino_desativado"] == 2


async def test_parametros_invalidos():
    with pytest.raises(ValueError):
        PoliticaRetentativa(max_tentativas=0)
    with pytest.raises(ValueError):
        Despachante(httpx.AsyncClient(), prob_perda=1.5)


# ------------------- Integração com o receptor (módulo 3) -------------------

async def test_integracao_provedor_despachante_receptor():
    """Evento sai do provedor, é assinado, entregue e aplicado pelo receptor."""
    app = criar_app(ConfigReceptor(segredos={"loja-a": SEGREDO}))
    async with app.router.lifespan_context(app):
        cli = httpx.AsyncClient(transport=httpx.ASGITransport(app=app),
                                base_url="http://receptor")
        d = Despachante(cli, dormir=Sono())
        prov = Provedor()
        ev = novo_evento(prov)
        r1 = await d.entregar(ev, DESTINO)
        r2 = await d.entregar(ev, DESTINO)  # mesma entrega de novo
        await app.state.fila.join()
    assert r1.entregue and r2.entregue
    assert app.state.contadores["aceitos"] == 1
    assert app.state.contadores["duplicados"] == 1
    assert app.state.estado.obter("loja-a:c_1").dados["status"] == "abandonado"


async def test_integracao_segredo_errado_vira_falha_definitiva_sem_repetir():
    outro = "whsec_" + base64.b64encode(os.urandom(32)).decode()
    app = criar_app(ConfigReceptor(segredos={"loja-a": outro}))
    async with app.router.lifespan_context(app):
        cli = httpx.AsyncClient(transport=httpx.ASGITransport(app=app),
                                base_url="http://receptor")
        d = Despachante(cli, dormir=Sono())
        r = await d.entregar(novo_evento(), DESTINO)
    assert not r.entregue and r.motivo == "4xx" and r.tentativas == 1

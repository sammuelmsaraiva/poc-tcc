"""Testes do consultor (polling) e do reconciliador (C2, C3, C15; H2, H3)."""

import base64
import os

import httpx
import pytest

from poc.consultor import Consultor, Reconciliador
from poc.despachante import Despachante, Destino
from poc.idempotencia import EstadoPorRegistro
from poc.provedor import (ConfigProvedor, CotaPorAplicativo, Provedor,
                          criar_app_provedor)
from poc.receptor import ConfigReceptor, criar_app

pytestmark = pytest.mark.anyio


@pytest.fixture
def anyio_backend():
    return "asyncio"


class Relogio:
    def __init__(self, t: float = 1_800_000_000.0) -> None:
        self.t = t

    def __call__(self) -> float:
        return self.t

    def avancar(self, s: float) -> None:
        self.t += s


def montar(cota=None, relogio=None):
    """Provedor + cliente HTTP apontando para ele (sem rede real)."""
    rel = relogio or Relogio()
    prov = Provedor(relogio=rel, relogio_parede=rel)
    cfg = ConfigProvedor(provedor=prov, cota=cota or CotaPorAplicativo(None))
    app = criar_app_provedor(cfg)
    cli = httpx.AsyncClient(transport=httpx.ASGITransport(app=app),
                            base_url="http://provedor")
    return prov, cli, rel


def criar(prov, rel, n, inicio=0, status="abandonado"):
    for i in range(inicio, inicio + n):
        rel.avancar(0.001)  # updated_at distinto e crescente
        tipo = "pedido.pago" if status == "pago" else "carrinho.abandonado"
        prov.criar_evento("loja-a", tipo, f"c_{i}", status)


# ------------------------------ Consultor ------------------------------------

async def test_incremental_detecta_so_o_que_e_novo():
    prov, cli, rel = montar()
    criar(prov, rel, 3)
    c = Consultor(cli, "loja-a", intervalo_s=5)
    assert await c.rodada() == 3 and c.cursor == 3
    criar(prov, rel, 1, inicio=3)
    assert await c.rodada() == 1
    assert await c.rodada() == 0
    assert c.contadores["requisicoes"] == 3  # uma por rodada (P = 1)


async def test_incremental_pagina_quando_ha_mais_que_um_limite():
    prov, cli, rel = montar()
    criar(prov, rel, 250)
    c = Consultor(cli, "loja-a", intervalo_s=5, por_pagina=100)
    assert await c.rodada() == 250
    assert c.contadores["requisicoes"] == 3


async def test_janela_rele_tudo_a_cada_rodada():
    """Termo P: com 250 registros e 100 por página, 3 requisições por rodada,
    mesmo quando nada mudou."""
    prov, cli, rel = montar()
    criar(prov, rel, 250)
    c = Consultor(cli, "loja-a", intervalo_s=5, modo="janela")
    assert await c.rodada() == 250
    assert await c.rodada() == 0
    assert c.contadores["requisicoes"] == 6


async def test_rodada_com_429_e_abandonada_e_nada_avanca():
    rel = Relogio()
    prov, cli, _ = montar(cota=CotaPorAplicativo(limite=2, janela_s=60, relogio=rel),
                          relogio=rel)
    criar(prov, rel, 250)
    c = Consultor(cli, "loja-a", intervalo_s=5, modo="janela")
    assert await c.rodada() == 0  # 3ª página recebe 429
    assert c.contadores["rodadas_falhas"] == 1
    assert c.contadores["respostas_429"] == 1
    assert c.vistos == set()
    rel.avancar(60)
    # cota de 2 por minuto ainda não comporta 3 páginas: falha de novo
    assert await c.rodada() == 0 and c.contadores["rodadas_falhas"] == 2


async def test_cursor_nao_avanca_se_a_rodada_falha_no_meio():
    chamadas = {"n": 0}

    def tratar(req: httpx.Request):
        chamadas["n"] += 1
        if chamadas["n"] == 2:
            return httpx.Response(500)
        cursor = int(req.url.params["cursor"])
        return httpx.Response(200, json={
            "itens": [{"id": f"evt_{cursor + 1}", "type": "x", "data": {}}],
            "proximo_cursor": cursor + 1, "tem_mais": True})

    cli = httpx.AsyncClient(transport=httpx.MockTransport(tratar),
                            base_url="http://provedor")
    c = Consultor(cli, "loja-a", intervalo_s=5)
    assert await c.rodada() == 0
    assert c.cursor == 0 and c.contadores["rodadas_falhas"] == 1


async def test_ao_detectar_recebe_o_instante_da_deteccao():
    prov, cli, rel = montar()
    criar(prov, rel, 1)
    detectados = []
    c = Consultor(cli, "loja-a", intervalo_s=5, relogio=rel,
                  ao_detectar=lambda emp, ev, t: detectados.append((ev["id"], t)))
    await c.rodada()
    assert detectados == [("evt_00000001", rel.t)]


async def test_executar_faz_uma_rodada_por_intervalo():
    prov, cli, rel = montar()

    async def dormir(s):
        rel.avancar(s)

    c = Consultor(cli, "loja-a", intervalo_s=5, relogio=rel, dormir=dormir)
    await c.executar(duracao_s=60)
    assert c.contadores["rodadas_ok"] == 12  # instantes 0, 5, ..., 55
    assert c.contadores["atraso_max_ms"] == 0  # relógio falso: sem atraso


async def test_modo_invalido():
    with pytest.raises(ValueError):
        Consultor(httpx.AsyncClient(), "loja-a", 5, modo="outro")


# ----------------------------- Reconciliador ---------------------------------

async def test_reconciliador_recupera_so_o_que_faltava():
    prov, cli, rel = montar()
    criar(prov, rel, 2)
    estado = EstadoPorRegistro()
    primeiro = prov.listar_janela("loja-a")["itens"][0]["data"]
    estado.aplicar("loja-a:c_0", primeiro["updated_at"], primeiro)  # chegou por webhook
    r = Reconciliador(cli, "loja-a", estado, intervalo_s=50)
    assert await r.reconciliar() == 1  # só o c_1 estava faltando
    assert await r.reconciliar() == 0  # depois disso, nada a corrigir


async def test_reconciliador_corrige_estado_desatualizado():
    prov, cli, rel = montar()
    criar(prov, rel, 1)
    antigo = prov.listar_janela("loja-a")["itens"][0]["data"]
    estado = EstadoPorRegistro()
    estado.aplicar("loja-a:c_0", antigo["updated_at"], antigo)
    criar(prov, rel, 1, status="pago")  # o webhook do "pago" se perdeu
    r = Reconciliador(cli, "loja-a", estado, intervalo_s=50)
    assert await r.reconciliar() == 1
    assert estado.obter("loja-a:c_0").dados["status"] == "pago"


async def test_reconciliador_nao_reconta_com_ordem_desligada():
    prov, cli, rel = montar()
    criar(prov, rel, 1)
    item = prov.listar_janela("loja-a")["itens"][0]["data"]
    estado = EstadoPorRegistro(verificar_ordem=False)
    estado.aplicar("loja-a:c_0", item["updated_at"], item)
    r = Reconciliador(cli, "loja-a", estado, intervalo_s=50)
    assert await r.reconciliar() == 0


# ------------------- Arranjo híbrido (cenário G, ponta a ponta) --------------

async def test_hibrido_webhook_com_perda_e_reconciliacao_fecha_100():
    """Webhook perde parte dos eventos; a reconciliação recupera o resto."""
    segredo = "whsec_" + base64.b64encode(os.urandom(32)).decode()
    prov, cli_prov, rel = montar()
    receptor = criar_app(ConfigReceptor(segredos={"loja-a": segredo}))

    async def sem_espera(_):
        return None

    async with receptor.router.lifespan_context(receptor):
        cli_rec = httpx.AsyncClient(transport=httpx.ASGITransport(app=receptor),
                                    base_url="http://receptor")
        despachante = Despachante(cli_rec, prob_perda=0.2, semente_perda=5,
                                  dormir=sem_espera)
        destino = Destino("http://receptor/webhooks/loja-a", segredo)
        for i in range(200):
            rel.avancar(0.01)
            ev = prov.criar_evento("loja-a", "carrinho.abandonado", f"c_{i}", "abandonado")
            await despachante.entregar(ev, destino)
        await receptor.state.fila.join()

        estado = receptor.state.estado
        perdidos = despachante.contadores["nao_entregue_perda_induzida"]
        assert 0 < perdidos < 200
        rec = Reconciliador(cli_prov, "loja-a", estado, intervalo_s=50)
        assert await rec.reconciliar() == perdidos

    # Completude: todo carrinho do provedor está no estado local, igual.
    for item in prov.listar_janela("loja-a", por_pagina=100)["itens"] + \
            prov.listar_janela("loja-a", pagina=2, por_pagina=100)["itens"]:
        local = estado.obter(f"loja-a:{item['data']['carrinho_id']}")
        assert local is not None and local.dados == item["data"]

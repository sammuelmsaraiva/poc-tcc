"""Testes do provedor simulado (Poisson, atraso D, cursor, janela P, cota)."""

import itertools
import math
import statistics

import pytest
from fastapi.testclient import TestClient

from poc.provedor import (ConfigProvedor, CotaPorAplicativo, Provedor,
                          criar_app_provedor, intervalos_poisson)


class RelogioFalso:
    def __init__(self, t: float = 1000.0) -> None:
        self.t = t

    def __call__(self) -> float:
        return self.t

    def avancar(self, s: float) -> None:
        self.t += s


# ------------------------------ Poisson -------------------------------------

def test_intervalos_tem_media_proxima_de_1_sobre_lambda():
    amostra = list(itertools.islice(intervalos_poisson(2.0, semente=42), 20_000))
    assert statistics.mean(amostra) == pytest.approx(0.5, rel=0.03)


def test_mesma_semente_gera_mesma_sequencia():
    a = list(itertools.islice(intervalos_poisson(1.0, semente=7), 10))
    b = list(itertools.islice(intervalos_poisson(1.0, semente=7), 10))
    assert a == b


def test_taxa_invalida():
    with pytest.raises(ValueError):
        next(intervalos_poisson(0, semente=1))


# ------------------------------ Provedor ------------------------------------

def test_evento_so_fica_visivel_depois_de_d():
    rel = RelogioFalso()
    prov = Provedor(atraso_deteccao_s=15, relogio=rel)
    prov.criar_evento("loja-a", "carrinho.abandonado", "c_1", "abandonado")
    rel.avancar(14.9)
    assert prov.listar_incremental("loja-a")["itens"] == []
    rel.avancar(0.2)
    assert len(prov.listar_incremental("loja-a")["itens"]) == 1


def test_cursor_traz_so_o_que_e_novo():
    prov = Provedor()
    for i in range(3):
        prov.criar_evento("loja-a", "carrinho.abandonado", f"c_{i}", "abandonado")
    r1 = prov.listar_incremental("loja-a", cursor=0)
    assert len(r1["itens"]) == 3 and r1["proximo_cursor"] == 3
    prov.criar_evento("loja-a", "pedido.pago", "c_0", "pago")
    r2 = prov.listar_incremental("loja-a", cursor=r1["proximo_cursor"])
    assert [e["type"] for e in r2["itens"]] == ["pedido.pago"]
    r3 = prov.listar_incremental("loja-a", cursor=r2["proximo_cursor"])
    assert r3["itens"] == [] and r3["proximo_cursor"] == 4


def test_cursor_respeita_limite_e_indica_tem_mais():
    prov = Provedor()
    for i in range(5):
        prov.criar_evento("loja-a", "carrinho.abandonado", f"c_{i}", "abandonado")
    r = prov.listar_incremental("loja-a", limite=2)
    assert len(r["itens"]) == 2 and r["tem_mais"] is True


def test_empresas_nao_se_misturam():
    prov = Provedor()
    prov.criar_evento("loja-a", "carrinho.abandonado", "c_1", "abandonado")
    assert prov.listar_incremental("loja-b")["itens"] == []
    assert prov.listar_janela("loja-b")["total"] == 0


@pytest.mark.parametrize("registros, por_pagina", [(250, 100), (1000, 100), (100, 100)])
def test_janela_tem_p_igual_a_teto_de_m_sobre_m(registros, por_pagina):
    prov = Provedor()
    for i in range(registros):
        prov.criar_evento("loja-a", "carrinho.abandonado", f"c_{i}", "abandonado")
    r = prov.listar_janela("loja-a", pagina=1, por_pagina=por_pagina)
    assert r["total_paginas"] == math.ceil(registros / por_pagina)


def test_janela_mostra_estado_atual_de_cada_carrinho():
    prov = Provedor()
    prov.criar_evento("loja-a", "carrinho.abandonado", "c_1", "abandonado")
    prov.criar_evento("loja-a", "pedido.pago", "c_1", "pago")
    r = prov.listar_janela("loja-a")
    assert r["total"] == 1 and r["itens"][0]["data"]["status"] == "pago"


def test_janela_limitada_mantem_p_constante():
    prov = Provedor(tamanho_janela=1000)
    for i in range(2500):
        prov.criar_evento("loja-a", "carrinho.abandonado", f"c_{i}", "abandonado")
    r = prov.listar_janela("loja-a", por_pagina=100)
    assert r["total"] == 1000 and r["total_paginas"] == 10
    assert prov.listar_janela("loja-a", pagina=10)["itens"][-1]["data"]["carrinho_id"] == "c_2499"
    assert len(prov.estado_atual("loja-a")) == 2500


# ------------------------------- Cota ---------------------------------------

def test_cota_estoura_e_libera_depois_da_janela():
    rel = RelogioFalso()
    cota = CotaPorAplicativo(limite=3, janela_s=60, relogio=rel)
    assert [cota.consumir("app") for _ in range(3)] == [None, None, None]
    espera = cota.consumir("app")
    assert espera == pytest.approx(60)
    rel.avancar(60)
    assert cota.consumir("app") is None


def test_cota_e_por_aplicativo():
    cota = CotaPorAplicativo(limite=1, janela_s=60, relogio=RelogioFalso())
    assert cota.consumir("app-1") is None
    assert cota.consumir("app-2") is None
    assert cota.consumir("app-1") is not None


def test_sem_cota_nunca_bloqueia():
    cota = CotaPorAplicativo(limite=None)
    assert all(cota.consumir("app") is None for _ in range(1000))


# --------------------------------- HTTP -------------------------------------

def test_api_incremental_janela_e_429():
    rel = RelogioFalso()
    prov = Provedor(relogio=rel)
    cota = CotaPorAplicativo(limite=2, janela_s=60, relogio=rel)
    app = criar_app_provedor(ConfigProvedor(provedor=prov, cota=cota))
    prov.criar_evento("loja-a", "carrinho.abandonado", "c_1", "abandonado")
    with TestClient(app) as cli:
        r = cli.get("/v1/eventos", params={"empresa": "loja-a"}, headers={"x-app": "painel"})
        assert r.status_code == 200 and r.json()["proximo_cursor"] == 1
        r = cli.get("/v1/carrinhos", params={"empresa": "loja-a"}, headers={"x-app": "painel"})
        assert r.status_code == 200 and r.json()["total"] == 1
        r = cli.get("/v1/eventos", params={"empresa": "loja-a"}, headers={"x-app": "painel"})
        assert r.status_code == 429 and r.headers["Retry-After"] == "60"
        m = cli.get("/metricas").json()
        assert m["requisicoes_consulta"] == 3 and m["respostas_429"] == 1


def test_api_cadastra_assinatura_de_webhook():
    app = criar_app_provedor(ConfigProvedor())
    with TestClient(app) as cli:
        r = cli.post("/v1/assinaturas", json={"empresa": "loja-a",
                                              "url": "http://x/webhooks/loja-a",
                                              "segredo": "whsec_abc"})
        assert r.json() == {"status": "cadastrado"}
        assert app.state.assinaturas["loja-a"]["url"].endswith("loja-a")

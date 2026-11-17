"""Testes da instrumentação e do injetor de falhas (cenário C; H1)."""

import base64
import csv
import os

import httpx
import pytest

from poc.consultor import Consultor
from poc.despachante import Despachante, Destino, PoliticaRetentativa
from poc.falhas import InjetorFalhas, TransporteComQueda
from poc.instrumentacao import Registro, resumo, versao_git
from poc.provedor import ConfigProvedor, Provedor, criar_app_provedor
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


SEGREDO = "whsec_" + base64.b64encode(os.urandom(32)).decode()


async def sem_espera(_):
    return None


# ------------------------------ Instrumentação -------------------------------

async def test_latencia_e_conhecido_menos_ocorrido():
    rel = Relogio()
    reg = Registro(relogio=rel)
    reg.iniciar_execucao("A", repeticao=1, semente=42, parametros={"T": 5})
    prov = Provedor(atraso_deteccao_s=2, relogio=rel, relogio_parede=rel)
    ev = prov.criar_evento("loja-a", "carrinho.abandonado", "c_1", "abandonado")
    reg.evento_criado(ev)
    rel.avancar(3.5)
    reg.conhecimento(ev.id, "polling")
    assert reg.latencias("polling") == [pytest.approx(3.5)]


async def test_so_a_primeira_deteccao_de_cada_mecanismo_vale():
    rel = Relogio()
    reg = Registro(relogio=rel)
    reg.iniciar_execucao("A", 1, 42, {})
    ev = Provedor(relogio=rel, relogio_parede=rel).criar_evento(
        "loja-a", "carrinho.abandonado", "c_1", "abandonado")
    reg.evento_criado(ev)
    rel.avancar(1)
    reg.conhecimento(ev.id, "webhook")
    rel.avancar(5)
    reg.conhecimento(ev.id, "webhook")   # repetido: ignorado
    reg.conhecimento(ev.id, "polling")   # outro mecanismo: conta
    assert reg.latencias("webhook") == [pytest.approx(1)]
    assert reg.latencias("polling") == [pytest.approx(6)]


async def test_registrar_sem_execucao_e_erro():
    with pytest.raises(RuntimeError):
        Registro().conhecimento("evt_1", "polling")


async def test_versao_do_codigo_fica_registrada(tmp_path):
    reg = Registro()
    reg.iniciar_execucao("A", 1, 42, {}, versao_codigo="abc123")
    reg.exportar_csv(tmp_path)
    linha = next(csv.DictReader((tmp_path / "execucoes.csv").open()))
    assert linha["versao_codigo"] == "abc123"


async def test_versao_git_sem_commits(tmp_path):
    assert versao_git(tmp_path) == "sem-commit"


async def test_resumo_estatistico():
    r = resumo([1, 2, 3, 4, 100])
    assert r["n"] == 5 and r["media"] == 22 and r["mediana"] == 3
    assert r["p95"] == 100
    assert resumo([]) == {"n": 0}


async def test_exportar_csv_gera_uma_planilha_por_tabela(tmp_path):
    reg = Registro()
    reg.iniciar_execucao("B", 2, 7, {"T": 30, "D": 0})
    reg.contadores("receptor", {"aceitos": 10, "duplicados": 1})
    arquivos = reg.exportar_csv(tmp_path)
    assert {a.name for a in arquivos} == {"execucoes.csv", "eventos.csv",
                                          "conhecimentos.csv", "entregas.csv",
                                          "contadores.csv"}
    linhas = list(csv.DictReader((tmp_path / "contadores.csv").open()))
    assert {(l["chave"], l["valor"]) for l in linhas} == {("aceitos", "10"), ("duplicados", "1")}


async def test_consultor_alimenta_o_registro():
    rel = Relogio()
    reg = Registro(relogio=rel)
    reg.iniciar_execucao("A", 1, 42, {"T": 5})
    prov = Provedor(relogio=rel, relogio_parede=rel)
    cli = httpx.AsyncClient(transport=httpx.ASGITransport(
        app=criar_app_provedor(ConfigProvedor(provedor=prov))), base_url="http://p")
    ev = prov.criar_evento("loja-a", "carrinho.abandonado", "c_1", "abandonado")
    reg.evento_criado(ev)
    rel.avancar(4)
    c = Consultor(cli, "loja-a", 5, relogio=rel, ao_detectar=reg.gancho_consultor())
    await c.rodada()
    assert reg.latencias("polling") == [pytest.approx(4)]


async def test_receptor_e_despachante_alimentam_o_registro():
    reg = Registro()
    reg.iniciar_execucao("B", 1, 42, {})
    app = criar_app(ConfigReceptor(segredos={"loja-a": SEGREDO},
                                   ao_processar=reg.gancho_receptor()))
    async with app.router.lifespan_context(app):
        cli = httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://r")
        d = Despachante(cli, dormir=sem_espera, ao_concluir=reg.entrega)
        ev = Provedor().criar_evento("loja-a", "carrinho.abandonado", "c_1", "abandonado")
        reg.evento_criado(ev)
        await d.entregar(ev, Destino("http://r/webhooks/loja-a", SEGREDO))
        await app.state.fila.join()
    lat = reg.latencias("webhook")
    assert len(lat) == 1 and lat[0] >= 0


# ------------------------------- Falhas ---------------------------------------

def receptor_com_injetor(**kwargs):
    app = criar_app(ConfigReceptor(segredos={"loja-a": SEGREDO}))
    return app, InjetorFalhas(app, **kwargs)


async def test_injetor_500_tem_frequencia_proxima_de_p():
    app, inj = receptor_com_injetor(prob_500=0.3, semente=1)
    async with app.router.lifespan_context(app):
        cli = httpx.AsyncClient(transport=httpx.ASGITransport(app=inj), base_url="http://r")
        codigos = [(await cli.post("/webhooks/loja-a", content=b"{}")).status_code
                   for _ in range(1000)]
    taxa = codigos.count(500) / 1000
    assert taxa == pytest.approx(0.3, abs=0.04)


async def test_injetor_nao_afeta_rotas_fora_dos_webhooks():
    app, inj = receptor_com_injetor(prob_500=1.0)
    async with app.router.lifespan_context(app):
        cli = httpx.AsyncClient(transport=httpx.ASGITransport(app=inj), base_url="http://r")
        assert (await cli.get("/saude")).status_code == 200


async def test_resposta_lenta_estoura_o_prazo_da_origem():
    app, inj = receptor_com_injetor(atraso_s=0.3)
    async with app.router.lifespan_context(app):
        cli = httpx.AsyncClient(transport=httpx.ASGITransport(app=inj), base_url="http://r")
        d = Despachante(cli, PoliticaRetentativa(max_tentativas=2), prazo_s=0.05,
                        dormir=sem_espera)
        ev = Provedor().criar_evento("loja-a", "carrinho.abandonado", "c_1", "abandonado")
        r = await d.entregar(ev, Destino("http://r/webhooks/loja-a", SEGREDO))
    assert not r.entregue and r.motivo == "esgotou_prazo" and r.tentativas == 2


async def test_processa_e_demora_gera_duplicado_e_nao_perda():
    """Atraso DEPOIS da gravação: a origem desiste e repete; o receptor já
    tinha gravado, então a repetição chega como duplicado (pelo menos uma vez)."""
    app, inj = receptor_com_injetor(atraso_s=0.3, momento_atraso="depois")
    async with app.router.lifespan_context(app):
        cli = httpx.AsyncClient(transport=httpx.ASGITransport(app=inj), base_url="http://r")
        d = Despachante(cli, PoliticaRetentativa(max_tentativas=2), prazo_s=0.05,
                        dormir=sem_espera)
        ev = Provedor().criar_evento("loja-a", "carrinho.abandonado", "c_1", "abandonado")
        r = await d.entregar(ev, Destino("http://r/webhooks/loja-a", SEGREDO))
        await app.state.fila.join()
    assert not r.entregue                       # a origem acha que falhou...
    assert app.state.contadores["aceitos"] == 1   # ...mas o receptor gravou
    assert app.state.contadores["duplicados"] == 1
    assert app.state.contadores["aplicado"] == 1  # e processou uma vez só


async def test_momento_de_atraso_invalido():
    with pytest.raises(ValueError):
        InjetorFalhas(None, momento_atraso="durante")


async def test_queda_recusa_conexao_dentro_da_janela_e_volta_depois():
    rel = Relogio()
    chamadas = []
    interno = httpx.MockTransport(lambda req: chamadas.append(1) or httpx.Response(200))
    queda = TransporteComQueda(interno, janelas=[(0, 10)], relogio=rel)
    cli = httpx.AsyncClient(transport=queda)
    with pytest.raises(httpx.ConnectError):
        await cli.get("http://r/")
    rel.avancar(10)
    assert (await cli.get("http://r/")).status_code == 200
    assert queda.contadores["conexoes_recusadas"] == 1 and len(chamadas) == 1


async def test_cenario_c_retentativas_vencem_500_intermitente():
    """Com 50% de 500 e 6 tentativas, praticamente tudo acaba entregue."""
    app, inj = receptor_com_injetor(prob_500=0.5, semente=3)
    async with app.router.lifespan_context(app):
        cli = httpx.AsyncClient(transport=httpx.ASGITransport(app=inj), base_url="http://r")
        d = Despachante(cli, PoliticaRetentativa(max_tentativas=6, semente=3),
                        dormir=sem_espera)
        prov = Provedor()
        for i in range(200):
            ev = prov.criar_evento("loja-a", "carrinho.abandonado", f"c_{i}", "abandonado")
            await d.entregar(ev, Destino("http://r/webhooks/loja-a", SEGREDO))
        await app.state.fila.join()
    # Perda esperada 0,5^6 ≈ 1,6% (≈ 3 de 200, aprox. Poisson); o limite de
    # até 8 perdas falha em menos de 1% das sementes.
    assert d.contadores["entregues"] >= 192
    assert app.state.contadores["aceitos"] == d.contadores["entregues"]

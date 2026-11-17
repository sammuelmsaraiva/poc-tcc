"""Testes do receptor de webhooks (C4, C6, C7, C10, C11, C14; H4)."""

import base64
import json
import os
import time

import pytest
from fastapi.testclient import TestClient

from poc.assinatura import cabecalhos
from poc.receptor import Armazenamento, ConfigReceptor, criar_app


def novo_segredo() -> str:
    return "whsec_" + base64.b64encode(os.urandom(32)).decode()


SEGREDOS = {"loja-a": novo_segredo(), "loja-b": novo_segredo()}


def evento(msg_id: str, status: str, quando: str, empresa: str = "loja-a",
           carrinho: str = "c_1") -> bytes:
    return json.dumps({
        "id": msg_id, "type": f"carrinho.{status}", "tenant": empresa,
        "data": {"carrinho_id": carrinho, "status": status, "itens": 3,
                 "updated_at": quando},
    }).encode()


def enviar(cliente, empresa: str, corpo: bytes, msg_id: str,
           segredo: str | None = None, timestamp: int | None = None):
    h = cabecalhos(segredo or SEGREDOS[empresa], msg_id, corpo,
                   timestamp=timestamp)
    h["content-type"] = "application/json"
    return cliente.post(f"/webhooks/{empresa}", content=corpo, headers=h)


def drenar(cliente, app) -> None:
    """Espera a fila interna terminar de processar os eventos aceitos."""
    cliente.portal.call(app.state.fila.join)


@pytest.fixture
def montar():
    """Cria app e cliente com a configuração pedida pelo teste."""
    abertos = []

    def _montar(**kwargs):
        config = ConfigReceptor(segredos=SEGREDOS, **kwargs)
        app = criar_app(config)
        cliente = TestClient(app)
        cliente.__enter__()
        abertos.append(cliente)
        return app, cliente

    yield _montar
    for c in abertos:
        c.__exit__(None, None, None)


T1, T2 = "2026-11-20T14:00:00Z", "2026-11-20T14:05:00Z"


def test_evento_valido_e_aceito_e_processado(montar):
    app, cli = montar()
    r = enviar(cli, "loja-a", evento("evt_1", "abandonado", T1), "evt_1")
    assert r.status_code == 200 and r.json()["status"] == "aceito"
    drenar(cli, app)
    assert app.state.estado.obter("loja-a:c_1").dados["status"] == "abandonado"


def test_empresa_desconhecida_responde_404(montar):
    _, cli = montar()
    r = enviar(cli, "loja-a", evento("evt_1", "abandonado", T1), "evt_1")
    r = cli.post("/webhooks/loja-x", content=b"{}", headers=dict(r.request.headers))
    assert r.status_code == 404


def test_assinatura_de_outra_empresa_e_rejeitada(montar):
    """H4 (d): evento assinado com o segredo de B não entra na rota de A."""
    _, cli = montar()
    corpo = evento("evt_1", "abandonado", T1)
    r = enviar(cli, "loja-a", corpo, "evt_1", segredo=SEGREDOS["loja-b"])
    assert r.status_code == 404


def test_empresa_inexistente_e_assinatura_invalida_respondem_igual(montar):
    """C14: não dá para descobrir quais empresas existem pela resposta."""
    app, cli = montar()
    corpo = evento("evt_1", "abandonado", T1)
    h = cabecalhos(SEGREDOS["loja-b"], "evt_1", corpo)  # segredo errado para A
    r_inexistente = cli.post("/webhooks/loja-x", content=corpo, headers=h)
    r_assinatura = cli.post("/webhooks/loja-a", content=corpo, headers=h)
    assert r_inexistente.status_code == r_assinatura.status_code == 404
    assert r_inexistente.content == r_assinatura.content
    m = app.state.contadores  # o motivo real fica só nas métricas
    assert m["empresa_desconhecida"] == 1 and m["rejeitados_assinatura"] == 1


def test_corpo_adulterado_e_rejeitado(montar):
    _, cli = montar()
    corpo = evento("evt_1", "abandonado", T1)
    h = cabecalhos(SEGREDOS["loja-a"], "evt_1", corpo)
    r = cli.post("/webhooks/loja-a", content=corpo.replace(b"3", b"9"), headers=h)
    assert r.status_code == 404


def test_tenant_no_corpo_diferente_da_rota_e_rejeitado(montar):
    _, cli = montar()
    corpo = evento("evt_1", "abandonado", T1, empresa="loja-b")
    r = enviar(cli, "loja-a", corpo, "evt_1")
    assert r.status_code == 400


def test_mensagem_antiga_rejeitada_pela_tolerancia(montar):
    _, cli = montar(tolerancia_s=10)
    corpo = evento("evt_1", "abandonado", T1)
    r = enviar(cli, "loja-a", corpo, "evt_1", timestamp=int(time.time()) - 20)
    assert r.status_code == 404


def test_repetido_responde_200_duplicado_e_nao_reprocessa(montar):
    """H4 (c): a origem recebe 2xx e para de repetir; nada é processado duas vezes."""
    app, cli = montar()
    corpo = evento("evt_1", "abandonado", T1)
    enviar(cli, "loja-a", corpo, "evt_1")
    r = enviar(cli, "loja-a", corpo, "evt_1")
    assert r.status_code == 200 and r.json()["status"] == "duplicado"
    drenar(cli, app)
    assert app.state.contadores["aplicado"] == 1
    assert app.state.contadores["duplicados"] == 1


def test_mesmo_id_em_empresas_diferentes_nao_colide(montar):
    """A idempotência é por empresa: evt_1 de A não bloqueia evt_1 de B."""
    _, cli = montar()
    assert enviar(cli, "loja-a", evento("evt_1", "abandonado", T1), "evt_1").json()["status"] == "aceito"
    corpo_b = evento("evt_1", "abandonado", T1, empresa="loja-b")
    assert enviar(cli, "loja-b", corpo_b, "evt_1").json()["status"] == "aceito"


def test_falha_de_gravacao_responde_500_e_permite_nova_tentativa(montar):
    """Falha interna -> 5xx para a origem repetir; a repetição é aceita."""
    falhar = {"agora": True}
    arm = Armazenamento(falhar=lambda: falhar["agora"])
    app, cli = montar(armazenamento=arm)
    corpo = evento("evt_1", "abandonado", T1)
    assert enviar(cli, "loja-a", corpo, "evt_1").status_code == 500
    falhar["agora"] = False
    r = enviar(cli, "loja-a", corpo, "evt_1")
    assert r.status_code == 200 and r.json()["status"] == "aceito"
    assert arm.total() == 1


def test_sem_verificacao_de_ordem_conta_regressao(montar):
    app, cli = montar(verificar_ordem=False)
    enviar(cli, "loja-a", evento("evt_a", "abandonado", T1), "evt_a")
    enviar(cli, "loja-a", evento("evt_b", "pago", T2), "evt_b")
    enviar(cli, "loja-a", evento("evt_c", "abandonado", T1), "evt_c")
    drenar(cli, app)
    assert app.state.contadores["regressao"] == 1
    assert app.state.estado.obter("loja-a:c_1").dados["status"] == "abandonado"


def test_com_verificacao_de_ordem_ignora_evento_antigo(montar):
    app, cli = montar(verificar_ordem=True)
    enviar(cli, "loja-a", evento("evt_a", "abandonado", T1), "evt_a")
    enviar(cli, "loja-a", evento("evt_b", "pago", T2), "evt_b")
    enviar(cli, "loja-a", evento("evt_c", "abandonado", T1), "evt_c")
    drenar(cli, app)
    assert app.state.contadores["ignorado_antigo"] == 1
    assert app.state.estado.obter("loja-a:c_1").dados["status"] == "pago"


def test_sem_idempotencia_repeticao_e_processada_de_novo(montar):
    """Configuração "sem proteção" dos cenários D e E: a repetição passa."""
    app, cli = montar(idempotencia=False)
    corpo = evento("evt_1", "abandonado", T1)
    assert enviar(cli, "loja-a", corpo, "evt_1").json()["status"] == "aceito"
    assert enviar(cli, "loja-a", corpo, "evt_1").json()["status"] == "aceito"
    drenar(cli, app)
    assert app.state.contadores["aceitos"] == 2


def test_gancho_ao_processar_e_chamado(montar):
    vistos = []
    app, cli = montar(ao_processar=lambda emp, ev, res: vistos.append((emp, ev["id"], res.value)))
    enviar(cli, "loja-a", evento("evt_1", "abandonado", T1), "evt_1")
    drenar(cli, app)
    assert vistos == [("loja-a", "evt_1", "aplicado")]


def test_metricas_e_saude(montar):
    _, cli = montar()
    assert cli.get("/saude").json() == {"status": "ok"}
    enviar(cli, "loja-a", evento("evt_1", "abandonado", T1), "evt_1")
    m = cli.get("/metricas").json()
    assert m["recebidos"] == 1 and m["aceitos"] == 1

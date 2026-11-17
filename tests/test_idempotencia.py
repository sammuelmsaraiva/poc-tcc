"""Testes de idempotência (C6) e ordem (C7); base da hipótese H4 (a, b, c)."""

import threading

import pytest

from poc.idempotencia import (EstadoPorRegistro, RegistroIdempotencia,
                              Resultado, ler_data)


class RelogioFalso:
    """Relógio controlado pelo teste, para simular a passagem do tempo."""

    def __init__(self) -> None:
        self.t = 1000.0

    def __call__(self) -> float:
        return self.t

    def avancar(self, segundos: float) -> None:
        self.t += segundos


# ---------------------------- RegistroIdempotencia --------------------------

def test_evento_novo_e_aceito_e_repetido_e_barrado():
    reg = RegistroIdempotencia()
    assert reg.registrar_se_novo("evt_1") is True
    assert reg.registrar_se_novo("evt_1") is False
    assert reg.registrar_se_novo("evt_2") is True


def test_sem_expiracao_barra_mesmo_muito_tempo_depois():
    relogio = RelogioFalso()
    reg = RegistroIdempotencia(retencao_s=None, relogio=relogio)
    reg.registrar_se_novo("evt_1")
    relogio.avancar(10 * 24 * 3600)  # dez dias
    assert reg.registrar_se_novo("evt_1") is False


def test_retencao_expirada_volta_a_aceitar():
    relogio = RelogioFalso()
    reg = RegistroIdempotencia(retencao_s=12, relogio=relogio)
    reg.registrar_se_novo("evt_1")
    relogio.avancar(13)
    assert reg.registrar_se_novo("evt_1") is True


@pytest.mark.parametrize("idade_reenvio, aceito", [(5, False), (20, True)])
def test_cenario_e_reenvio_dentro_e_fora_da_janela(idade_reenvio, aceito):
    """H4 (a) e (b) com X = 12 s: barrado aos 5 s, aceito de novo aos 20 s."""
    relogio = RelogioFalso()
    reg = RegistroIdempotencia(retencao_s=12, relogio=relogio)
    reg.registrar_se_novo("evt_1")
    relogio.avancar(idade_reenvio)
    assert reg.registrar_se_novo("evt_1") is aceito


def test_capacidade_descarta_os_mais_antigos():
    """Anel dos últimos N identificadores: o mais antigo sai primeiro."""
    reg = RegistroIdempotencia(capacidade=3)
    for i in range(1, 5):  # evt_1 .. evt_4
        reg.registrar_se_novo(f"evt_{i}")
    assert len(reg) == 3
    assert reg.registrar_se_novo("evt_1") is True   # já tinha sido descartado
    assert reg.registrar_se_novo("evt_4") is False  # ainda lembrado


def test_expurgo_remove_apenas_os_expirados():
    relogio = RelogioFalso()
    reg = RegistroIdempotencia(retencao_s=10, relogio=relogio)
    reg.registrar_se_novo("antigo")
    relogio.avancar(8)
    reg.registrar_se_novo("recente")
    relogio.avancar(3)  # "antigo" com 11 s, "recente" com 3 s
    assert len(reg) == 1
    assert reg.registrar_se_novo("recente") is False


def test_entregas_simultaneas_do_mesmo_evento_aceitam_uma_so():
    reg = RegistroIdempotencia()
    resultados: list[bool] = []
    barreira = threading.Barrier(50)

    def entregar():
        barreira.wait()
        resultados.append(reg.registrar_se_novo("evt_1"))

    threads = [threading.Thread(target=entregar) for _ in range(50)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    assert resultados.count(True) == 1


def test_esquecer_permite_nova_tentativa():
    """Se a gravação falhou, o mesmo evento precisa poder ser aceito de novo."""
    reg = RegistroIdempotencia()
    reg.registrar_se_novo("evt_1")
    reg.esquecer("evt_1")
    assert reg.registrar_se_novo("evt_1") is True
    reg.esquecer("nunca_visto")  # não deve lançar erro


@pytest.mark.parametrize("kwargs", [{"retencao_s": 0}, {"capacidade": 0}])
def test_parametros_invalidos(kwargs):
    with pytest.raises(ValueError):
        RegistroIdempotencia(**kwargs)


# ------------------------------ EstadoPorRegistro ---------------------------

T1 = "2026-11-20T14:00:00Z"
T2 = "2026-11-20T14:05:00Z"


def test_ler_data_aceita_sufixo_z():
    assert ler_data(T1).utcoffset().total_seconds() == 0


def test_com_ordem_evento_antigo_e_ignorado():
    estado = EstadoPorRegistro(verificar_ordem=True)
    assert estado.aplicar("c_1", T1, {"status": "abandonado"}) is Resultado.APLICADO
    assert estado.aplicar("c_1", T2, {"status": "pago"}) is Resultado.APLICADO
    assert estado.aplicar("c_1", T1, {"status": "abandonado"}) is Resultado.IGNORADO_ANTIGO
    assert estado.obter("c_1").dados["status"] == "pago"


def test_sem_ordem_evento_antigo_causa_regressao():
    estado = EstadoPorRegistro(verificar_ordem=False)
    estado.aplicar("c_1", T1, {"status": "abandonado"})
    estado.aplicar("c_1", T2, {"status": "pago"})
    assert estado.aplicar("c_1", T1, {"status": "abandonado"}) is Resultado.REGRESSAO
    assert estado.obter("c_1").dados["status"] == "abandonado"


def test_mesma_versao_com_ordem_e_ignorada():
    estado = EstadoPorRegistro(verificar_ordem=True)
    estado.aplicar("c_1", T1, {"status": "abandonado"})
    assert estado.aplicar("c_1", T1, {"status": "abandonado"}) is Resultado.IGNORADO_ANTIGO


def test_sequencia_do_cenario_e_combinando_as_duas_defesas():
    """Abandonado -> pago -> reenvio do 'abandonado' depois da retenção.

    A idempotência com retenção expirada deixa o reenvio passar; só a
    verificação de ordem impede que o carrinho pago volte a 'abandonado'.
    """
    relogio = RelogioFalso()
    reg = RegistroIdempotencia(retencao_s=12, relogio=relogio)
    com_ordem = EstadoPorRegistro(verificar_ordem=True)
    sem_ordem = EstadoPorRegistro(verificar_ordem=False)

    eventos = [("evt_a", T1, "abandonado"), ("evt_b", T2, "pago")]
    for msg_id, quando, status in eventos:
        assert reg.registrar_se_novo(msg_id)
        com_ordem.aplicar("c_1", quando, {"status": status})
        sem_ordem.aplicar("c_1", quando, {"status": status})

    relogio.avancar(20)  # reenvio de evt_a com o dobro da tolerância
    assert reg.registrar_se_novo("evt_a") is True  # a idempotência deixou passar
    assert com_ordem.aplicar("c_1", T1, {"status": "abandonado"}) is Resultado.IGNORADO_ANTIGO
    assert sem_ordem.aplicar("c_1", T1, {"status": "abandonado"}) is Resultado.REGRESSAO

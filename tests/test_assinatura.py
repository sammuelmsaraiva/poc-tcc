"""Testes do módulo de assinatura (critérios C10, C11 e C14; hipótese H4-d)."""

import base64
import os
from datetime import datetime, timezone

import pytest
from standardwebhooks.webhooks import Webhook

from poc.assinatura import AssinaturaInvalida, assinar, cabecalhos, verificar


def novo_segredo() -> str:
    return "whsec_" + base64.b64encode(os.urandom(32)).decode()


SEGREDO_A = novo_segredo()  # empresa A
SEGREDO_B = novo_segredo()  # empresa B
CORPO = b'{"id":"evt_1","type":"carrinho.abandonado","data":{"itens":3}}'
AGORA = 1_800_000_000


def test_mensagem_valida_e_aceita():
    h = cabecalhos(SEGREDO_A, "evt_1", CORPO, timestamp=AGORA)
    assert verificar(SEGREDO_A, h, CORPO, agora=AGORA) == "evt_1"


def test_corpo_adulterado_e_rejeitado():
    h = cabecalhos(SEGREDO_A, "evt_1", CORPO, timestamp=AGORA)
    adulterado = CORPO.replace(b"3", b"9")
    with pytest.raises(AssinaturaInvalida, match="não confere"):
        verificar(SEGREDO_A, h, adulterado, agora=AGORA)


def test_id_trocado_e_rejeitado():
    """O identificador também é assinado: trocá-lo invalida a mensagem."""
    h = cabecalhos(SEGREDO_A, "evt_1", CORPO, timestamp=AGORA)
    h["webhook-id"] = "evt_2"
    with pytest.raises(AssinaturaInvalida):
        verificar(SEGREDO_A, h, CORPO, agora=AGORA)


def test_segredo_de_outra_empresa_e_rejeitado():
    """Isolamento (C14): assinatura feita com o segredo de B não vale para A."""
    h = cabecalhos(SEGREDO_B, "evt_1", CORPO, timestamp=AGORA)
    with pytest.raises(AssinaturaInvalida):
        verificar(SEGREDO_A, h, CORPO, agora=AGORA)


def test_mensagem_antiga_e_rejeitada_pela_tolerancia():
    """Atualidade (C11): reenvio com o dobro da tolerância é barrado."""
    h = cabecalhos(SEGREDO_A, "evt_1", CORPO, timestamp=AGORA)
    with pytest.raises(AssinaturaInvalida, match="tolerância"):
        verificar(SEGREDO_A, h, CORPO, tolerancia_s=10, agora=AGORA + 20)


def test_mensagem_dentro_da_tolerancia_e_aceita():
    h = cabecalhos(SEGREDO_A, "evt_1", CORPO, timestamp=AGORA)
    assert verificar(SEGREDO_A, h, CORPO, tolerancia_s=10, agora=AGORA + 5)


def test_sem_tolerancia_aceita_mensagem_antiga():
    """Configuração do cenário E que só confere a assinatura."""
    h = cabecalhos(SEGREDO_A, "evt_1", CORPO, timestamp=AGORA)
    assert verificar(SEGREDO_A, h, CORPO, tolerancia_s=None, agora=AGORA + 3600)


def test_timestamp_no_futuro_tambem_e_rejeitado():
    h = cabecalhos(SEGREDO_A, "evt_1", CORPO, timestamp=AGORA + 60)
    with pytest.raises(AssinaturaInvalida, match="tolerância"):
        verificar(SEGREDO_A, h, CORPO, tolerancia_s=10, agora=AGORA)


@pytest.mark.parametrize("faltando", ["webhook-id", "webhook-timestamp",
                                      "webhook-signature"])
def test_cabecalho_ausente_e_rejeitado(faltando):
    h = cabecalhos(SEGREDO_A, "evt_1", CORPO, timestamp=AGORA)
    del h[faltando]
    with pytest.raises(AssinaturaInvalida, match="ausente"):
        verificar(SEGREDO_A, h, CORPO, agora=AGORA)


def test_varias_assinaturas_basta_uma_valida():
    """Troca de segredo sem interrupção: o cabeçalho traz duas assinaturas."""
    h = cabecalhos(SEGREDO_A, "evt_1", CORPO, timestamp=AGORA)
    antiga = assinar(SEGREDO_B, "evt_1", AGORA, CORPO)
    h["webhook-signature"] = f"{antiga} {h['webhook-signature']}"
    assert verificar(SEGREDO_A, h, CORPO, agora=AGORA) == "evt_1"


def test_nomes_de_cabecalho_sem_diferenciar_maiusculas():
    h = {k.upper(): v for k, v in
         cabecalhos(SEGREDO_A, "evt_1", CORPO, timestamp=AGORA).items()}
    assert verificar(SEGREDO_A, h, CORPO, agora=AGORA) == "evt_1"


# --- Compatibilidade com a biblioteca de referência do Standard Webhooks ---

def test_biblioteca_de_referencia_aceita_assinatura_da_poc():
    agora = int(datetime.now(timezone.utc).timestamp())
    h = cabecalhos(SEGREDO_A, "evt_1", CORPO, timestamp=agora)
    Webhook(SEGREDO_A).verify(CORPO, h)  # lança exceção se não aceitar


def test_poc_aceita_assinatura_da_biblioteca_de_referencia():
    agora = datetime.now(timezone.utc)
    assinatura = Webhook(SEGREDO_A).sign("evt_1", agora, CORPO.decode())
    h = {"webhook-id": "evt_1",
         "webhook-timestamp": str(int(agora.timestamp())),
         "webhook-signature": assinatura}
    assert verificar(SEGREDO_A, h, CORPO) == "evt_1"

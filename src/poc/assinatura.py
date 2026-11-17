"""Assinatura e verificação de webhooks no formato Standard Webhooks.

Cada mensagem leva três cabeçalhos:
    webhook-id         identificador único do evento
    webhook-timestamp  horário do envio, em segundos desde a época Unix
    webhook-signature  "v1,<base64(HMAC-SHA256(segredo, id.timestamp.corpo))>"

O conteúdo assinado junta identificador, horário e corpo. Assim a assinatura
protege a integridade do corpo (critério C10) e, com a tolerância de tempo,
barra mensagens antigas reenviadas (critério C11).

O segredo segue a convenção da especificação: "whsec_" + chave em base64.
"""

from __future__ import annotations

import base64
import hashlib
import hmac
import time
from collections.abc import Mapping

PREFIXO_SEGREDO = "whsec_"
VERSAO = "v1"
TOLERANCIA_PADRAO_S = 300  # mesmo valor da biblioteca de referência (5 min)


class AssinaturaInvalida(Exception):
    """A mensagem não pode ser aceita; o motivo vai na mensagem do erro."""


def _chave(segredo: str) -> bytes:
    """Converte o segredo "whsec_<base64>" nos bytes da chave HMAC."""
    if segredo.startswith(PREFIXO_SEGREDO):
        segredo = segredo[len(PREFIXO_SEGREDO):]
    return base64.b64decode(segredo)


def assinar(segredo: str, msg_id: str, timestamp: int, corpo: bytes) -> str:
    """Devolve o valor do cabeçalho webhook-signature para a mensagem."""
    conteudo = f"{msg_id}.{timestamp}.".encode() + corpo
    digest = hmac.new(_chave(segredo), conteudo, hashlib.sha256).digest()
    return f"{VERSAO},{base64.b64encode(digest).decode()}"


def cabecalhos(segredo: str, msg_id: str, corpo: bytes,
               timestamp: int | None = None) -> dict[str, str]:
    """Monta os três cabeçalhos de envio (usado pelo provedor simulado)."""
    ts = int(time.time()) if timestamp is None else timestamp
    return {
        "webhook-id": msg_id,
        "webhook-timestamp": str(ts),
        "webhook-signature": assinar(segredo, msg_id, ts, corpo),
    }


def verificar(segredo: str, headers: Mapping[str, str], corpo: bytes,
              tolerancia_s: float | None = TOLERANCIA_PADRAO_S,
              agora: float | None = None) -> str:
    """Confere autenticidade e atualidade; devolve o webhook-id se aceita.

    - corpo deve ser o corpo cru, exatamente como chegou (sem reformatar JSON);
    - tolerancia_s=None desliga a checagem de idade (usado no cenário E para
      reproduzir um receptor que só confere a assinatura);
    - a comparação usa hmac.compare_digest, que leva o mesmo tempo
      independentemente de quantos caracteres coincidem.
    """
    h = {k.lower(): v for k, v in headers.items()}
    try:
        msg_id = h["webhook-id"]
        ts_txt = h["webhook-timestamp"]
        recebidas = h["webhook-signature"]
    except KeyError as falta:
        raise AssinaturaInvalida(f"cabeçalho ausente: {falta.args[0]}") from None

    try:
        ts = int(ts_txt)
    except ValueError:
        raise AssinaturaInvalida("timestamp não numérico") from None

    if tolerancia_s is not None:
        referencia = time.time() if agora is None else agora
        if abs(referencia - ts) > tolerancia_s:
            raise AssinaturaInvalida("mensagem fora da tolerância de tempo")

    esperada = assinar(segredo, msg_id, ts, corpo).split(",", 1)[1]
    # O cabeçalho pode trazer várias assinaturas separadas por espaço
    # (troca de segredo sem interrupção); basta uma "v1" conferir.
    for item in recebidas.split():
        versao, _, valor = item.partition(",")
        if versao == VERSAO and hmac.compare_digest(valor, esperada):
            return msg_id
    raise AssinaturaInvalida("assinatura não confere")

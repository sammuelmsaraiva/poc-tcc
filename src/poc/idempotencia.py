"""Idempotência por identificador e verificação de ordem.

Duas defesas diferentes, que o trabalho mantém separadas de propósito:

1. RegistroIdempotencia (critério C6): lembra os identificadores de eventos já
   processados e recusa repetições. A memória pode ter prazo (retencao_s) e/ou
   tamanho máximo (capacidade), como acontece em sistemas reais. Passado esse
   limite, um evento antigo reenviado volta a ser aceito — é exatamente o que
   o cenário E mede (hipótese H4, condição b).

2. EstadoPorRegistro (critério C7): guarda a versão mais recente de cada
   registro (ex.: um carrinho) e, se a verificação de ordem estiver ligada,
   recusa eventos mais antigos que o estado atual. Sem essa verificação, um
   evento antigo aceito faz o registro "voltar no tempo" (regressão de estado).
"""

from __future__ import annotations

import threading
import time
from collections import OrderedDict
from collections.abc import Callable
from dataclasses import dataclass
from datetime import datetime
from enum import Enum
from typing import Any

Relogio = Callable[[], float]


class RegistroIdempotencia:
    """Registro de identificadores já vistos, com retenção e capacidade.

    retencao_s=None  -> o identificador nunca expira por tempo;
    capacidade=None  -> sem limite de quantidade (senão, descarta os mais
                        antigos, como um anel dos últimos N identificadores).
    """

    def __init__(self, retencao_s: float | None = None,
                 capacidade: int | None = None,
                 relogio: Relogio = time.monotonic) -> None:
        if retencao_s is not None and retencao_s <= 0:
            raise ValueError("retencao_s deve ser positiva ou None")
        if capacidade is not None and capacidade <= 0:
            raise ValueError("capacidade deve ser positiva ou None")
        self.retencao_s = retencao_s
        self.capacidade = capacidade
        self._relogio = relogio
        self._vistos: OrderedDict[str, float] = OrderedDict()  # id -> instante
        self._trava = threading.Lock()

    def _expurgar(self, agora: float) -> None:
        """Remove do início (mais antigos) o que passou da retenção."""
        if self.retencao_s is None:
            return
        while self._vistos:
            msg_id, instante = next(iter(self._vistos.items()))
            if agora - instante <= self.retencao_s:
                break
            self._vistos.popitem(last=False)

    def registrar_se_novo(self, msg_id: str) -> bool:
        """Devolve True se o evento é novo (e o registra); False se repetido.

        A checagem e o registro acontecem sob a mesma trava, para que duas
        entregas simultâneas do mesmo evento não sejam ambas aceitas.
        """
        with self._trava:
            agora = self._relogio()
            self._expurgar(agora)
            if msg_id in self._vistos:
                return False
            self._vistos[msg_id] = agora
            if self.capacidade is not None and len(self._vistos) > self.capacidade:
                self._vistos.popitem(last=False)
            return True

    def esquecer(self, msg_id: str) -> None:
        """Desfaz o registro de um identificador.

        Usado quando o evento foi registrado mas a gravação falhou: sem isso,
        a nova tentativa da origem seria recusada como repetida e o evento
        se perderia.
        """
        with self._trava:
            self._vistos.pop(msg_id, None)

    def __len__(self) -> int:
        with self._trava:
            self._expurgar(self._relogio())
            return len(self._vistos)


class Resultado(str, Enum):
    APLICADO = "aplicado"            # evento novo ou mais recente: estado atualizado
    IGNORADO_ANTIGO = "ignorado_antigo"  # mais antigo (ou igual) ao estado: descartado
    REGRESSAO = "regressao"          # mais antigo, mas aplicado (ordem desligada)


@dataclass(frozen=True)
class Versao:
    atualizado_em: datetime
    dados: dict[str, Any]


def ler_data(valor: str | datetime) -> datetime:
    """Converte 'AAAA-MM-DDTHH:MM:SSZ' (ISO 8601) em datetime com fuso."""
    if isinstance(valor, datetime):
        return valor
    return datetime.fromisoformat(valor.replace("Z", "+00:00"))


class EstadoPorRegistro:
    """Estado atual de cada registro, com verificação de ordem opcional (C7)."""

    def __init__(self, verificar_ordem: bool = True) -> None:
        self.verificar_ordem = verificar_ordem
        self._atual: dict[str, Versao] = {}
        self._trava = threading.Lock()

    def aplicar(self, chave: str, atualizado_em: str | datetime,
                dados: dict[str, Any]) -> Resultado:
        nova = Versao(ler_data(atualizado_em), dados)
        with self._trava:
            atual = self._atual.get(chave)
            if atual is None or nova.atualizado_em > atual.atualizado_em:
                self._atual[chave] = nova
                return Resultado.APLICADO
            if self.verificar_ordem:
                return Resultado.IGNORADO_ANTIGO
            # Sem verificação de ordem: a última mensagem recebida vence.
            self._atual[chave] = nova
            return (Resultado.REGRESSAO
                    if nova.atualizado_em < atual.atualizado_em
                    else Resultado.APLICADO)

    def obter(self, chave: str) -> Versao | None:
        with self._trava:
            return self._atual.get(chave)

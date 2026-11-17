"""Instrumentação: registra os instantes de cada evento e exporta para análise.

Todos os instantes usam o relógio monotônico da máquina (time.monotonic). Como
provedor, despachante, receptor e consultor rodam na mesma máquina, não há
problema de sincronização de relógios (Seção 3.6).

Tabelas (SQLite, exportadas para CSV):
  execucoes      uma linha por execução: cenário, repetição, semente, parâmetros;
  eventos        criação do evento no provedor: ocorrido_em e visivel_em (= +D);
  conhecimentos  PRIMEIRA vez que cada mecanismo tomou conhecimento do evento
                 ("polling", "webhook" ou "reconciliacao"), com o instante;
  entregas       resultado de cada entrega do despachante (tentativas, motivo);
  contadores     contagens finais de cada componente (requisições, 429, ...).

Latência de detecção = conhecido_em - ocorrido_em (Seções 2.3 e 3.5, H1).
"""

from __future__ import annotations

import csv
import json
import sqlite3
import statistics
import subprocess
import threading
import time
from collections.abc import Callable, Mapping
from pathlib import Path
from typing import Any

from poc.despachante import ResultadoEntrega
from poc.provedor import Evento

TABELAS = {
    "execucoes": "id INTEGER PRIMARY KEY, cenario TEXT, repeticao INTEGER,"
                 " semente INTEGER, parametros TEXT, inicio REAL,"
                 " versao_codigo TEXT",
    "eventos": "execucao INTEGER, evento_id TEXT, empresa TEXT, tipo TEXT,"
               " ocorrido_em REAL, visivel_em REAL,"
               " PRIMARY KEY (execucao, evento_id)",
    "conhecimentos": "execucao INTEGER, evento_id TEXT, mecanismo TEXT,"
                     " conhecido_em REAL,"
                     " PRIMARY KEY (execucao, evento_id, mecanismo)",
    "entregas": "execucao INTEGER, evento_id TEXT, empresa TEXT,"
                " entregue INTEGER, tentativas INTEGER, motivo TEXT,"
                " status_final INTEGER, inicio REAL, fim REAL",
    "contadores": "execucao INTEGER, componente TEXT, chave TEXT, valor INTEGER",
}


class Registro:
    def __init__(self, caminho: str = ":memory:",
                 relogio: Callable[[], float] = time.monotonic) -> None:
        self._con = sqlite3.connect(caminho, check_same_thread=False)
        self._trava = threading.Lock()
        self._relogio = relogio
        with self._con:
            for nome, colunas in TABELAS.items():
                self._con.execute(f"CREATE TABLE IF NOT EXISTS {nome} ({colunas})")
        self.execucao: int | None = None

    def _exec(self, sql: str, args: tuple = ()) -> sqlite3.Cursor:
        with self._trava, self._con:
            return self._con.execute(sql, args)

    # ---- início da execução ------------------------------------------------
    def iniciar_execucao(self, cenario: str, repeticao: int, semente: int,
                         parametros: Mapping[str, Any],
                         versao_codigo: str | None = None) -> int:
        """versao_codigo: hash do commit (ver versao_git), para saber com qual
        versão do código cada resultado foi produzido (Seção 3.6)."""
        cur = self._exec(
            "INSERT INTO execucoes (cenario, repeticao, semente, parametros,"
            " inicio, versao_codigo) VALUES (?, ?, ?, ?, ?, ?)",
            (cenario, repeticao, semente,
             json.dumps(parametros, sort_keys=True), self._relogio(),
             versao_codigo))
        self.execucao = cur.lastrowid
        return self.execucao

    def _id(self) -> int:
        if self.execucao is None:
            raise RuntimeError("chame iniciar_execucao antes de registrar")
        return self.execucao

    # ---- ganchos para os componentes ----------------------------------------
    def evento_criado(self, ev: Evento) -> None:
        self._exec("INSERT OR IGNORE INTO eventos VALUES (?, ?, ?, ?, ?, ?)",
                   (self._id(), ev.id, ev.empresa, ev.tipo, ev.ocorrido_em,
                    ev.visivel_em))

    def conhecimento(self, evento_id: str, mecanismo: str,
                     instante: float | None = None) -> None:
        """Só a primeira vez de cada mecanismo vale (INSERT OR IGNORE)."""
        t = self._relogio() if instante is None else instante
        self._exec("INSERT OR IGNORE INTO conhecimentos VALUES (?, ?, ?, ?)",
                   (self._id(), evento_id, mecanismo, t))

    def entrega(self, r: ResultadoEntrega) -> None:
        self._exec("INSERT INTO entregas VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
                   (self._id(), r.evento_id, r.empresa, int(r.entregue),
                    r.tentativas, r.motivo, r.status_final, r.inicio, r.fim))

    def contadores(self, componente: str, valores: Mapping[str, int]) -> None:
        for chave, valor in valores.items():
            self._exec("INSERT INTO contadores VALUES (?, ?, ?, ?)",
                       (self._id(), componente, chave, int(valor)))

    # Adaptadores com a assinatura que cada componente espera:
    def gancho_consultor(self, mecanismo: str = "polling"):
        return lambda empresa, ev, instante: self.conhecimento(ev["id"], mecanismo, instante)

    def gancho_receptor(self):
        return lambda empresa, evento, resultado: self.conhecimento(evento["id"], "webhook")

    # ---- leitura -------------------------------------------------------------
    def latencias(self, mecanismo: str, execucao: int | None = None) -> list[float]:
        ex = self._id() if execucao is None else execucao
        with self._trava:
            linhas = self._con.execute(
                "SELECT c.conhecido_em - e.ocorrido_em FROM conhecimentos c"
                " JOIN eventos e ON e.execucao = c.execucao"
                "  AND e.evento_id = c.evento_id"
                " WHERE c.execucao = ? AND c.mecanismo = ?"
                " ORDER BY e.ocorrido_em", (ex, mecanismo)).fetchall()
        return [l[0] for l in linhas]

    def consultar(self, sql: str, args: tuple = ()) -> list[tuple]:
        """Consulta livre (somente leitura) para os cenários e a análise."""
        with self._trava:
            return self._con.execute(sql, args).fetchall()

    def exportar_csv(self, pasta: str | Path) -> list[Path]:
        pasta = Path(pasta)
        pasta.mkdir(parents=True, exist_ok=True)
        gerados = []
        with self._trava:
            for nome in TABELAS:
                cur = self._con.execute(f"SELECT * FROM {nome}")
                caminho = pasta / f"{nome}.csv"
                with caminho.open("w", newline="", encoding="utf-8") as f:
                    w = csv.writer(f)
                    w.writerow([d[0] for d in cur.description])
                    w.writerows(cur.fetchall())
                gerados.append(caminho)
        return gerados


def versao_git(pasta: str | Path = ".") -> str:
    """Hash do commit atual; acrescenta "+modificado" se há alterações não
    commitadas. Devolve "sem-commit" se o repositório ainda não tem commits."""
    def git(*args: str) -> subprocess.CompletedProcess:
        return subprocess.run(["git", "-C", str(pasta), *args],
                              capture_output=True, text=True)
    cabeca = git("rev-parse", "HEAD")
    if cabeca.returncode != 0:
        return "sem-commit"
    sujo = git("status", "--porcelain").stdout.strip()
    return cabeca.stdout.strip() + ("+modificado" if sujo else "")


def resumo(valores: list[float]) -> dict[str, float]:
    """Média, mediana, desvio-padrão e percentil 95 (Seção 3.7)."""
    if not valores:
        return {"n": 0}
    ordenados = sorted(valores)
    p95 = ordenados[min(len(ordenados) - 1, int(round(0.95 * (len(ordenados) - 1))))]
    return {
        "n": len(valores),
        "media": statistics.fmean(valores),
        "mediana": statistics.median(valores),
        "desvio": statistics.stdev(valores) if len(valores) > 1 else 0.0,
        "p95": p95,
    }

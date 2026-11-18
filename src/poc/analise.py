"""Análise dos resultados (Seções 3.5 e 3.7; Capítulo 6).

Uso:
    uv run python -m poc.analise resultados/<pasta>

Lê <pasta>/experimento.sqlite (ou combina os das subpastas, quando as
repetições rodaram em paralelo, cada uma na sua pasta) e grava em
<pasta>/analise/:
    tabela_h1.csv, tabela_h2_custo.csv, tabela_h2_escala.csv, tabela_h3.csv,
    tabela_h4.csv, tabela_falhas.csv   (uma linha por configuração)
    fig_h1_latencia.png, fig_h2_custo.png, fig_h2_escala.png, fig_h3_hibrido.png
    relatorio.md                        (veredito de cada critério do Quadro 5)

Estatística (Seção 3.7): média, mediana, desvio-padrão e percentil 95;
Mann-Whitney (scipy), δ de Cliff e IC 95% da diferença das medianas por
reamostragem (bootstrap). Se os dados vierem do modo --rapido, o relatório é
marcado como ENSAIO e não deve ser usado no TCC.
"""

from __future__ import annotations

import csv
import json
import math
import sqlite3
import sys
from collections import defaultdict
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import numpy as np
from scipy import stats

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt  # noqa: E402
from matplotlib.ticker import ScalarFormatter  # noqa: E402


class _FormatoVirgula(ScalarFormatter):
    """Rótulos dos eixos com vírgula decimal, como no texto do TCC."""

    def __call__(self, x, pos=None):
        return super().__call__(x, pos).replace(".", ",")


def _virgula(fig) -> None:
    """Aplica a vírgula decimal aos eixos lineares de todos os gráficos."""
    for ax in fig.axes:
        for eixo in (ax.xaxis, ax.yaxis):
            if eixo.get_scale() == "linear":
                eixo.set_major_formatter(_FormatoVirgula())

# ------------------------------------------------------------ estatística ----


def resumo(v: list[float]) -> dict[str, float]:
    if not v:
        return {"n": 0}
    a = np.asarray(v, dtype=float)
    return {"n": len(a), "media": float(a.mean()), "mediana": float(np.median(a)),
            "desvio": float(a.std(ddof=1)) if len(a) > 1 else 0.0,
            "p95": float(np.percentile(a, 95))}


def delta_cliff(a: list[float], b: list[float]) -> float:
    """δ = P(a > b) − P(a < b), entre −1 e 1 (Cliff, 1993)."""
    x = np.asarray(a, dtype=float)[:, None]
    y = np.asarray(b, dtype=float)[None, :]
    return float(((x > y).sum() - (x < y).sum()) / (x.size * y.size))


def ic95_diferenca_medianas(a: list[float], b: list[float], n_boot: int = 2000,
                            semente: int = 1) -> tuple[float, float]:
    rng = np.random.default_rng(semente)
    x, y = np.asarray(a, float), np.asarray(b, float)
    difs = [np.median(rng.choice(x, x.size)) - np.median(rng.choice(y, y.size))
            for _ in range(n_boot)]
    return float(np.percentile(difs, 2.5)), float(np.percentile(difs, 97.5))


def comparar(a: list[float], b: list[float]) -> dict[str, float]:
    """Compara a (ex.: polling) com b (ex.: webhook)."""
    u = stats.mannwhitneyu(a, b, alternative="two-sided")
    lo, hi = ic95_diferenca_medianas(a, b)
    return {"U": float(u.statistic), "p_valor": float(u.pvalue),
            "delta_cliff": delta_cliff(a, b), "ic95_inf": lo, "ic95_sup": hi}


def equilibrio(x: list[float], diferenca: list[float]) -> float | None:
    """Valor de x (λT) em que 'diferenca' (webhook − polling) troca de sinal,
    por interpolação linear em escala logarítmica. None se não cruzar."""
    pares = sorted(zip(x, diferenca))
    for (x0, d0), (x1, d1) in zip(pares, pares[1:]):
        if d0 == 0:
            return x0
        if d0 * d1 < 0:
            l0, l1 = math.log10(x0), math.log10(x1)
            return 10 ** (l0 + (l1 - l0) * (-d0) / (d1 - d0))
    return pares[-1][0] if pares and pares[-1][1] == 0 else None


def n_estrela(cota_por_min: float, T: float, F: int, P: int) -> float:
    """N* = Q·T/(60·F·P): a partir daí o polling ultrapassa a cota."""
    return cota_por_min * T / (60 * F * P)


# ----------------------------------------------------------------- dados ----

@dataclass
class Execucao:
    id: int
    cenario: str
    repeticao: int
    parametros: dict[str, Any]
    versao: str | None


class Dados:
    def __init__(self, caminho: Path) -> None:
        self.con = sqlite3.connect(caminho)
        self.execucoes = [Execucao(i, c, r, json.loads(p), v) for i, c, r, p, v in
                          self.con.execute("SELECT id, cenario, repeticao, parametros,"
                                           " versao_codigo FROM execucoes")]

    def do_cenario(self, nome: str) -> list[Execucao]:
        return [e for e in self.execucoes if e.cenario == nome]

    def contador(self, ex: int, componente: str, chave: str) -> int:
        r = self.con.execute("SELECT SUM(valor) FROM contadores WHERE execucao = ?"
                             " AND componente = ? AND chave = ?",
                             (ex, componente, chave)).fetchone()[0]
        return int(r or 0)

    def latencias(self, ex: int, mecanismo: str) -> list[float]:
        return [l for (l,) in self.con.execute(
            "SELECT c.conhecido_em - e.ocorrido_em FROM conhecimentos c JOIN eventos e"
            " ON e.execucao = c.execucao AND e.evento_id = c.evento_id"
            " WHERE c.execucao = ? AND c.mecanismo = ?", (ex, mecanismo))]

    @property
    def ensaio(self) -> bool:
        return any(e.parametros.get("fator_tempo", 1.0) != 1.0 for e in self.execucoes)


def agrupar(execs: list[Execucao], chaves: tuple[str, ...]):
    grupos: dict[tuple, list[Execucao]] = defaultdict(list)
    for e in execs:
        grupos[tuple(e.parametros.get(k) for k in chaves)].append(e)
    return dict(sorted(grupos.items(), key=lambda kv: [str(x) for x in kv[0]]))


def gravar_csv(caminho: Path, linhas: list[dict[str, Any]]) -> None:
    if not linhas:
        return
    campos: list[str] = []
    for linha in linhas:  # união das colunas, na ordem em que aparecem
        campos += [c for c in linha if c not in campos]
    with caminho.open("w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=campos)
        w.writeheader()
        w.writerows(linhas)


# ------------------------------------------------------------- hipóteses ----

def analisar_h1(d: Dados, saida: Path) -> tuple[list[dict], list[str]]:
    linhas, veredito, caixas = [], [], []
    for (T, D), execs in agrupar(d.do_cenario("h1"), ("T", "D")).items():
        fator = execs[0].parametros.get("fator_tempo", 1.0)
        pol = [l for e in execs for l in d.latencias(e.id, "polling")]
        web = [l for e in execs for l in d.latencias(e.id, "webhook")]
        if not pol or not web:
            continue
        req = sum(d.contador(e.id, "consultor", "requisicoes") for e in execs)
        t_us = sum(d.contador(e.id, "consultor", "tempo_resposta_total_us") for e in execs)
        r_medio = (t_us / req / 1e6) if req else 0.0
        previsto = (D + T / 2) * fator + r_medio
        rp, rw = resumo(pol), resumo(web)
        erro = (rp["media"] - previsto) / previsto
        ok = abs(erro) <= 0.15 and rw["media"] <= (D + 1) * fator
        cmp = comparar(pol, web)
        linhas.append({"T": T, "D": D, "repeticoes": len(execs), "n": rp["n"],
                       **{f"polling_{k}": v for k, v in rp.items() if k != "n"},
                       **{f"webhook_{k}": v for k, v in rw.items() if k != "n"},
                       "previsto_polling": previsto, "erro_relativo": erro,
                       **cmp, "sustentada": ok})
        veredito.append(f"- T = {T} s, D = {D} s: polling {rp['media']:.3f} s "
                        f"(previsto {previsto:.3f} s, erro {erro:+.1%}); webhook "
                        f"{rw['media']:.3f} s; δ de Cliff {cmp['delta_cliff']:.2f} → "
                        f"{'SUSTENTADA' if ok else 'REFUTADA'}")
        caixas.append((f"T={T}\nD={D}", pol, web))
    gravar_csv(saida / "tabela_h1.csv", linhas)
    if caixas:
        fig, ax = plt.subplots(figsize=(8, 4))
        pos = np.arange(len(caixas)) * 3
        ax.boxplot([c[1] for c in caixas], positions=pos, widths=0.8,
                   tick_labels=[""] * len(caixas))
        ax.boxplot([c[2] for c in caixas], positions=pos + 1, widths=0.8,
                   tick_labels=[""] * len(caixas))
        ax.set_xticks(pos + 0.5, [c[0] for c in caixas])
        ax.set_ylabel("latência de detecção (s)")
        ax.set_title("Polling (esquerda) × webhook (direita) por configuração")
        fig.tight_layout()
        _virgula(fig); fig.savefig(saida / "fig_h1_latencia.png", dpi=200)
        plt.close(fig)
    return linhas, veredito


def analisar_h2(d: Dados, saida: Path) -> tuple[list[dict], list[str]]:
    linhas, veredito = [], []
    fig, ax = plt.subplots(figsize=(6, 4))
    for (P,), execs_P in agrupar(d.do_cenario("custo"), ("P",)).items():
        xs, difs, ys_p, ys_w = [], [], [], []
        for (lT,), execs in agrupar(execs_P, ("lambda_T",)).items():
            pol = np.mean([d.contador(e.id, "consultor", "requisicoes") for e in execs])
            web = np.mean([d.contador(e.id, "despachante", "tentativas") for e in execs])
            linhas.append({"P": P, "lambda_T": lT, "repeticoes": len(execs),
                           "req_polling": pol, "req_webhook": web})
            xs.append(lT); difs.append(web - pol); ys_p.append(pol); ys_w.append(web)
        eq = equilibrio(xs, difs)
        faixa = (0.5, 2) if P == 1 else (5, 20)
        ok = eq is not None and faixa[0] <= eq <= faixa[1]
        veredito.append(f"- P = {P}: equilíbrio observado em λT ≈ "
                        f"{'—' if eq is None else f'{eq:.2f}'} (faixa {faixa[0]}–{faixa[1]})"
                        f" → {'SUSTENTADA' if ok else 'REFUTADA'} (H2-a)")
        ax.plot(xs, ys_p, "o-", label=f"polling (P={P})")
        ax.plot(xs, ys_w, "s--", label=f"webhook (P={P})")
    ax.set_xscale("log"); ax.set_yscale("log")
    ax.set_xlabel("eventos por intervalo de consulta (λT)")
    ax.set_ylabel("requisições no período")
    ax.legend(fontsize=8); fig.tight_layout()
    _virgula(fig); fig.savefig(saida / "fig_h2_custo.png", dpi=200); plt.close(fig)
    gravar_csv(saida / "tabela_h2_custo.csv", linhas)

    escala, fig2 = [], None
    grupos = agrupar(d.do_cenario("escala"), ("P", "N"))
    por_P: dict[int, list[tuple[int, float, float, bool]]] = defaultdict(list)
    for (P, N), execs in grupos.items():
        p = execs[0].parametros
        r429 = np.mean([d.contador(e.id, "resultado", "polling_429") for e in execs])
        w429 = np.mean([d.contador(e.id, "resultado", "webhook_429") for e in execs])
        fiel = all(d.contador(e.id, "resultado", "fiel") for e in execs)
        escala.append({"P": P, "N": N, "polling_429": r429, "webhook_429": w429,
                       "fiel": fiel,
                       "N_estrela": n_estrela(p["cota_por_min"], p["T"], 1, P)})
        por_P[P].append((N, r429, w429, fiel))
    for P, pts in por_P.items():
        ns = sorted(pts)
        nstar = escala[[i for i, l in enumerate(escala) if l["P"] == P][0]]["N_estrela"]
        acima = [n for n, *_ in ns if n > nstar]
        primeiro = acima[0] if acima else None
        b_ok = all((r > 0) == (n >= (primeiro or math.inf)) for n, r, _, _ in ns)
        c_ok = all(w == 0 for _, _, w, _ in ns)
        fiel = all(f for *_, f in ns)
        veredito.append(f"- Escala P = {P}: N* ≈ {nstar:.1f}; 429 a partir de N = "
                        f"{next((n for n, r, *_ in ns if r > 0), '—')} (previsto {primeiro}); "
                        f"webhook sem 429: {c_ok}; execução fiel ao horário: {fiel} → "
                        f"{'SUSTENTADA' if b_ok and c_ok and fiel else 'REFUTADA ou INCONCLUSIVA'}"
                        " (H2-b, c)")
    gravar_csv(saida / "tabela_h2_escala.csv", escala)
    if escala:
        fig2, ax2 = plt.subplots(figsize=(6, 4))
        for P, pts in por_P.items():
            ns = sorted(pts)
            ax2.plot([n for n, *_ in ns], [r for _, r, *_ in ns], "o-", label=f"polling P={P}")
        ax2.set_xlabel("empresas atendidas (N)"); ax2.set_ylabel("respostas 429 no período")
        ax2.legend(); fig2.tight_layout()
        _virgula(fig2); fig2.savefig(saida / "fig_h2_escala.png", dpi=200); plt.close(fig2)
    return linhas + escala, veredito


def analisar_h3(d: Dados, saida: Path) -> tuple[list[dict], list[str]]:
    linhas, veredito = [], []
    for (P, p), execs in agrupar(d.do_cenario("hibrido"), ("P", "p")).items():
        par = execs[0].parametros
        fator = par.get("fator_tempo", 1.0)
        T_R, T, lT = par["T_R"] * fator, par["T"] * fator, par["lambda_T"]
        perd = sum(d.contador(e.id, "resultado", "perdidos") for e in execs)
        rec = sum(d.contador(e.id, "resultado", "perdidos_recuperados") for e in execs)
        falt = sum(d.contador(e.id, "resultado", "faltando_no_fim") for e in execs)
        web = sum(d.contador(e.id, "despachante", "tentativas") for e in execs)
        rep = sum(d.contador(e.id, "despachante", "repeticoes") for e in execs)
        rec_req = sum(d.contador(e.id, "reconciliador", "requisicoes") for e in execs)
        k = rep / max(1, web - rep)
        n_ev = sum(e.parametros["n_eventos"] for e in execs)
        # Fórmula simplificada (Seção 2.3): supõe o mesmo período para os dois
        # mecanismos, sem perdas e sem a rodada inicial do reconciliador.
        previsto_formula = 1 + P / (10 * lT * (1 + k))
        # Critério do Quadro 5 (aprovado em 16/11/2026): o reconciliador roda no instante 0 e a cada
        # T_R até uma rodada depois do último evento (1 + ⌊(H+T_R)/T_R⌋ rodadas
        # de P páginas), e o webhook só envia os eventos não perdidos.
        H = np.mean([max(0.0, _duracao(d, e.id)) for e in execs])
        rodadas = (1 + math.floor((H + T_R) / T_R)) * len(execs)
        previsto = 1 + (P * rodadas) / (n_ev * (1 - p) * (1 + k))
        observado = (web + rec_req) / max(1, web)
        rec_lat = [l for e in execs for l in _latencias_recuperacao(d, e.id)]
        dentro = all(l <= T_R * 1.05 for l in rec_lat)
        ok = (rec == perd) and falt == 0 and dentro and abs(observado / previsto - 1) <= 0.10
        linhas.append({"P": P, "p": p, "repeticoes": len(execs), "perdidos": perd,
                       "perdidos_recuperados": rec, "faltando_no_fim": falt,
                       "tempo_recuperacao_max": max(rec_lat, default=0.0),
                       "razao_observada": observado, "razao_prevista": previsto,
                       "razao_prevista_simplificada": previsto_formula,
                       "sustentada": ok if p > 0 else None})
        if p > 0:
            veredito.append(f"- P = {P}, p = {p:.0%}: recuperados {rec}/{perd}, faltando {falt}, "
                            f"recuperação máx. {max(rec_lat, default=0):.2f} s (T_R = {T_R:.2f} s), "
                            f"requisições {observado:.2f}× (previsto {previsto:.2f}×; "
                            f"fórmula simplificada da Seção 2.3: {previsto_formula:.2f}×) → "
                            f"{'SUSTENTADA' if ok else 'REFUTADA'}")
    gravar_csv(saida / "tabela_h3.csv", linhas)
    if linhas:
        fig, ax = plt.subplots(figsize=(6, 4))
        for P in sorted({l["P"] for l in linhas}):
            pts = [l for l in linhas if l["P"] == P]
            ax.plot([l["p"] * 100 for l in pts], [l["razao_observada"] for l in pts],
                    "o-", label=f"observado P={P}")
            ax.plot([l["p"] * 100 for l in pts], [l["razao_prevista"] for l in pts],
                    "x--", label=f"previsto P={P}")
        ax.set_xlabel("perda induzida no webhook (%)")
        ax.set_ylabel("requisições híbrido ÷ webhook")
        ax.legend(fontsize=8); fig.tight_layout()
        _virgula(fig); fig.savefig(saida / "fig_h3_hibrido.png", dpi=200); plt.close(fig)
    return linhas, veredito


def _duracao(d: Dados, ex: int) -> float:
    r = d.con.execute("SELECT MAX(ocorrido_em) - MIN(ocorrido_em) FROM eventos"
                      " WHERE execucao = ?", (ex,)).fetchone()[0]
    return float(r or 0.0)


def _latencias_recuperacao(d: Dados, ex: int) -> list[float]:
    return [l for (l,) in d.con.execute(
        "SELECT c.conhecido_em - e.ocorrido_em FROM entregas en"
        " JOIN conhecimentos c ON c.execucao = en.execucao AND c.evento_id = en.evento_id"
        " JOIN eventos e ON e.execucao = en.execucao AND e.evento_id = en.evento_id"
        " WHERE en.execucao = ? AND en.motivo = 'perda_induzida'"
        " AND c.mecanismo = 'reconciliacao'", (ex,))]


PREVISTO_H4 = {  # (aceitos aos 5 s, aceitos aos 20 s) em fração das tentativas
    "sem_protecao": (1.0, 1.0),
    "idempotencia_retencao_12s": (0.0, 1.0),
    "idempotencia_sem_expiracao": (0.0, 0.0),
    "idempotencia_e_tolerancia_10s": (0.0, 0.0),
}


def analisar_h4(d: Dados, saida: Path) -> tuple[list[dict], list[str]]:
    linhas, veredito = [], []
    for (cfg, ordem), execs in agrupar(d.do_cenario("seguranca"),
                                       ("config", "verificar_ordem")).items():
        n = sum(e.parametros["n"] for e in execs)
        get = lambda k: sum(d.contador(e.id, "resultado", k) for e in execs)  # noqa: E731
        a5, a20 = get("reenvio_5s_aceitos") / n, get("reenvio_20s_aceitos") / n
        rej = min(get("adulterados_rejeitados"), get("outra_empresa_rejeitados"),
                  get("assinatura_invalida_rejeitados")) / n
        esp5, esp20 = PREVISTO_H4[cfg]
        ok = a5 == esp5 and a20 == esp20 and rej == 1.0
        linhas.append({"config": cfg, "verificar_ordem": ordem, "tentativas": n,
                       "aceitos_5s": a5, "aceitos_20s": a20, "rejeitados_ataques": rej,
                       "regressoes": get("regressoes"), "sustentada": ok})
        veredito.append(f"- {cfg} (ordem {'ligada' if ordem else 'desligada'}): reenvio aceito "
                        f"{a5:.0%} aos 5 s e {a20:.0%} aos 20 s (previsto {esp5:.0%} e "
                        f"{esp20:.0%}); ataques rejeitados {rej:.0%}; regressões "
                        f"{get('regressoes')} → {'SUSTENTADA' if ok else 'REFUTADA'}")
    for (idem,), execs in agrupar(d.do_cenario("duplicidade"), ("idempotencia",)).items():
        dup = sum(d.contador(e.id, "resultado", "processamentos_duplicados") for e in execs)
        linhas.append({"config": f"duplicidade_idempotencia={idem}",
                       "processamentos_duplicados": dup,
                       "sustentada": (dup == 0) if idem else None})
        if idem:
            veredito.append(f"- Entrega dupla com idempotência: {dup} processamentos "
                            f"duplicados → {'SUSTENTADA' if dup == 0 else 'REFUTADA'} (H4-c)")
    gravar_csv(saida / "tabela_h4.csv", linhas)
    return linhas, veredito


def analisar_falhas(d: Dados, saida: Path) -> tuple[list[dict], list[str]]:
    linhas, texto = [], []
    for (v,), execs in agrupar(d.do_cenario("falhas"), ("variante",)).items():
        n = sum(e.parametros["n_eventos"] for e in execs)
        get = lambda k: sum(d.contador(e.id, "resultado", k) for e in execs)  # noqa: E731
        linhas.append({"variante": v, "eventos": n,
                       "completude_receptor": get("conhecidos_receptor") / n,
                       "entregues_segundo_origem": get("entregues_origem") / n,
                       "duplicados": get("duplicados")})
        texto.append(f"- {v}: completude no receptor {get('conhecidos_receptor') / n:.0%}; "
                     f"origem registrou {get('entregues_origem') / n:.0%}; "
                     f"duplicados {get('duplicados')}")
    gravar_csv(saida / "tabela_falhas.csv", linhas)
    return linhas, texto


# ---------------------------------------------------------- combinar pastas --

def combinar(fontes: list[Path], destino: Path) -> Path:
    """Junta vários experimento.sqlite (execuções em paralelo, cada uma na sua
    pasta) num só, renumerando as execuções para não colidirem."""
    from poc.instrumentacao import Registro  # cria as tabelas no destino
    Registro(str(destino))
    con = sqlite3.connect(destino)
    for fonte in fontes:
        deslocamento = con.execute("SELECT COALESCE(MAX(id), 0) FROM execucoes").fetchone()[0]
        con.execute("ATTACH DATABASE ? AS f", (str(fonte),))
        con.execute("INSERT INTO execucoes SELECT id + ?, cenario, repeticao, semente,"
                    " parametros, inicio, versao_codigo FROM f.execucoes", (deslocamento,))
        for tabela in ("eventos", "conhecimentos", "entregas", "contadores"):
            colunas = [c[1] for c in con.execute(f"PRAGMA f.table_info({tabela})")]
            sel = ", ".join("execucao + ?" if c == "execucao" else c for c in colunas)
            con.execute(f"INSERT INTO {tabela} SELECT {sel} FROM f.{tabela}", (deslocamento,))
        con.commit()
        con.execute("DETACH DATABASE f")
    con.close()
    return destino


def _preparar(pasta: Path) -> Path:
    """Usa pasta/experimento.sqlite; se não existir, combina os das subpastas
    (uma por repetição executada em paralelo) e junta os logs de duração."""
    alvo = pasta / "experimento.sqlite"
    if alvo.exists():
        return alvo
    fontes = sorted(pasta.glob("*/experimento.sqlite"))
    if not fontes:
        sys.exit(f"nenhum experimento.sqlite em {pasta} nem nas subpastas")
    combinar(fontes, alvo)
    logs = sorted(pasta.glob("*/log_execucao.csv"))
    if logs:
        linhas = [logs[0].read_text(encoding="utf-8").splitlines()[0]]
        for log in logs:
            linhas += log.read_text(encoding="utf-8").splitlines()[1:]
        (pasta / "log_execucao.csv").write_text("\n".join(linhas) + "\n", encoding="utf-8")
    return alvo


# --------------------------------------------------------------- relatório --

def analisar(pasta: Path) -> Path:
    d = Dados(_preparar(pasta))
    saida = pasta / "analise"
    saida.mkdir(exist_ok=True)
    partes = []
    if d.ensaio:
        partes.append("> **ENSAIO (modo --rapido): números só para conferência. "
                      "NÃO usar no TCC.**\n")
    versoes = sorted({e.versao or "?" for e in d.execucoes})
    partes.append(f"Versão do código: {', '.join(versoes)}\n")
    for titulo, funcao in (("H1 — latência", analisar_h1), ("H2 — custo e escala", analisar_h2),
                           ("H3 — arranjo híbrido", analisar_h3),
                           ("H4 — confiabilidade e segurança", analisar_h4),
                           ("Cenário C — falhas (descritivo)", analisar_falhas)):
        _, linhas = funcao(d, saida)
        if linhas:
            partes.append(f"## {titulo}\n\n" + "\n".join(linhas) + "\n")
    relatorio = saida / "relatorio.md"
    relatorio.write_text("# Relatório da análise\n\n" + "\n".join(partes), encoding="utf-8")
    return relatorio


def main() -> None:
    if len(sys.argv) != 2:
        sys.exit("uso: python -m poc.analise resultados/<pasta>")
    print(analisar(Path(sys.argv[1])).read_text(encoding="utf-8"))


if __name__ == "__main__":
    main()

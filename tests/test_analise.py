"""Testes da análise (estatística, critérios e relatório)."""

import pytest

from poc import analise
from poc.experimento import (Execucao, cenario_custo, cenario_duplicidade,
                             cenario_h1, cenario_hibrido)
from poc.instrumentacao import Registro

# ------------------------------- estatística ---------------------------------


def test_delta_cliff_extremos_e_empate():
    assert analise.delta_cliff([5, 6, 7], [1, 2, 3]) == 1.0
    assert analise.delta_cliff([1, 2, 3], [5, 6, 7]) == -1.0
    assert analise.delta_cliff([1, 2], [1, 2]) == 0.0


def test_ic95_contem_a_diferenca_verdadeira():
    import numpy as np
    rng = np.random.default_rng(0)
    a = list(rng.normal(10, 1, 300))
    b = list(rng.normal(7, 1, 300))
    lo, hi = analise.ic95_diferenca_medianas(a, b)
    assert lo < 3 < hi


def test_comparar_detecta_diferenca_grande():
    r = analise.comparar([10, 11, 12, 13, 14] * 10, [1, 2, 3, 4, 5] * 10)
    assert r["p_valor"] < 0.001 and r["delta_cliff"] == 1.0


def test_equilibrio_interpola_no_cruzamento():
    # webhook − polling: negativo antes de λT = 1, positivo depois
    eq = analise.equilibrio([0.1, 0.5, 2, 10], [-180, -100, 200, 1800])
    assert 0.5 < eq < 2
    assert analise.equilibrio([1, 2], [-5, -1]) is None


def test_n_estrela():
    assert analise.n_estrela(500, 5, 1, 1) == pytest.approx(41.67, abs=0.01)
    assert analise.n_estrela(500, 5, 1, 10) == pytest.approx(4.17, abs=0.01)


# ------------------------------- ponta a ponta -------------------------------

pytestmark_anyio = pytest.mark.anyio


@pytest.fixture
def anyio_backend():
    return "asyncio"


@pytest.mark.anyio
async def test_relatorio_de_um_ensaio_pequeno(tmp_path):
    reg = Registro(str(tmp_path / "experimento.sqlite"))
    rapido = 1 / 50

    async def rodar(nome, funcao, rep, **kw):
        ex = Execucao(reg, nome, rep, 100 + rep, rapido)
        r = await funcao(ex, **kw)
        reg.contadores("resultado", {k: int(v) for k, v in r.items()
                                     if isinstance(v, (int, float, bool))})

    await rodar("h1", cenario_h1, 1, T=30, D=0, n_eventos=20)
    for lT in (0.5, 2):
        await rodar("custo", cenario_custo, 1, lambda_T=lT, P=1, duracao=200)
    await rodar("hibrido", cenario_hibrido, 1, p=0.3, P=1, n_eventos=30)
    await rodar("duplicidade", cenario_duplicidade, 1, idempotencia=True, n_eventos=5)
    # H4 com dados sintéticos (o cenário real leva ~21 s por configuração)
    reg.iniciar_execucao("seguranca", 1, 1, {"config": "idempotencia_retencao_12s",
                                            "verificar_ordem": True, "n": 10})
    reg.contadores("resultado", {"reenvio_5s_aceitos": 0, "reenvio_20s_aceitos": 10,
                                 "adulterados_rejeitados": 10,
                                 "outra_empresa_rejeitados": 10,
                                 "assinatura_invalida_rejeitados": 10, "regressoes": 0})

    relatorio = analise.analisar(tmp_path)
    texto = relatorio.read_text(encoding="utf-8")
    assert "ENSAIO" in texto
    for secao in ("H1", "H2", "H3", "H4"):
        assert f"## {secao}" in texto
    pasta = tmp_path / "analise"
    for arq in ("tabela_h1.csv", "tabela_h2_custo.csv", "tabela_h3.csv",
                "tabela_h4.csv", "fig_h1_latencia.png", "fig_h2_custo.png",
                "fig_h3_hibrido.png"):
        assert (pasta / arq).exists(), arq
    assert "idempotencia_retencao_12s" in texto and "SUSTENTADA" in texto


def test_combinar_renumera_execucoes(tmp_path):
    for i in (1, 2):
        (tmp_path / f"rep{i}").mkdir()
        reg = Registro(str(tmp_path / f"rep{i}" / "experimento.sqlite"))
        reg.iniciar_execucao("custo", i, 100 + i, {"P": 1, "lambda_T": 1})
        reg.contadores("consultor", {"requisicoes": 10 * i})
        (tmp_path / f"rep{i}" / "log_execucao.csv").write_text(
            f"cenario,parametros,repeticao,semente,inicio,fim,duracao_s\ncusto,x,{i},1,a,b,1\n")
    alvo = analise._preparar(tmp_path)
    d = analise.Dados(alvo)
    assert sorted(e.repeticao for e in d.execucoes) == [1, 2]
    assert len({e.id for e in d.execucoes}) == 2
    assert sum(d.contador(e.id, "consultor", "requisicoes") for e in d.execucoes) == 30
    assert len((tmp_path / "log_execucao.csv").read_text().splitlines()) == 3

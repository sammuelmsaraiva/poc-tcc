#!/usr/bin/env bash
# Plano da execução longa (Seção 3.6). Rodar na pasta do repositório, com o
# Mac na tomada:
#
#     ./executar_plano.sh
#
# Regras do plano:
#   - pré-requisito: código commitado (o executor recusa "sem-commit" e
#     alterações não commitadas);
#   - caffeinate impede o Mac de dormir durante toda a execução;
#   - H1 roda sozinha, em série (5 repetições): nada em paralelo enquanto ela roda;
#   - escala roda em série (3 repetições), por causa da cota e da marca "fiel";
#   - falhas, duplicidade e segurança rodam em série (5 repetições, são curtas);
#   - custo (3 repetições) e híbrido (5 repetições) rodam em paralelo, cada
#     repetição num processo, com pasta de saída e semente próprias (as portas
#     são livres, sorteadas pelo sistema), no máximo (núcleos − 1) processos;
#   - cada configuração registra início e fim em log_execucao.csv.
# Ao final, a análise junta as pastas e gera o relatório.
# Compatível com o bash 3.2 do macOS.

set -euo pipefail
cd "$(dirname "$0")"

# Reexecuta o próprio script dentro do caffeinate (Mac acordado o tempo todo).
if [ -z "${DENTRO_DO_CAFFEINATE:-}" ]; then
  DENTRO_DO_CAFFEINATE=1 exec caffeinate -dims "$0" "$@"
fi

RAIZ="resultados/$(date +%Y%m%d-%H%M%S)"
mkdir -p "$RAIZ"
NUCLEOS=$(sysctl -n hw.ncpu)
MAX_PARALELO=$(( NUCLEOS > 1 ? NUCLEOS - 1 : 1 ))
echo "Resultados em $RAIZ (até $MAX_PARALELO processos em paralelo)"
git rev-parse HEAD > "$RAIZ/versao_codigo.txt"

serie() {  # serie <cenario> <n_repeticoes>
  uv run python -m poc.experimento --cenario "$1" --repeticoes "$2" \
    --saida "$RAIZ/$1_serie" 2>&1 | tee "$RAIZ/$1.log"
}

paralelo() {  # paralelo <cenario> <n_repeticoes>
  seq 1 "$2" | xargs -P "$MAX_PARALELO" -I{} sh -c \
    "uv run python -m poc.experimento --cenario $1 --repeticao {} \
       --saida '$RAIZ/$1_rep{}' > '$RAIZ/$1_rep{}.log' 2>&1"
}

serie h1 5
serie escala 3
serie falhas 5
serie duplicidade 5
serie seguranca 5
paralelo custo 3
paralelo hibrido 5

uv run python -m poc.analise "$RAIZ"
echo "Pronto. Relatório: $RAIZ/analise/relatorio.md"

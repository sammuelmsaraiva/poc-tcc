# PoC — webhooks, polling e arranjo híbrido

Prova de conceito do TCC *"Integração de sistemas com APIs e webhooks: estudo de caso de uma plataforma de gestão de e-commerce em transição para SaaS e avaliação experimental de webhooks e polling"* (iCev - Instituto de Ensino Superior, 2026).

Autor: Sammuel Moura Saraiva. Código gerado com auxílio de ferramenta de IA e revisado pelo autor (ver a Declaração de Uso de IA da monografia).

Todos os dados são fictícios.

## Como rodar

Requer Python 3.12 e [uv](https://docs.astral.sh/uv/).

> **Atenção:** mantenha o repositório fora de pastas sincronizadas com a nuvem (iCloud, OneDrive etc.). Dentro do Desktop sincronizado pelo iCloud, a suíte levou ~7 min; em `~/dev/poc-tcc`, menos de 1 s. A sincronização distorceria as medições do experimento.

```bash
uv sync --python 3.12
uv run pytest -q

# ensaio curto (tempos ÷ 50, 20 eventos) — só para conferir; NÃO usar no TCC
uv run python -m poc.experimento --cenario todos --repeticoes 1 --rapido

# experimento de verdade (um cenário por vez; resultados em resultados/<data>/)
uv run python -m poc.experimento --cenario h1 --repeticoes 5

# análise (tabelas, gráficos e relatório com o veredito de cada critério)
uv run python -m poc.analise resultados/<pasta>
```

**Execução longa completa (plano da Seção 3.6):** `./executar_plano.sh` — roda dentro do `caffeinate`, H1 e escala em série, custo e híbrido em paralelo (uma pasta por repetição), e no fim gera o relatório.

Antes do experimento de verdade, faça um commit (o executor se recusa a rodar sem ele): cada execução registra o hash do commit (coluna `versao_codigo`).

## Módulos

| Módulo | O que faz | Critérios |
|---|---|---|
| `poc.assinatura` | Assina e verifica webhooks no formato Standard Webhooks (id + horário + corpo, HMAC-SHA256, tolerância de tempo) | C10, C11, C14 |
| `poc.idempotencia` | Registro de identificadores com retenção e capacidade (idempotência) e estado por registro com verificação de ordem | C6, C7 |
| `poc.receptor` | Receptor FastAPI `POST /webhooks/{empresa}`: segredo por empresa, mesmo 404 para empresa inexistente e assinatura inválida (não revela quais empresas existem), idempotência por empresa, 200 "duplicado", 500 em falha de gravação (com nova tentativa aceita), fila interna e verificação de ordem | C4, C6, C7, C10, C11, C14 |
| `poc.provedor` | Provedor simulado: eventos em processo de Poisson com semente, atraso de detecção D, consulta incremental por cursor (P = 1) e releitura de janela paginada (P > 1), cota por aplicativo com 429 + Retry-After, cadastro de destino de webhook | C2, C15; H1, H2 |
| `poc.despachante` | Entrega de webhooks pela origem: nova assinatura a cada tentativa, prazo de resposta, repetição só em 5xx/429/prazo/conexão com espera exponencial e jitter completo (ou Retry-After), 4xx sem repetição, perda induzida p, desativação do destino após N falhas e lista de falhas definitivas | C4, C5, C8; cenários C e G |
| `poc.consultor` | Consultor periódico (incremental por cursor ou releitura de janela; rodada abandonada sem avançar se qualquer requisição falhar; execução em instantes fixos sem deriva) e reconciliador (relê a janela a cada T_R e corrige o estado local ausente ou desatualizado) | C2, C3, C15; H2, H3 |
| `poc.instrumentacao` | Registro em SQLite dos instantes de cada evento (ocorrência, visibilidade, primeiro conhecimento por mecanismo), entregas e contadores; latência = conhecido − ocorrido; resumo (média, mediana, desvio, p95); exportação para CSV | H1–H4 (coleta) |
| `poc.falhas` | Injetor de falhas: middleware ASGI (500 em fração p, resposta lenta) e transporte com janelas de queda (conexão recusada) | cenário C |
| `poc.experimento` | Executor dos cenários (h1, custo, escala, falhas, duplicidade, seguranca, hibrido) com servidores HTTP reais (uvicorn em 127.0.0.1), grade de parâmetros aprovada, sementes, modo `--rapido` e exportação para SQLite/CSV | H1–H4 |
| `poc.analise` | Tabelas por configuração, gráficos (PNG), Mann-Whitney, δ de Cliff, IC 95% por bootstrap, ponto de equilíbrio (H2), N* (H2), razão híbrido/webhook observada × prevista (H3), matriz da H4 e relatório com o veredito de cada critério; marca ENSAIO se os dados vierem do modo rápido | H1–H4 |

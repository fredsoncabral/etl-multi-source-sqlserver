# ETL Multi-Origem (Oracle / SAP HANA / SQL Server → SQL Server)

ETL em Python para replicar tabelas de **Oracle**, **SAP HANA** ou **SQL
Server** em um **SQL Server de destino**, com **fidelidade estrutural
total** (nullability, IDENTITY, colunas computadas, PRIMARY KEY, UNIQUE e
FOREIGN KEY) e carga de dados em alta performance (`fast_executemany`),
preparado para rodar sem supervisão contra bancos de produção: resiliente a
quedas de conexão, com cópia estrutural incremental self-healing e um modo
de reconciliação completa para fechar divergências.

> **Sobre este repositório.** Este projeto nasceu de uma necessidade real
> de replicação de dados entre bancos heterogêneos num ambiente de
> produção, construído e endurecido de forma iterativa contra erros reais
> de execução (não em ambiente de teste isolado). Para publicação como
> portfólio, o código foi **modularizado** a partir do script original
> (um único arquivo de ~2000 linhas) e o módulo de configuração de
> credenciais foi **reconstruído para ler de variáveis de ambiente**,
> removendo qualquer acoplamento com o ambiente interno de origem. Toda a
> lógica de negócio - mapeamento de tipos, introspecção de schema,
> resiliência, reconciliação - é a mesma, linha a linha.

## Por que isso existe

Replicar uma tabela de um banco legado (Oracle/HANA) ou de outro SQL Server
para um data warehouse/réplica em SQL Server parece trivial até você
precisar fazer isso **de verdade, em produção, de novo e de novo**:

- A tabela de destino precisa existir com a estrutura certa - não só os
  tipos de coluna, mas nullability, chaves e relacionamentos.
- Rodar o script de novo não pode duplicar dados nem exigir recriar a
  tabela do zero.
- A conexão de rede com bancos de produção cai. Um ETL que trava ou perde
  progresso a cada "Communication link failure" não é confiável.
- Chaves nem sempre são sequenciais/crescentes - um watermark por `MAX()`
  sozinho não garante que nada ficou pra trás.
- Tabelas com FOREIGN KEY entre si não têm uma ordem de carga óbvia quando
  você está copiando o banco inteiro, tabela por tabela.

Este projeto resolve esses cinco problemas diretamente, como decisões de
design explícitas (veja [`docs/DECISIONS.md`](docs/DECISIONS.md)).

## Principais capacidades

- **Três origens, um destino**: Oracle (via `python-oracledb`), SAP HANA
  (via `hdbcli`) e SQL Server (via `pyodbc`) → sempre SQL Server.
- **Criação de tabela com fidelidade estrutural total**: introspecção nos
  catálogos nativos de cada banco (`ALL_CONSTRAINTS`/`ALL_TAB_IDENTITY_COLS`
  no Oracle, `SYS.CONSTRAINTS`/`SYS.REFERENTIAL_CONSTRAINTS` no HANA,
  `sys.*` no SQL Server), unificada num dispatch table
  (`SCHEMA_EXTRACTORS`).
- **Cópia estrutural incremental ("self-healing")**: se a tabela de destino
  já existe, nunca é recriada por padrão - só recebe as colunas e
  constraints (PK/UNIQUE/FK) que ainda estiverem faltando, via
  `ALTER TABLE`. FKs para tabelas que ainda não existem no destino são
  puladas com aviso e criadas automaticamente assim que a tabela
  referenciada aparecer numa execução futura.
- **Carga literal de dados**: insere todas as colunas tal como estão na
  origem, inclusive PK/FK/SK e colunas IDENTITY (via
  `SET IDENTITY_INSERT`) - as únicas exceções são colunas `computed` e
  `rowversion`, que o próprio SQL Server nunca aceita como valor explícito.
- **Carga incremental por watermark**: usa `MAX(coluna)` já carregado no
  destino; detecta automaticamente uma PK de coluna única quando
  `--incremental-column` não é informado.
- **Modo `--reconcile`**: compara **todas** as chaves entre origem e
  destino (não confia em `MAX`), otimizado para baixo consumo de memória -
  chaves do destino num `array.array` ordenado + busca binária, origem
  sempre em streaming, nunca materializada inteira.
- **`--delete-extra`**: remove do destino linhas que não existem mais na
  origem, pulando automaticamente as que ainda estão protegidas por FK de
  outra tabela.
- **Resiliente a quedas de conexão** em qualquer fase (extração, carga,
  comparação de chaves, exclusão): reconecta com backoff exponencial e
  retoma de onde parou, sem duplicar nem perder linhas já commitadas.
- **Resiliente a limites de `IN (...)`**: lotes grandes demais para a
  cláusula `IN` do banco de origem (ou do driver ODBC) são divididos pela
  metade recursivamente até um tamanho seguro.

## Instalação

```bash
git clone <url-do-seu-fork>
cd etl-multi-source-sqlserver
python -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
```

Você também precisa do **driver ODBC do SQL Server** instalado no sistema
(ex: `ODBC Driver 18 for SQL Server` da Microsoft) e, se for conectar a um
Oracle Database anterior à versão 12.1, do **Oracle Instant Client**.

Copie `.env.example` para `.env` e preencha com as credenciais do seu
ambiente:

```bash
cp .env.example .env
```

## Uso

```bash
# Carga completa inicial (cria a tabela com fidelidade estrutural total)
python -m etl_multi_source --source oracle --table SCHEMA.CLIENTES \
    --target-table CLIENTES

# Carga incremental por coluna de controle (ex: ID crescente)
python -m etl_multi_source --source hana --table SCHEMA.PEDIDOS \
    --target-table PEDIDOS --incremental-column ID_PEDIDO

# Forçar recriação total da tabela de destino
python -m etl_multi_source --source sqlserver --table dbo.LOG \
    --target-table LOG --recreate

# Reconciliação completa (chaves não sequenciais / corrigir divergências)
python -m etl_multi_source --source oracle --table SCHEMA.CLIENTES \
    --target-table CLIENTES --reconcile

# Reconciliação + remoção de linhas que não existem mais na origem
python -m etl_multi_source --source oracle --table SCHEMA.CLIENTES \
    --target-table CLIENTES --reconcile --delete-extra
```

| Flag | Efeito |
|---|---|
| `--source {oracle,hana,sqlserver}` | Banco de origem |
| `--table SCHEMA.TABELA` | Tabela de origem |
| `--target-table NOME` | Tabela a criar/atualizar no destino (sempre em `dbo.`) |
| `--recreate` | Derruba e recria a tabela de destino do zero (carga completa) |
| `--incremental-column COLUNA` | Coluna de controle para carga incremental |
| `--incremental-value VALOR` | Valor inicial do filtro incremental (senão usa `MAX()` do destino) |
| `--reconcile` | Compara todas as chaves em vez de usar watermark |
| `--delete-extra` | Com `--reconcile`: exclui do destino linhas que não existem mais na origem |

Variáveis de ambiente relevantes (ver `.env.example`): `ETL_BATCH_SIZE`
(padrão 100.000 linhas por lote), `ETL_MAX_RETRIES` (padrão 5),
`ETL_RETRY_BASE_DELAY` (padrão 5s, com backoff exponencial até 120s).

## Estrutura do projeto

```
src/etl_multi_source/
├── cli.py                 # argparse + ponto de entrada (`python -m etl_multi_source`)
├── config.py              # configuração via variáveis de ambiente
├── connections.py         # abertura de conexão Oracle/HANA/SQL Server
├── resilience.py          # detecção de queda de conexão + retry com backoff
├── type_mapping.py        # tradução de tipos de cada origem -> T-SQL
├── introspection/
│   ├── oracle.py          # ALL_TAB_COLUMNS, ALL_CONSTRAINTS, ALL_TAB_IDENTITY_COLS...
│   ├── hana.py             # SYS.TABLE_COLUMNS, SYS.CONSTRAINTS, SYS.REFERENTIAL_CONSTRAINTS
│   └── sqlserver.py        # INFORMATION_SCHEMA.COLUMNS, sys.columns, sys.foreign_keys...
├── ddl.py                  # CREATE TABLE + PK/UNIQUE/FK com fidelidade estrutural
├── target.py               # self-healing: ALTER TABLE ADD de colunas/constraints pendentes
├── loader.py                # extração em lote + fast_executemany
├── reconcile.py              # modo --reconcile e --delete-extra
└── orchestration.py           # run() - decide o fluxo e dispara load_with_resume

tests/                        # testes unitários da lógica pura (sem banco real)
docs/
├── ARCHITECTURE.md           # diagrama e explicação do fluxo ponta a ponta
└── DECISIONS.md               # decisões de design e trade-offs, formato ADR
```

## Rodando os testes

Os testes cobrem a lógica que **não depende de uma conexão de banco real**:
mapeamento de tipos, construção de DDL e a detecção de erro de
conexão/retry/comparação de chaves do modo `--reconcile`.

```bash
pip install -r requirements-dev.txt
PYTHONPATH=src pytest -v
```

## Limitações conhecidas (documentadas no próprio código/log)

- Colunas virtuais/calculadas do Oracle e do HANA **não** são traduzidas
  automaticamente para T-SQL (a sintaxe PL/SQL/SQLScript não é portável) -
  viram coluna comum no destino, com a expressão original registrada em
  log para ajuste manual.
- A cópia 1:1 de colunas computadas só é automática quando a **origem
  também é SQL Server** (mesmo dialeto T-SQL).
- `sync_missing_columns` só **adiciona** colunas que faltam; não corrige o
  tipo de uma coluna que já existia no destino com estrutura diferente da
  origem - para isso, use `--recreate`.
- Sem `--incremental-column` nem PK de coluna única detectável, uma carga
  repetida sobre uma tabela já existente não tem como ser filtrada
  automaticamente - o script avisa e insere tudo por cima, o que pode
  gerar chave duplicada.

## Roadmap / próximos passos

- Estender a resiliência de reconexão também para a fase de **streaming
  inicial de comparação de chaves** do `--reconcile` (hoje a reconexão com
  retomada integral já cobre a comparação e as chamadas de refetch por
  chunk; a otimização pendente é redução do custo de reiniciar a
  comparação do zero numa tabela muito grande).
- Para reconciliações muito grandes, avaliar uma abordagem por *temp table*
  + `JOIN` do lado do próprio SQL Server, em vez de comparar em memória no
  cliente.
- Testes de integração contra um SQL Server local (ex: via Testcontainers)
  cobrindo o fluxo completo de `ensure_target_table` + `load_with_resume`.

## Licença

MIT - veja [`LICENSE`](LICENSE).

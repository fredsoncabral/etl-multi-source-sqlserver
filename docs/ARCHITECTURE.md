# Arquitetura

## Visão geral do fluxo

```
                         ┌─────────────────────┐
                         │   cli.py (argparse)  │
                         └──────────┬───────────┘
                                    │
                                    ▼
                      ┌─────────────────────────┐
                      │  orchestration.run(...)  │
                      └────────────┬─────────────┘
                                    │
            ┌───────────────────────┼────────────────────────┐
            ▼                       ▼                         ▼
   conecta ao destino      conecta à origem            lê colunas da origem
   (connections.py)        (connections.py)            (introspection/*.py)
            │                       │                         │
            └───────────┬───────────┴─────────────┬───────────┘
                         ▼                         ▼
              target.ensure_target_table   decide modo de carga:
              (cria OU self-heal da         watermark / --reconcile
               tabela existente)                    │
                         │                           ▼
                         │                  reconcile.reconcile_missing_rows
                         │                     (ou)
                         ▼                  orchestration.load_with_resume
              ddl.create_table_full_fidelity      │
              (PK/UNIQUE/FK via                   ▼
               introspection.SCHEMA_EXTRACTORS)  loader.extract_and_load_*
                                                  (fetchmany + fast_executemany)
```

Toda operação de rede (origem ou destino) passa, direta ou indiretamente,
por `resilience._retry_on_disconnect`: se uma exceção for identificada como
queda de conexão (`resilience._is_connection_error`), a operação é refeita
após reconectar com backoff exponencial; qualquer outro erro (SQL, dados,
constraint) sobe imediatamente, sem mascarar o problema real.

## Por que um dispatch table para introspecção (`SCHEMA_EXTRACTORS`)

Oracle, HANA e SQL Server expõem metadados de schema em catálogos
completamente diferentes (`ALL_CONSTRAINTS` vs `SYS.CONSTRAINTS` vs
`sys.key_constraints`, por exemplo). Em vez de `if/elif` espalhados pelo
código de criação de tabela, cada origem implementa as mesmas 4 funções
(`extras`, `pk`, `unique`, `fks`) com a mesma assinatura `(conn, table)`.
Isso significa que `ddl.create_table_full_fidelity` e
`target.sync_missing_constraints` são **genéricos**: funcionam
identicamente para as três origens, e adicionar uma quarta origem no
futuro (ex: PostgreSQL) significa só implementar um novo módulo em
`introspection/` com essas 4 funções e registrá-lo no dicionário - nenhuma
mudança no restante do pipeline.

## Por que `ReconnectingConnection` em vez de reconectar "inline"

Quando a conexão de origem OU destino cai no meio de uma operação longa
(extração, carga, comparação de chaves), várias funções diferentes
precisam da conexão atual - e a conexão trocada por uma nova a cada
reconexão. `ReconnectingConnection` encapsula "a conexão atual + como
recriá-la" (via factory), para que quem consome sempre acesse `.conn` na
hora do uso em vez de guardar uma referência que pode já estar morta.
`_retry_on_disconnect` recebe uma lista de `ReconnectingConnection`
(às vezes só a de destino, às vezes origem + destino) e reconecta todas
elas antes de repetir a operação, chamando um callback opcional
`on_reconnect` para recriar estado de sessão (cursor, `IDENTITY_INSERT`,
etc.) que não sobrevive à queda.

## Por que o modo `--reconcile` não usa `set()` ingênuo

A primeira versão comparava chaves carregando as duas tabelas inteiras em
memória com `set()`. Para tabelas de dezenas de milhões de linhas isso
estourava memória. A solução adotada:

1. As chaves do **destino** são lidas uma vez e guardadas da forma mais
   compacta possível: se todos os valores forem inteiros (caso comum - ID
   sequencial/IDENTITY), vira um `array.array('q')` **ordenado**
   (~8 bytes por valor, sem o overhead de um objeto Python por chave) e
   localizado por **busca binária** (`bisect`). Só cai para um `set()`
   genérico se aparecer algum valor não inteiro.
2. As chaves da **origem** nunca são materializadas inteiras: são lidas em
   lotes (`fetchmany`) e comparadas contra o array do destino na hora,
   mantendo só a lista de chaves que estão faltando - que tende a ser bem
   menor que a tabela inteira.

## Por que desabilitar FK/CHECK durante o INSERT

Numa cópia tabela-a-tabela de um banco inteiro, não existe garantia de que
a tabela referenciada por uma FK já tenha sido copiada (ou sequer criada)
no destino no momento em que a tabela atual está sendo carregada. Em vez
de impor uma ordem de carga (o que exigiria resolver o grafo de
dependências entre todas as tabelas do banco), o script desabilita a
validação de FK/CHECK durante o `INSERT`
(`ALTER TABLE ... NOCHECK CONSTRAINT ALL`) e a reabilita ao final **sem
revalidar** (`WITH NOCHECK CHECK CONSTRAINT ALL`) - os dados são gravados
fielmente como estão na origem; se a origem já era referencialmente
íntegra, o destino também será, sem pagar o custo de revalidar tudo que
acabou de ser inserido.

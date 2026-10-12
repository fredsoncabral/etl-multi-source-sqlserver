"""Interface de linha de comando do ETL.

Exemplos:

    python -m etl_multi_source --source oracle --table SCHEMA.CLIENTES \\
        --target-table CLIENTES

    python -m etl_multi_source --source hana --table SCHEMA.PEDIDOS \\
        --target-table PEDIDOS --incremental-column ID_PEDIDO

    python -m etl_multi_source --source sqlserver --table dbo.LOG \\
        --target-table LOG --reconcile --delete-extra

Veja o README.md na raiz do repositório para a descrição completa de cada
flag e dos trade-offs de cada modo de carga.
"""
import argparse

from .orchestration import run

def main():
    parser = argparse.ArgumentParser(
        description="ETL: extrai tabela do Oracle/HANA/SQL Server e replica/atualiza em um SQL Server de destino."
    )
    parser.add_argument("--source", choices=["oracle", "hana", "sqlserver"], required=True,
                         help="Banco de origem")
    parser.add_argument("--table", required=True,
                         help="Tabela de origem. Oracle: SCHEMA.TABELA. HANA: SCHEMA.TABELA. "
                              "SQL Server: SCHEMA.TABELA (ex: dbo.TABELA)")
    parser.add_argument("--target-table", required=True,
                         help="Nome da tabela a ser criada/atualizada no SQL Server de destino "
                              "(sem o schema; sempre criada em dbo.). Ex: --target-table CLIENTES")
    parser.add_argument("--recreate", action="store_true",
                         help="Se a tabela de destino já existir, derruba e recria do zero "
                              "(carga completa). Sem essa flag, se a tabela já existir a carga "
                              "é incremental (apenas insere por cima, sem recriar).")
    parser.add_argument("--incremental-column", default=None,
                         help="Coluna de controle (ex: ID, DATA_ATUALIZACAO) usada para trazer da "
                              "origem apenas os registros mais novos que o já carregado no destino "
                              "(evita duplicar linhas em execuções repetidas).")
    parser.add_argument("--incremental-value", default=None,
                         help="Valor inicial a partir do qual filtrar (> valor). Se omitido, o "
                              "script consulta automaticamente o MAX(--incremental-column) já "
                              "existente na tabela de destino.")
    parser.add_argument("--reconcile", action="store_true",
                         help="Em vez da carga normal (incremental por MAX/watermark), compara "
                              "TODAS as chaves entre origem e destino e insere só as linhas que "
                              "estiverem faltando no destino. Mais lento que a carga incremental "
                              "normal, mas não depende de a chave ser sequencial/crescente - use "
                              "para conferir/corrigir divergências depois de cargas anteriores. "
                              "Use --incremental-column para indicar a chave, se não houver PK de "
                              "coluna única.")
    parser.add_argument("--delete-extra", action="store_true",
                         help="Só tem efeito junto com --reconcile. Além de inserir o que estiver "
                              "faltando, EXCLUI do destino as linhas cuja chave não existe mais na "
                              "origem. Operação destrutiva - linhas ainda referenciadas por FK de "
                              "outra tabela são puladas automaticamente (com aviso), não travam a "
                              "exclusão das demais.")
    args = parser.parse_args()
 
    run(
        source=args.source,
        source_table=args.table,
        target_table=args.target_table,
        recreate=args.recreate,
        incremental_column=args.incremental_column,
        incremental_value=args.incremental_value,
        reconcile=args.reconcile,
        delete_extra=args.delete_extra,
    )
 
 
if __name__ == "__main__":
    main()
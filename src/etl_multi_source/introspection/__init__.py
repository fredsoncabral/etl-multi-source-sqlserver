"""Introspecção de schema por banco de origem, unificada num dispatch table.

Cada submódulo (oracle, hana, sqlserver) expõe as MESMAS 4 funções com a
MESMA assinatura (conn, table):
    - extras(conn, table) -> dict de metadados por coluna (nullable,
      identity, coluna computada)
    - pk(conn, table)     -> (nome_constraint, [colunas]) ou None
    - unique(conn, table) -> [(nome_constraint, [colunas]), ...]
    - fks(conn, table)    -> [{name, columns, ref_schema, ref_table,
      ref_columns}, ...]

Isso permite que a criação de tabela com fidelidade estrutural
(ver `ddl.create_table_full_fidelity`) funcione de forma idêntica para
Oracle, HANA e SQL Server, apenas trocando o dicionário de funções usado.
"""
from . import oracle, hana, sqlserver

# Dispatch de introspecção estrutural por origem. Cada uma expõe as mesmas
# 4 funções (extras, pk, unique, fks) com a MESMA assinatura (conn, table).
SCHEMA_EXTRACTORS = {
    "oracle": dict(
        extras=oracle.get_oracle_column_extras,
        pk=oracle.get_oracle_primary_key,
        unique=oracle.get_oracle_unique_constraints,
        fks=oracle.get_oracle_foreign_keys,
    ),
    "hana": dict(
        extras=hana.get_hana_column_extras,
        pk=hana.get_hana_primary_key,
        unique=hana.get_hana_unique_constraints,
        fks=hana.get_hana_foreign_keys,
    ),
    "sqlserver": dict(
        extras=sqlserver.get_sqlserver_column_extras,
        pk=sqlserver.get_sqlserver_primary_key,
        unique=sqlserver.get_sqlserver_unique_constraints,
        fks=sqlserver.get_sqlserver_foreign_keys,
    ),
}

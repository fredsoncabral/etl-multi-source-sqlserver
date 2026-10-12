"""Introspecção de schema de uma tabela SQL Server (origem) via
INFORMATION_SCHEMA.COLUMNS (tipos) e catálogos sys.* (nullability,
IDENTITY, colunas computadas, PRIMARY KEY, UNIQUE, FOREIGN KEY).

Esta é a única origem em que a cópia estrutural é "quase 1:1" (mesmo
dialeto T-SQL da origem e do destino), incluindo a tradução automática de
colunas computadas (fórmula + PERSISTED), o que Oracle/HANA não suportam.
"""
from typing import List, Optional, Tuple

from ..type_mapping import map_sqlserver_type

def get_sqlserver_columns(conn, table: str) -> List[Tuple[str, str]]:
    """table no formato SCHEMA.TABELA (ex: dbo.MINHA_TABELA)."""
    if "." in table:
        schema, tbl = table.split(".", 1)
    else:
        schema, tbl = "dbo", table
 
    cur = conn.cursor()
    cur.execute(
        """
        SELECT COLUMN_NAME, DATA_TYPE, CHARACTER_MAXIMUM_LENGTH,
               NUMERIC_PRECISION, NUMERIC_SCALE
        FROM INFORMATION_SCHEMA.COLUMNS
        WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?
        ORDER BY ORDINAL_POSITION
        """,
        (schema, tbl),
    )
    rows = cur.fetchall()
    cur.close()
    if not rows:
        raise RuntimeError(f"Tabela SQL Server '{table}' não encontrada ou sem colunas.")
 
    columns = []
    for col_name, data_type, char_len, num_prec, num_scale in rows:
        sql_type = map_sqlserver_type(data_type, char_len, num_prec, num_scale)
        columns.append((col_name, sql_type))
    return columns
 
 
# =============================================================================
# CÓPIA ESTRUTURAL COMPLETA (origem SQL Server): nullability, identity,
# colunas computadas, PRIMARY KEY, FOREIGN KEY e UNIQUE constraints.
#
# Isso só é possível de forma automática quando a ORIGEM também é SQL Server,
# pois usamos os catálogos sys.* nativos (sys.columns, sys.key_constraints,
# sys.foreign_keys etc). Para origem Oracle/HANA, a tabela de destino é
# criada com os tipos corretos e NULL (comportamento anterior); replicar
# PK/FK exigiria mapear os catálogos equivalentes de cada banco - avise se
# precisar disso também.
# =============================================================================
 
def get_sqlserver_column_extras(conn, table: str) -> dict:
    """Metadados extras por coluna que o INFORMATION_SCHEMA não expõe:
    nullable, identity (seed/increment) e coluna computada (fórmula +
    PERSISTED)."""
    if "." in table:
        schema, tbl = table.split(".", 1)
    else:
        schema, tbl = "dbo", table
 
    cur = conn.cursor()
    cur.execute(
        """
        SELECT c.name, c.is_nullable, c.is_identity,
               CONVERT(bigint, ISNULL(ic.seed_value, 1)), CONVERT(bigint, ISNULL(ic.increment_value, 1)),
               c.is_computed, cc.definition, ISNULL(cc.is_persisted, 0)
        FROM sys.columns c
        JOIN sys.tables tb ON tb.object_id = c.object_id
        JOIN sys.schemas s ON s.schema_id = tb.schema_id
        LEFT JOIN sys.identity_columns ic ON ic.object_id = c.object_id AND ic.column_id = c.column_id
        LEFT JOIN sys.computed_columns cc ON cc.object_id = c.object_id AND cc.column_id = c.column_id
        WHERE s.name = ? AND tb.name = ?
        ORDER BY c.column_id
        """,
        (schema, tbl),
    )
    rows = cur.fetchall()
    cur.close()
 
    extras = {}
    for name, nullable, is_identity, seed, increment, is_computed, comp_def, is_persisted in rows:
        extras[name] = dict(
            nullable=bool(nullable),
            is_identity=bool(is_identity),
            identity_seed=int(seed),
            identity_increment=int(increment),
            is_computed=bool(is_computed),
            computed_definition=comp_def,
            is_persisted=bool(is_persisted),
        )
    return extras
 
 
def get_sqlserver_primary_key(conn, table: str) -> Optional[Tuple[str, List[str]]]:
    """Retorna (nome_da_constraint, [colunas]) da PK da tabela de origem, ou
    None se a tabela não tiver PK."""
    if "." in table:
        schema, tbl = table.split(".", 1)
    else:
        schema, tbl = "dbo", table
 
    cur = conn.cursor()
    cur.execute(
        """
        SELECT kc.name, c.name
        FROM sys.key_constraints kc
        JOIN sys.tables tb ON tb.object_id = kc.parent_object_id
        JOIN sys.schemas s ON s.schema_id = tb.schema_id
        JOIN sys.index_columns ic ON ic.object_id = kc.parent_object_id AND ic.index_id = kc.unique_index_id
        JOIN sys.columns c ON c.object_id = ic.object_id AND c.column_id = ic.column_id
        WHERE kc.type = 'PK' AND s.name = ? AND tb.name = ?
        ORDER BY ic.key_ordinal
        """,
        (schema, tbl),
    )
    rows = cur.fetchall()
    cur.close()
    if not rows:
        return None
    return rows[0][0], [r[1] for r in rows]
 
 
def get_sqlserver_unique_constraints(conn, table: str) -> List[Tuple[str, List[str]]]:
    """Retorna lista de (nome_da_constraint, [colunas]) para cada UNIQUE
    constraint da tabela de origem."""
    if "." in table:
        schema, tbl = table.split(".", 1)
    else:
        schema, tbl = "dbo", table
 
    cur = conn.cursor()
    cur.execute(
        """
        SELECT kc.name, c.name
        FROM sys.key_constraints kc
        JOIN sys.tables tb ON tb.object_id = kc.parent_object_id
        JOIN sys.schemas s ON s.schema_id = tb.schema_id
        JOIN sys.index_columns ic ON ic.object_id = kc.parent_object_id AND ic.index_id = kc.unique_index_id
        JOIN sys.columns c ON c.object_id = ic.object_id AND c.column_id = ic.column_id
        WHERE kc.type = 'UQ' AND s.name = ? AND tb.name = ?
        ORDER BY kc.name, ic.key_ordinal
        """,
        (schema, tbl),
    )
    rows = cur.fetchall()
    cur.close()
 
    grouped: List[Tuple[str, List[str]]] = []
    for name, col in rows:
        if grouped and grouped[-1][0] == name:
            grouped[-1][1].append(col)
        else:
            grouped.append((name, [col]))
    return grouped
 
 
def get_sqlserver_foreign_keys(conn, table: str) -> List[dict]:
    """Retorna lista de FKs da tabela de origem: cada item tem
    {name, columns, ref_schema, ref_table, ref_columns}."""
    if "." in table:
        schema, tbl = table.split(".", 1)
    else:
        schema, tbl = "dbo", table
 
    cur = conn.cursor()
    cur.execute(
        """
        SELECT fk.name, pc.name, rs.name, rt.name, rc.name
        FROM sys.foreign_keys fk
        JOIN sys.foreign_key_columns fkc ON fkc.constraint_object_id = fk.object_id
        JOIN sys.tables pt ON pt.object_id = fk.parent_object_id
        JOIN sys.schemas ps ON ps.schema_id = pt.schema_id
        JOIN sys.columns pc ON pc.object_id = fkc.parent_object_id AND pc.column_id = fkc.parent_column_id
        JOIN sys.tables rt ON rt.object_id = fk.referenced_object_id
        JOIN sys.schemas rs ON rs.schema_id = rt.schema_id
        JOIN sys.columns rc ON rc.object_id = fkc.referenced_object_id AND rc.column_id = fkc.referenced_column_id
        WHERE ps.name = ? AND pt.name = ?
        ORDER BY fk.name, fkc.constraint_column_id
        """,
        (schema, tbl),
    )
    rows = cur.fetchall()
    cur.close()
 
    grouped: dict = {}
    for fk_name, col, ref_schema, ref_table, ref_col in rows:
        entry = grouped.setdefault(fk_name, {
            "name": fk_name, "columns": [], "ref_schema": ref_schema,
            "ref_table": ref_table, "ref_columns": [],
        })
        entry["columns"].append(col)
        entry["ref_columns"].append(ref_col)
    return list(grouped.values())

"""Introspecção de schema de uma tabela SAP HANA (colunas, tipos,
nullability, PRIMARY KEY, UNIQUE e FOREIGN KEY) via catálogos
SYS.TABLE_COLUMNS / SYS.CONSTRAINTS / SYS.REFERENTIAL_CONSTRAINTS.

HANA também suporta colunas calculadas, mas os metadados da fórmula não são
expostos de forma simples/portável nos catálogos SYS - por ora essas
colunas (se existirem) são copiadas como comuns (nullable) no destino, sem
a tradução automática que existe quando a origem já é SQL Server.

Diferente de Oracle/SQL Server, aqui a tabela DEVE ser informada como
"SCHEMA.TABELA" (HANA não assume um schema implícito por sessão).
"""
from typing import List, Optional, Tuple

def get_hana_columns(conn, table: str) -> List[Tuple[str, str]]:
    """table no formato SCHEMA.TABELA ou "SCHEMA"."TABELA"."""
    clean = table.replace('"', "")
    if "." in clean:
        schema, tbl = clean.split(".", 1)
    else:
        raise ValueError("Para HANA informe a tabela como SCHEMA.TABELA")
 
    cur = conn.cursor()
    cur.execute(
        """
        SELECT COLUMN_NAME, DATA_TYPE_NAME, LENGTH, SCALE, POSITION
        FROM TABLE_COLUMNS
        WHERE SCHEMA_NAME = ? AND TABLE_NAME = ?
        ORDER BY POSITION
        """,
        (schema.upper(), tbl.upper()),
    )
    rows = cur.fetchall()
    cur.close()
    if not rows:
        raise RuntimeError(f"Tabela HANA '{table}' não encontrada ou sem colunas.")
 
    columns = []
    for col_name, data_type, length, scale, _pos in rows:
        sql_type = map_hana_type(data_type, length, scale)
        columns.append((col_name, sql_type))
    return columns
 
 
def _hana_split(table: str):
    clean = table.replace('"', "")
    if "." not in clean:
        raise ValueError("Para HANA informe a tabela como SCHEMA.TABELA")
    schema, tbl = clean.split(".", 1)
    return schema.upper(), tbl.upper()
 
 
def get_hana_column_extras(conn, table: str) -> dict:
    """nullable do HANA. HANA também suporta colunas calculadas, mas a
    metadados de fórmula não são expostos de forma simples e portável nos
    catálogos SYS - por ora essas colunas (se existirem) são copiadas como
    comuns (nullable) no destino."""
    schema, tbl = _hana_split(table)
    cur = conn.cursor()
    cur.execute(
        'SELECT COLUMN_NAME, IS_NULLABLE FROM "SYS"."TABLE_COLUMNS" '
        "WHERE SCHEMA_NAME = ? AND TABLE_NAME = ?",
        (schema, tbl),
    )
    rows = cur.fetchall()
    cur.close()
    return {
        name: dict(nullable=(flag == "TRUE"), is_identity=False, identity_seed=1,
                    identity_increment=1, is_computed=False, computed_definition=None,
                    is_persisted=False)
        for name, flag in rows
    }
 
 
def get_hana_primary_key(conn, table: str) -> Optional[Tuple[str, List[str]]]:
    schema, tbl = _hana_split(table)
    cur = conn.cursor()
    cur.execute(
        'SELECT CONSTRAINT_NAME, COLUMN_NAME FROM "SYS"."CONSTRAINTS" '
        "WHERE SCHEMA_NAME = ? AND TABLE_NAME = ? AND IS_PRIMARY_KEY = 'TRUE' "
        "ORDER BY POSITION",
        (schema, tbl),
    )
    rows = cur.fetchall()
    cur.close()
    if not rows:
        return None
    return rows[0][0], [r[1] for r in rows]
 
 
def get_hana_unique_constraints(conn, table: str) -> List[Tuple[str, List[str]]]:
    schema, tbl = _hana_split(table)
    cur = conn.cursor()
    cur.execute(
        'SELECT CONSTRAINT_NAME, COLUMN_NAME FROM "SYS"."CONSTRAINTS" '
        "WHERE SCHEMA_NAME = ? AND TABLE_NAME = ? AND IS_UNIQUE_KEY = 'TRUE' "
        "AND IS_PRIMARY_KEY = 'FALSE' ORDER BY CONSTRAINT_NAME, POSITION",
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
 
 
def get_hana_foreign_keys(conn, table: str) -> List[dict]:
    schema, tbl = _hana_split(table)
    cur = conn.cursor()
    cur.execute(
        'SELECT CONSTRAINT_NAME, COLUMN_NAME, REFERENCED_SCHEMA_NAME, '
        'REFERENCED_TABLE_NAME, REFERENCED_COLUMN_NAME '
        'FROM "SYS"."REFERENTIAL_CONSTRAINTS" '
        "WHERE SCHEMA_NAME = ? AND TABLE_NAME = ? "
        "ORDER BY CONSTRAINT_NAME, POSITION",
        (schema, tbl),
    )
    rows = cur.fetchall()
    cur.close()
 
    grouped: dict = {}
    for name, col, ref_schema, ref_table, ref_col in rows:
        entry = grouped.setdefault(name, {
            "name": name, "columns": [], "ref_schema": ref_schema,
            "ref_table": ref_table, "ref_columns": [],
        })
        entry["columns"].append(col)
        entry["ref_columns"].append(ref_col)
    return list(grouped.values())

"""Introspecção de schema de uma tabela Oracle (colunas, tipos, nullability,
IDENTITY 12c+, colunas virtuais/computadas, PRIMARY KEY, UNIQUE e FOREIGN
KEY) via catálogos nativos ALL_TAB_COLUMNS / ALL_TAB_IDENTITY_COLS /
ALL_CONSTRAINTS / ALL_CONS_COLUMNS (ou USER_* quando a tabela é informada
sem schema, assumindo o schema do usuário logado).

Colunas virtuais/calculadas usam sintaxe PL/SQL, que não é traduzida
automaticamente para T-SQL: viram coluna comum (nullable) no destino, com
um aviso no log contendo a expressão original para ajuste manual.
"""
import re
from typing import List, Optional, Tuple

from ..type_mapping import map_oracle_type
from ..logging_config import log

def get_oracle_columns(conn, table: str) -> List[Tuple[str, str]]:
    """table no formato SCHEMA.TABELA ou apenas TABELA (schema do usuário logado)."""
    if "." in table:
        schema, tbl = table.split(".", 1)
    else:
        schema, tbl = None, table
 
    cur = conn.cursor()
    if schema:
        cur.execute(
            """
            SELECT column_name, data_type, data_precision, data_scale, char_length
            FROM all_tab_columns
            WHERE owner = :owner AND table_name = :tname
            ORDER BY column_id
            """,
            owner=schema.upper(), tname=tbl.upper(),
        )
    else:
        cur.execute(
            """
            SELECT column_name, data_type, data_precision, data_scale, char_length
            FROM user_tab_columns
            WHERE table_name = :tname
            ORDER BY column_id
            """,
            tname=tbl.upper(),
        )
    rows = cur.fetchall()
    cur.close()
    if not rows:
        raise RuntimeError(f"Tabela Oracle '{table}' não encontrada ou sem colunas.")
 
    columns = []
    for col_name, data_type, precision, scale, length in rows:
        sql_type = map_oracle_type(data_type, precision, scale, length)
        columns.append((col_name, sql_type))
    return columns
 
 
def _oracle_split(table: str):
    if "." in table:
        schema, tbl = table.split(".", 1)
        return schema.upper(), tbl.upper()
    return None, table.upper()
 
 
def get_oracle_column_extras(conn, table: str) -> dict:
    """nullable + identity (12c+) + colunas virtuais/computadas do Oracle.
    Fórmulas de coluna virtual usam sintaxe PL/SQL, que NÃO é traduzida
    automaticamente para T-SQL - a coluna é criada como comum (nullable) no
    destino e um aviso é emitido com a expressão original, para ajuste
    manual se a fórmula precisar ser preservada."""
    schema, tbl = _oracle_split(table)
    cur = conn.cursor()
    if schema:
        cur.execute(
            "SELECT column_name, nullable FROM all_tab_columns WHERE owner = :o AND table_name = :t",
            o=schema, t=tbl,
        )
    else:
        cur.execute(
            "SELECT column_name, nullable FROM user_tab_columns WHERE table_name = :t", t=tbl,
        )
    extras = {
        name: dict(nullable=(flag == "Y"), is_identity=False, identity_seed=1,
                    identity_increment=1, is_computed=False, computed_definition=None,
                    is_persisted=False)
        for name, flag in cur.fetchall()
    }
    cur.close()
 
    # Identity columns (Oracle 12c+). Ignora silenciosamente em versões
    # antigas que não têm essas views.
    try:
        cur = conn.cursor()
        if schema:
            cur.execute(
                "SELECT column_name, identity_options FROM all_tab_identity_cols "
                "WHERE owner = :o AND table_name = :t",
                o=schema, t=tbl,
            )
        else:
            cur.execute(
                "SELECT column_name, identity_options FROM user_tab_identity_cols "
                "WHERE table_name = :t",
                t=tbl,
            )
        for name, opts in cur.fetchall():
            if name not in extras:
                continue
            seed, increment = 1, 1
            if opts:
                m = re.search(r"START WITH:\s*(-?\d+)", opts)
                if m:
                    seed = int(m.group(1))
                m = re.search(r"INCREMENT BY:\s*(-?\d+)", opts)
                if m:
                    increment = int(m.group(1))
            extras[name]["is_identity"] = True
            extras[name]["identity_seed"] = seed
            extras[name]["identity_increment"] = increment
        cur.close()
    except Exception:
        pass
 
    # Colunas virtuais/computadas
    try:
        cur = conn.cursor()
        if schema:
            cur.execute(
                "SELECT column_name, data_default FROM all_tab_cols "
                "WHERE owner = :o AND table_name = :t AND virtual_column = 'YES'",
                o=schema, t=tbl,
            )
        else:
            cur.execute(
                "SELECT column_name, data_default FROM user_tab_cols "
                "WHERE table_name = :t AND virtual_column = 'YES'",
                t=tbl,
            )
        for name, default in cur.fetchall():
            if name not in extras:
                continue
            expr = default.read() if hasattr(default, "read") else default
            log.warning(
                "Coluna '%s' é uma coluna VIRTUAL/computada no Oracle (expressão: %s). "
                "A sintaxe PL/SQL não é traduzida automaticamente para T-SQL, então ela "
                "será criada como coluna comum (nullable) no destino. Ajuste manualmente "
                "com ALTER TABLE ... ADD [%s] AS (<expressão T-SQL>) se precisar do cálculo.",
                name, expr, name,
            )
        cur.close()
    except Exception:
        pass
 
    return extras
 
 
def get_oracle_primary_key(conn, table: str) -> Optional[Tuple[str, List[str]]]:
    schema, tbl = _oracle_split(table)
    cur = conn.cursor()
    if schema:
        cur.execute(
            """
            SELECT ac.constraint_name, acc.column_name
            FROM all_constraints ac
            JOIN all_cons_columns acc ON acc.owner = ac.owner AND acc.constraint_name = ac.constraint_name
            WHERE ac.constraint_type = 'P' AND ac.owner = :o AND ac.table_name = :t
            ORDER BY acc.position
            """,
            o=schema, t=tbl,
        )
    else:
        cur.execute(
            """
            SELECT uc.constraint_name, ucc.column_name
            FROM user_constraints uc
            JOIN user_cons_columns ucc ON ucc.constraint_name = uc.constraint_name
            WHERE uc.constraint_type = 'P' AND uc.table_name = :t
            ORDER BY ucc.position
            """,
            t=tbl,
        )
    rows = cur.fetchall()
    cur.close()
    if not rows:
        return None
    return rows[0][0], [r[1] for r in rows]
 
 
def get_oracle_unique_constraints(conn, table: str) -> List[Tuple[str, List[str]]]:
    schema, tbl = _oracle_split(table)
    cur = conn.cursor()
    if schema:
        cur.execute(
            """
            SELECT ac.constraint_name, acc.column_name
            FROM all_constraints ac
            JOIN all_cons_columns acc ON acc.owner = ac.owner AND acc.constraint_name = ac.constraint_name
            WHERE ac.constraint_type = 'U' AND ac.owner = :o AND ac.table_name = :t
            ORDER BY ac.constraint_name, acc.position
            """,
            o=schema, t=tbl,
        )
    else:
        cur.execute(
            """
            SELECT uc.constraint_name, ucc.column_name
            FROM user_constraints uc
            JOIN user_cons_columns ucc ON ucc.constraint_name = uc.constraint_name
            WHERE uc.constraint_type = 'U' AND uc.table_name = :t
            ORDER BY uc.constraint_name, ucc.position
            """,
            t=tbl,
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
 
 
def get_oracle_foreign_keys(conn, table: str) -> List[dict]:
    schema, tbl = _oracle_split(table)
    cur = conn.cursor()
    if schema:
        cur.execute(
            """
            SELECT ac.constraint_name, acc.column_name, ac.r_owner, rc.table_name, racc.column_name
            FROM all_constraints ac
            JOIN all_cons_columns acc ON acc.owner = ac.owner AND acc.constraint_name = ac.constraint_name
            JOIN all_constraints rc ON rc.owner = ac.r_owner AND rc.constraint_name = ac.r_constraint_name
            JOIN all_cons_columns racc ON racc.owner = rc.owner AND racc.constraint_name = rc.constraint_name
                 AND racc.position = acc.position
            WHERE ac.constraint_type = 'R' AND ac.owner = :o AND ac.table_name = :t
            ORDER BY ac.constraint_name, acc.position
            """,
            o=schema, t=tbl,
        )
    else:
        cur.execute(
            """
            SELECT uc.constraint_name, ucc.column_name, uc.r_owner, rc.table_name, racc.column_name
            FROM user_constraints uc
            JOIN user_cons_columns ucc ON ucc.constraint_name = uc.constraint_name
            JOIN all_constraints rc ON rc.owner = uc.r_owner AND rc.constraint_name = uc.r_constraint_name
            JOIN all_cons_columns racc ON racc.owner = rc.owner AND racc.constraint_name = rc.constraint_name
                 AND racc.position = ucc.position
            WHERE uc.constraint_type = 'R' AND uc.table_name = :t
            ORDER BY uc.constraint_name, ucc.position
            """,
            t=tbl,
        )
    rows = cur.fetchall()
    cur.close()
 
    grouped: dict = {}
    for name, col, ref_owner, ref_table, ref_col in rows:
        entry = grouped.setdefault(name, {
            "name": name, "columns": [], "ref_schema": ref_owner,
            "ref_table": ref_table, "ref_columns": [],
        })
        entry["columns"].append(col)
        entry["ref_columns"].append(ref_col)
    return list(grouped.values())

"""Construção do DDL de criação de tabela e das constraints (PRIMARY KEY,
UNIQUE, FOREIGN KEY) com fidelidade estrutural total à origem.

`create_table_full_fidelity` é o ponto de entrada: usa o dispatch table
`introspection.SCHEMA_EXTRACTORS` para funcionar de forma idêntica com
Oracle, HANA ou SQL Server como origem. FKs cuja tabela referenciada ainda
não existe no destino são puladas com aviso (SQL Server não permite FK para
tabela inexistente) - ver `target.sync_missing_constraints` para como essas
FKs pendentes são criadas automaticamente numa execução futura.
"""
import pyodbc
from typing import List, Optional, Tuple

from .introspection import SCHEMA_EXTRACTORS
from .logging_config import log

def build_create_table_ddl(target_table: str, columns: List[Tuple[str, str]], extras: dict) -> str:
    """Monta o CREATE TABLE já respeitando nullability, IDENTITY e colunas
    computadas (a PK/FK/UNIQUE são adicionadas depois via ALTER TABLE)."""
    col_lines = []
    for name, sql_type in columns:
        ex = extras.get(name, {})
        if ex.get("is_computed"):
            definition = ex.get("computed_definition") or "NULL"
            persisted = " PERSISTED" if ex.get("is_persisted") else ""
            col_lines.append(f"[{name}] AS ({definition}){persisted}")
            continue
 
        identity_clause = ""
        if ex.get("is_identity"):
            identity_clause = f" IDENTITY({ex['identity_seed']},{ex['identity_increment']})"
        nullable_clause = "NULL" if ex.get("nullable", True) else "NOT NULL"
        col_lines.append(f"[{name}] {sql_type}{identity_clause} {nullable_clause}")
 
    return f"CREATE TABLE dbo.{target_table} (\n    " + ",\n    ".join(col_lines) + "\n);"
 
 
def add_table_constraints(conn, target_table: str,
                           primary_key: Optional[Tuple[str, List[str]]],
                           unique_constraints: List[Tuple[str, List[str]]],
                           foreign_keys: List[dict]) -> None:
    """Recria PK, UNIQUE e FK na tabela de destino com os MESMOS nomes de
    constraint da origem (cópia literal). FKs cuja tabela referenciada ainda
    não existe no destino são puladas com um aviso - o SQL Server não deixa
    criar uma FK apontando para uma tabela inexistente; a coluna continua
    sendo copiada normalmente, só sem a validação de integridade referencial
    até que a tabela referenciada também seja copiada para o destino."""
    cur = conn.cursor()
 
    if primary_key:
        pk_name, pk_cols = primary_key
        cols_sql = ", ".join(f"[{c}]" for c in pk_cols)
        log.info("Criando PRIMARY KEY '%s' em dbo.%s (%s)...", pk_name, target_table, cols_sql)
        cur.execute(f"ALTER TABLE dbo.{target_table} ADD CONSTRAINT [{pk_name}] PRIMARY KEY ({cols_sql});")
 
    for uq_name, uq_cols in unique_constraints:
        cols_sql = ", ".join(f"[{c}]" for c in uq_cols)
        log.info("Criando UNIQUE constraint '%s' em dbo.%s (%s)...", uq_name, target_table, cols_sql)
        cur.execute(f"ALTER TABLE dbo.{target_table} ADD CONSTRAINT [{uq_name}] UNIQUE ({cols_sql});")
 
    conn.commit()
 
    for fk in foreign_keys:
        cur.execute("SELECT OBJECT_ID(?, 'U')", (f"dbo.{fk['ref_table']}",))
        ref_exists = cur.fetchone()[0] is not None
        if not ref_exists:
            log.warning(
                "FK '%s' (%s) referencia a tabela '%s', que ainda não existe no "
                "destino - a constraint NÃO foi criada (a(s) coluna(s) %s continuam "
                "sendo copiadas normalmente, só sem a validação de integridade "
                "referencial). Copie '%s' para o destino e rode de novo para a FK "
                "ser criada.",
                fk["name"], target_table, fk["ref_table"], ", ".join(fk["columns"]), fk["ref_table"],
            )
            continue
 
        cols_sql = ", ".join(f"[{c}]" for c in fk["columns"])
        ref_cols_sql = ", ".join(f"[{c}]" for c in fk["ref_columns"])
        log.info("Criando FOREIGN KEY '%s' em dbo.%s -> dbo.%s (%s)...",
                  fk["name"], target_table, fk["ref_table"], cols_sql)
        try:
            cur.execute(
                f"ALTER TABLE dbo.{target_table} ADD CONSTRAINT [{fk['name']}] "
                f"FOREIGN KEY ({cols_sql}) REFERENCES dbo.{fk['ref_table']} ({ref_cols_sql});"
            )
            conn.commit()
        except pyodbc.Error as exc:
            log.warning("Não foi possível criar a FK '%s': %s. Seguindo sem essa constraint.", fk["name"], exc)
 
    cur.close()

def create_table_full_fidelity(dst_conn, target_table: str, src_conn, source_table: str,
                                columns: List[Tuple[str, str]], source: str) -> None:
    """Cria a tabela de destino replicando fielmente a estrutura da origem
    (Oracle, HANA ou SQL Server): nullability, IDENTITY (quando aplicável),
    colunas computadas (só traduzidas automaticamente quando a origem
    também é SQL Server - mesmo dialeto T-SQL), PRIMARY KEY, UNIQUE e
    FOREIGN KEY (quando a tabela referenciada já existir no destino)."""
    extractors = SCHEMA_EXTRACTORS[source]
    extras = extractors["extras"](src_conn, source_table)
    primary_key = extractors["pk"](src_conn, source_table)
    unique_constraints = extractors["unique"](src_conn, source_table)
    foreign_keys = extractors["fks"](src_conn, source_table)
 
    ddl = build_create_table_ddl(target_table, columns, extras)
    log.info(
        "Criando tabela dbo.%s com estrutura completa da origem (%s): "
        "nullability, %d coluna(s) computada(s), PK%s, %d UNIQUE, %d FK...",
        target_table, source,
        sum(1 for e in extras.values() if e.get("is_computed")),
        " " + primary_key[0] if primary_key else " nenhuma",
        len(unique_constraints),
        len(foreign_keys),
    )
    log.debug("DDL:\n%s", ddl)
 
    cur = dst_conn.cursor()
    cur.execute(ddl)
    dst_conn.commit()
    cur.close()
 
    add_table_constraints(dst_conn, target_table, primary_key, unique_constraints, foreign_keys)

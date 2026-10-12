"""Extração da origem e carga em lote no SQL Server de destino, via
fetchmany + pyodbc `fast_executemany` (uma única viagem ODBC por lote).

Por padrão, TODAS as colunas são inseridas literalmente - inclusive
PRIMARY KEY, FOREIGN KEY, SURROGATE KEY e colunas IDENTITY (via
SET IDENTITY_INSERT). As únicas exceções são colunas computed ou
rowversion/timestamp no destino, que o próprio SQL Server proíbe receber
valor explícito - detectadas e puladas automaticamente.

Durante o INSERT, FOREIGN KEY/CHECK são desabilitadas
(`NOCHECK CONSTRAINT ALL`) e reabilitadas sem revalidar
(`WITH NOCHECK CHECK CONSTRAINT ALL`) ao final: numa cópia literal
tabela-a-tabela não é garantido que a tabela referenciada por uma FK já
tenha sido copiada por completo no destino nesse momento.
"""
import os
import time
from typing import Any, List, Optional, Tuple

from .logging_config import log

BATCH_SIZE = int(os.getenv("ETL_BATCH_SIZE", "100000"))

# =============================================================================
# EXTRAÇÃO + CARGA EM LOTE
# =============================================================================
 
def extract_and_load_oracle(src_conn, sql_server_conn, source_table: str,
                             target_table: str, columns: List[Tuple[str, str]],
                             incremental_column: Optional[str] = None,
                             since_value: Any = None):
    cur = src_conn.cursor()
    cur.arraysize = BATCH_SIZE  # fetch em lote reduz round-trips na rede
 
    col_names = ", ".join(c[0] for c in columns)
    sql = f"SELECT {col_names} FROM {source_table}"
    bind_params = {}
    if incremental_column and since_value is not None:
        sql += f" WHERE {incremental_column} > :since_value"
        bind_params["since_value"] = since_value
        log.info("Carga incremental: filtrando %s > %s", incremental_column, since_value)
    if incremental_column:
        # ordem garantida -> retomada por MAX(coluna) não pula linhas
        sql += f" ORDER BY {incremental_column}"

    cur.execute(sql, bind_params) if bind_params else cur.execute(sql)
    log.info("Extraindo dados da origem Oracle (%s)... isso pode demorar em tabelas grandes.", source_table)
 
    _bulk_insert_loop(cur, sql_server_conn, target_table, [c[0] for c in columns])
    cur.close()
 
 
def extract_and_load_hana(src_conn, sql_server_conn, source_table: str,
                           target_table: str, columns: List[Tuple[str, str]],
                           incremental_column: Optional[str] = None,
                           since_value: Any = None):
    cur = src_conn.cursor()
 
    col_names = ", ".join(f'"{c[0]}"' for c in columns)
    sql = f"SELECT {col_names} FROM {source_table}"
    params = ()
    if incremental_column and since_value is not None:
        sql += f' WHERE "{incremental_column}" > ?'
        params = (since_value,)
        log.info("Carga incremental: filtrando %s > %s", incremental_column, since_value)
    if incremental_column:
        # ordem garantida -> retomada por MAX(coluna) não pula linhas
        sql += f' ORDER BY "{incremental_column}"'
 
    cur.execute(sql, params) if params else cur.execute(sql)
    log.info("Extraindo dados da origem HANA (%s)... isso pode demorar em tabelas grandes.", source_table)
 
    _bulk_insert_loop(cur, sql_server_conn, target_table, [c[0] for c in columns],
                       fetch_batch=cur.fetchmany)
    cur.close()
 
 
def extract_and_load_sqlserver(src_conn, sql_server_conn, source_table: str,
                                target_table: str, columns: List[Tuple[str, str]],
                                incremental_column: Optional[str] = None,
                                since_value: Any = None):
    """Origem = outro SQL Server. Usa fetchmany + fast_executemany, igual às
    demais origens, para manter o mesmo padrão de performance/memória.
 
    Usa WITH (NOLOCK) no SELECT da origem para não ficar esperando locks de
    outras transações na tabela de origem (comum em bancos de produção
    movimentados) - trade-off aceito em cenários de ETL: pode ler dados de
    uma transação ainda não confirmada (dirty read), mas evita o processo
    travar esperando um lock liberar. Se precisar de leitura 100% consistente
    (sem dirty reads), remova o WITH (NOLOCK) abaixo - só rode fora do
    horário de pico nesse caso."""
    cur = src_conn.cursor()
    cur.arraysize = BATCH_SIZE
 
    cols_quoted = ", ".join(f"[{c[0]}]" for c in columns)
    sql = f"SELECT {cols_quoted} FROM {source_table} WITH (NOLOCK)"
    params = ()
    if incremental_column and since_value is not None:
        sql += f" WHERE [{incremental_column}] > ?"
        params = (since_value,)
        log.info("Carga incremental: filtrando %s > %s", incremental_column, since_value)
    if incremental_column:
        # ordem garantida -> retomada por MAX(coluna) não pula linhas
        sql += f" ORDER BY [{incremental_column}]"
 
    cur.execute(sql, params) if params else cur.execute(sql)
    log.info("Extraindo dados da origem SQL Server (%s)... isso pode demorar em tabelas grandes.", source_table)
 
    _bulk_insert_loop(cur, sql_server_conn, target_table, [c[0] for c in columns])
    cur.close()
 
 
def get_identity_column(conn, target_table: str) -> Optional[str]:
    """Retorna o nome da coluna IDENTITY da tabela de destino, se existir.
    O SQL Server não permite inserir valor explícito em coluna IDENTITY a
    menos que IDENTITY_INSERT esteja ligado para aquela tabela na sessão."""
    cur = conn.cursor()
    cur.execute(
        """
        SELECT c.name
        FROM sys.columns c
        WHERE c.object_id = OBJECT_ID(?) AND c.is_identity = 1
        """,
        (f"dbo.{target_table}",),
    )
    row = cur.fetchone()
    cur.close()
    return row[0] if row else None
 
 
def get_non_insertable_columns(conn, target_table: str) -> set:
    """Colunas que o SQL Server NUNCA aceita valor explícito no INSERT,
    mesmo com IDENTITY_INSERT ON: colunas computadas (is_computed=1) e
    colunas rowversion/timestamp (geradas automaticamente pelo motor).
    Essas são as ÚNICAS exceções à cópia integral - todo o resto (comum,
    PK, FK, SK, identity) é sempre inserido com o valor vindo da origem."""
    cur = conn.cursor()
    cur.execute(
        """
        SELECT c.name
        FROM sys.columns c
        JOIN sys.types t ON t.user_type_id = c.user_type_id
        WHERE c.object_id = OBJECT_ID(?) AND (c.is_computed = 1 OR t.name = 'timestamp')
        """,
        (f"dbo.{target_table}",),
    )
    rows = cur.fetchall()
    cur.close()
    return {r[0] for r in rows}
 
 
def _prepare_bulk_insert(sql_server_conn, target_table: str, col_names: List[str]):
    """Prepara o INSERT (monta SQL, detecta colunas não-inseríveis e
    IDENTITY) e desabilita a validação de FK/CHECK durante a carga (cópia
    literal - ver nota em _bulk_insert_loop). Retorna
    (dst_cursor, insert_sql, keep_idx, needs_identity_insert)."""
    dst_cursor = sql_server_conn.cursor()
    dst_cursor.fast_executemany = True  # chave da performance no pyodbc
 
    non_insertable = get_non_insertable_columns(sql_server_conn, target_table)
    insert_cols = [c for c in col_names if c not in non_insertable]
    skipped = [c for c in col_names if c in non_insertable]
    if skipped:
        log.warning(
            "Colunas computed/rowversion no destino não recebem valor explícito "
            "do SQL Server e serão puladas no INSERT (geradas automaticamente): %s",
            ", ".join(skipped),
        )
    keep_idx = [i for i, c in enumerate(col_names) if c not in non_insertable]
 
    placeholders = ", ".join("?" for _ in insert_cols)
    cols_quoted = ", ".join(f"[{c}]" for c in insert_cols)
    insert_sql = f"INSERT INTO dbo.{target_table} ({cols_quoted}) VALUES ({placeholders})"
 
    identity_col = get_identity_column(sql_server_conn, target_table)
    needs_identity_insert = identity_col is not None and identity_col in insert_cols
    if needs_identity_insert:
        log.info("Coluna IDENTITY detectada (%s) - habilitando IDENTITY_INSERT durante a carga.", identity_col)
        dst_cursor.execute(f"SET IDENTITY_INSERT dbo.{target_table} ON;")
 
    # Cópia literal: os dados vêm da origem exatamente como estão lá, e não
    # podemos garantir que a tabela referenciada por uma FK já tenha sido
    # copiada por completo (ou até exista) no destino nesse momento. Por
    # isso, desabilita a validação de FOREIGN KEY/CHECK durante o INSERT -
    # os dados continuam sendo gravados fielmente; a validação volta a
    # valer para inserts futuros, sem revalidar o que acabou de ser
    # carregado (WITH NOCHECK), evitando cair no mesmo erro ao reabilitar.
    dst_cursor.execute(f"ALTER TABLE dbo.{target_table} NOCHECK CONSTRAINT ALL;")
 
    return dst_cursor, insert_sql, keep_idx, needs_identity_insert
 
 
def _finalize_bulk_insert(dst_cursor, sql_server_conn, target_table: str, needs_identity_insert: bool):
    dst_cursor.execute(f"ALTER TABLE dbo.{target_table} WITH NOCHECK CHECK CONSTRAINT ALL;")
    sql_server_conn.commit()
    if needs_identity_insert:
        dst_cursor.execute(f"SET IDENTITY_INSERT dbo.{target_table} OFF;")
        sql_server_conn.commit()
    dst_cursor.close()
 
 
def _bulk_insert_loop(src_cursor, sql_server_conn, target_table: str,
                       col_names: List[str], fetch_batch=None):
    """Lê a origem em lotes (BATCH_SIZE) e insere no SQL Server usando
    fast_executemany (uma única viagem ODBC por lote).
 
    Por padrão TODAS as colunas de col_names são inseridas literalmente,
    inclusive PK, FK, SK e colunas identity (via IDENTITY_INSERT). As únicas
    exceções são colunas computed ou rowversion/timestamp no destino, que o
    próprio SQL Server proíbe receber valor explícito - essas são detectadas
    e puladas automaticamente (o valor é gerado pelo motor de qualquer forma).
    """
    dst_cursor, insert_sql, keep_idx, needs_identity_insert = _prepare_bulk_insert(
        sql_server_conn, target_table, col_names,
    )
 
    fetch_batch = fetch_batch or (lambda n: src_cursor.fetchmany(n))
 
    total = 0
    start = time.time()
    try:
        while True:
            rows = fetch_batch(BATCH_SIZE) if fetch_batch is not src_cursor.fetchmany else src_cursor.fetchmany(BATCH_SIZE)
            if not rows:
                break
            if len(keep_idx) == len(col_names):
                rows = [tuple(r) for r in rows]
            else:
                rows = [tuple(r[i] for i in keep_idx) for r in rows]
            dst_cursor.executemany(insert_sql, rows)
            sql_server_conn.commit()
            total += len(rows)
            log.info("  -> %d linhas inseridas (lote de %d)", total, len(rows))
    finally:
        _finalize_bulk_insert(dst_cursor, sql_server_conn, target_table, needs_identity_insert)
 
    elapsed = time.time() - start
    log.info("Carga concluída: %d linhas em %.1fs (%.0f linhas/s)",
              total, elapsed, total / elapsed if elapsed > 0 else 0)

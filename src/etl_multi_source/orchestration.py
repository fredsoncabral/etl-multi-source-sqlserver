"""Orquestração de uma execução do ETL: resolve a origem (--source),
decide entre carga normal (watermark incremental) e `--reconcile`, garante
a tabela de destino e dispara a extração+carga com retomada automática em
caso de queda de conexão.

`load_with_resume` depende do SELECT da origem vir ordenado pela coluna
incremental (garantido pelas funções `extract_and_load_*` de `loader.py`):
se a conexão cair no meio, reconecta origem e destino, relê o
MAX(incremental_column) já gravado no destino e continua dali - cada lote é
commitado, então nada se perde nem duplica. O contador de falhas zera
sempre que a carga avança, então cargas longas aguentam várias quedas
espaçadas; só desiste após MAX_RETRIES quedas seguidas sem nenhum lote
novo.
"""
import time
from typing import Any, List, Optional, Tuple

from .logging_config import log
from .config import OracleConfig, HanaConfig, SqlServerConfig, SqlServerSourceConfig
from .connections import connect_oracle, connect_hana, connect_sqlserver, connect_sqlserver_source
from .resilience import ReconnectingConnection, _is_connection_error, _safe_close, _retry_on_disconnect, MAX_RETRIES, RETRY_BASE_DELAY, RETRY_MAX_DELAY
from .introspection import SCHEMA_EXTRACTORS
from .introspection.oracle import get_oracle_columns
from .introspection.hana import get_hana_columns
from .introspection.sqlserver import get_sqlserver_columns
from .target import ensure_target_table, get_target_max_value, get_primary_key_column
from .loader import extract_and_load_oracle, extract_and_load_hana, extract_and_load_sqlserver
from .reconcile import reconcile_missing_rows

# =============================================================================
# ORQUESTRAÇÃO
# =============================================================================
 
def load_with_resume(extract_fn, src_ref: ReconnectingConnection, dst_ref: ReconnectingConnection,
                     source_table: str, target_table: str, columns: List[Tuple[str, str]],
                     incremental_column: str, since_value: Any = None):
    """Executa a carga e, se a conexão (origem ou destino) cair no meio,
    reconecta as duas, lê o MAX(incremental_column) já gravado no destino e
    continua dali (cada lote é commitado, então nada se perde nem duplica).
    Depende do SELECT da origem vir ordenado pela coluna (ORDER BY nas
    funções extract_and_load_*). O contador de falhas zera sempre que a
    carga avança, então cargas longas aguentam várias quedas espaçadas -
    só desiste após MAX_RETRIES quedas seguidas sem nenhum lote novo."""
    failures = 0
    while True:
        try:
            extract_fn(src_ref.conn, dst_ref.conn, source_table, target_table, columns,
                       incremental_column=incremental_column, since_value=since_value)
            return
        except Exception as exc:
            if not _is_connection_error(exc):
                raise
            last_exc = exc

        failures += 1
        if failures > MAX_RETRIES:
            log.error("Conexão perdida durante a carga e não foi possível retomar após %d "
                      "tentativa(s) seguidas sem progresso.", MAX_RETRIES)
            raise last_exc
        delay = min(RETRY_BASE_DELAY * 2 ** (failures - 1), RETRY_MAX_DELAY)
        log.warning("Conexão perdida durante a carga (%s: %s) - tentativa %d/%d de retomada em %.0fs...",
                    type(last_exc).__name__, last_exc, failures, MAX_RETRIES, delay)
        time.sleep(delay)

        def reconnect_and_get_max(attempt):
            src_ref.reconnect()
            dst_ref.reconnect()
            return get_target_max_value(dst_ref.conn, target_table, incremental_column)

        new_since = _retry_on_disconnect(reconnect_and_get_max, [], "reconectar para retomar a carga")
        if new_since != since_value:
            failures = 0  # houve progresso desde a última queda
        since_value = new_since
        log.info("Retomando carga a partir de %s > %s", incremental_column, since_value)



def run(source: str, source_table: str, target_table: str,
        recreate: bool = False,
        incremental_column: Optional[str] = None,
        incremental_value: Any = None,
        reconcile: bool = False,
        delete_extra: bool = False):
 
    sql_cfg = SqlServerConfig()
    sql_factory = lambda: connect_sqlserver(sql_cfg)
    sql_conn = sql_factory()

    try:
        if source == "oracle":
            ora_cfg = OracleConfig()
            src_factory = lambda: connect_oracle(ora_cfg)
            get_columns_fn = get_oracle_columns
            extract_fn = extract_and_load_oracle
        elif source == "hana":
            hana_cfg = HanaConfig()
            src_factory = lambda: connect_hana(hana_cfg)
            get_columns_fn = get_hana_columns
            extract_fn = extract_and_load_hana
        elif source == "sqlserver":
            src_cfg = SqlServerSourceConfig()
            src_factory = lambda: connect_sqlserver_source(src_cfg)
            get_columns_fn = get_sqlserver_columns
            extract_fn = extract_and_load_sqlserver
        else:
            raise ValueError("source deve ser 'oracle', 'hana' ou 'sqlserver'")
        src_conn = src_factory()

        try:
            log.info("Lendo schema da tabela de origem '%s' (%s)...", source_table, source)
            columns = get_columns_fn(src_conn, source_table)
 
            # Cria a tabela de destino apenas se ela ainda não existir (ou se
            # --recreate for usado). Se já existir, mantemos a estrutura e
            # apenas inserimos por cima (carga incremental).
            just_created = ensure_target_table(
                sql_conn, target_table, columns, recreate,
                source=source, src_conn=src_conn, source_table=source_table,
            )
 
            since_value = None
            if reconcile:
                # Modo de comparação/reconciliação: ignora a lógica de
                # watermark (MAX/incremental-value) e compara TODAS as
                # chaves entre origem e destino, inserindo só o que faltar.
                key_column = incremental_column
                if not key_column:
                    extractors = SCHEMA_EXTRACTORS.get(source)
                    pk = extractors["pk"](src_conn, source_table) if extractors else None
                    if pk and len(pk[1]) == 1:
                        key_column = pk[1][0]
                if not key_column:
                    log.error(
                        "--reconcile precisa de uma chave de coluna única para comparar as "
                        "linhas. Não foi possível detectar uma PK de coluna única em '%s' - "
                        "informe explicitamente com --incremental-column <coluna>.",
                        source_table,
                    )
                    return
                src_ref = ReconnectingConnection("origem", src_factory, src_conn)
                dst_ref = ReconnectingConnection("destino", sql_factory, sql_conn)
                try:
                    reconcile_missing_rows(src_ref, dst_ref, source_table, target_table,
                                            columns, key_column, source, delete_extra=delete_extra)
                finally:
                    src_ref.close_if_replaced()
                    dst_ref.close_if_replaced()
                return
 
            if incremental_column:
                if just_created:
                    # tabela nova -> carga completa, sem filtro
                    log.info("Tabela recém-criada: carga completa (sem filtro incremental).")
                elif incremental_value is not None:
                    since_value = incremental_value
                    log.info("Usando --incremental-value informado: %s", since_value)
                else:
                    since_value = get_target_max_value(sql_conn, target_table, incremental_column)
                    log.info("MAX(%s) já carregado no destino: %s", incremental_column, since_value)
 
            elif not just_created:
                # Rede de segurança: a tabela já existia e o usuário NÃO pediu
                # carga incremental explicitamente. Inserir tudo de novo por
                # cima causaria erro de chave duplicada (ou duplicaria dados
                # em tabelas sem PK). Tenta detectar uma PK de coluna única e
                # usá-la automaticamente como filtro incremental.
                auto_pk = get_primary_key_column(sql_conn, target_table)
                if auto_pk:
                    incremental_column = auto_pk
                    since_value = get_target_max_value(sql_conn, target_table, auto_pk)
                    log.info(
                        "Nenhuma --incremental-column informada, mas a tabela já existia. "
                        "Detectada chave primária '%s' - usando-a automaticamente para a carga "
                        "incremental (MAX atual no destino: %s). Para outro comportamento, use "
                        "--incremental-column <coluna> ou --recreate.", auto_pk, since_value,
                    )
                else:
                    log.warning(
                        "A tabela já existia e nenhuma --incremental-column foi informada. Não foi "
                        "possível detectar automaticamente uma chave primária de coluna única para "
                        "usar como filtro. A carga será um INSERT completo por cima dos dados "
                        "existentes, o que pode causar erro de chave duplicada ou duplicar registros. "
                        "Use --incremental-column <coluna> para filtrar, ou --recreate para full reload.",
                    )
 
            # Coluna usada para ordenar a leitura e retomar após queda de
            # conexão. Em tabela recém-criada sem --incremental-column, usa a
            # PK (since_value continua None -> carga completa, só ordenada).
            resume_column = incremental_column or get_primary_key_column(sql_conn, target_table)
            if resume_column:
                src_ref = ReconnectingConnection("origem", src_factory, src_conn)
                dst_ref = ReconnectingConnection("destino", sql_factory, sql_conn)
                try:
                    load_with_resume(extract_fn, src_ref, dst_ref, source_table, target_table,
                                     columns, resume_column, since_value)
                finally:
                    src_ref.close_if_replaced()
                    dst_ref.close_if_replaced()
            else:
                log.warning("Sem coluna incremental nem PK de coluna única: se a conexão cair "
                            "no meio, a carga NÃO será retomada automaticamente.")
                extract_fn(src_conn, sql_conn, source_table, target_table, columns,
                           incremental_column=incremental_column, since_value=since_value)
        finally:
            _safe_close(src_conn)  # pode já ter sido fechada numa reconexão

    finally:
        _safe_close(sql_conn)
 
 

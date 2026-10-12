"""Modo `--reconcile`: comparação completa de chaves entre origem e
destino, sem depender de MAX()/watermark - existe porque o watermark
"normal" (MAX da coluna incremental) não cobre o caso de IDs não
sequenciais/com furos (ex: linhas que falharam silenciosamente numa carga
anterior e nunca chegaram a ser inseridas, mas cujo ID é menor que o maior
já carregado).

Otimizado para baixo consumo de memória em tabelas grandes:
- As chaves do DESTINO são carregadas de forma compacta: um
  `array.array('q')` ORDENADO quando todos os valores são inteiros
  (~8 bytes/valor, sem overhead de objeto Python), localizadas por busca
  binária (`bisect`); cai para um `set()` comum só se aparecer alguma
  chave não inteira (texto/GUID).
- As chaves da ORIGEM nunca são materializadas inteiras: são lidas e
  comparadas em streaming, lote a lote.

Resiliente a: limites de cláusula `IN (...)` do banco de origem/driver
(divide o lote pela metade recursivamente até um tamanho mínimo seguro) e a
quedas de conexão de rede (reconecta e repete só a etapa que falhou, via
`resilience._retry_on_disconnect`).

`--delete-extra` (opt-in, só com `--reconcile`): exclui do destino as
linhas cuja chave não existe mais na origem, pulando automaticamente linhas
ainda protegidas por FOREIGN KEY de outra tabela (com aviso), sem abortar o
restante do lote.
"""
import array
import bisect
from typing import List, Tuple

from .logging_config import log
from .resilience import ReconnectingConnection, _is_connection_error, _safe_close, _retry_on_disconnect
from .loader import BATCH_SIZE, _prepare_bulk_insert, _finalize_bulk_insert

# =============================================================================
# RECONCILIAÇÃO: compara TODAS as linhas entre origem e destino pela chave
# primária (ou coluna informada) e insere só o que estiver faltando no
# destino. Não depende de MAX()/watermark - por isso não perde linhas com
# ID mais baixo que nunca chegaram a ser carregadas.
# =============================================================================
 
def _fetch_target_keys_compact(conn, sql: str):
    """Carrega as chaves do destino da forma mais leve possível em memória:
    se TODOS os valores forem inteiros (caso comum: ID/IDENTITY), usa um
    array.array('q') ORDENADO - ~8 bytes por valor, sem o overhead de objeto
    Python de um set - e localiza cada chave por busca binária (bisect). Se
    algum valor não for inteiro (chave textual/GUID), cai para um set()
    comum. Retorna (kind, data) onde kind é 'int' ou 'set'."""
    cur = conn.cursor()
    cur.arraysize = BATCH_SIZE
    cur.execute(sql)
 
    int_values = array.array("q")
    generic_values = None  # só é criado se aparecer algum valor não-inteiro
    is_int = True
 
    while True:
        batch = cur.fetchmany(BATCH_SIZE)
        if not batch:
            break
        for row in batch:
            v = row[0]
            if is_int and isinstance(v, int):
                int_values.append(v)
            else:
                if is_int:
                    # primeiro valor não-inteiro encontrado: migra tudo que
                    # já foi lido pro modo genérico (set)
                    is_int = False
                    generic_values = set(int_values)
                    int_values = None
                generic_values.add(v)
    cur.close()
 
    if is_int:
        int_values = array.array("q", sorted(int_values))
        return "int", int_values
    return "set", generic_values
 
 
def _source_key_query(source: str, source_table: str, key_column: str) -> str:
    if source == "oracle":
        return f"SELECT {key_column} FROM {source_table}"
    if source == "hana":
        return f'SELECT "{key_column}" FROM {source_table}'
    return f"SELECT [{key_column}] FROM {source_table} WITH (NOLOCK)"  # sqlserver
 
 
def _source_rows_by_keys_query(source: str, source_table: str, key_column: str,
                                col_names: List[str], n_keys: int):
    """Monta (sql, placeholder_style) para buscar linhas específicas por
    chave. Cada driver usa uma sintaxe de bind diferente."""
    if source == "oracle":
        cols = ", ".join(col_names)
        placeholders = ", ".join(f":k{i}" for i in range(n_keys))
        return f"SELECT {cols} FROM {source_table} WHERE {key_column} IN ({placeholders})"
    if source == "hana":
        cols = ", ".join(f'"{c}"' for c in col_names)
        placeholders = ", ".join("?" for _ in range(n_keys))
        return f'SELECT {cols} FROM {source_table} WHERE "{key_column}" IN ({placeholders})'
    cols = ", ".join(f"[{c}]" for c in col_names)
    placeholders = ", ".join("?" for _ in range(n_keys))
    return f"SELECT {cols} FROM {source_table} WITH (NOLOCK) WHERE [{key_column}] IN ({placeholders})"
 
 
MIN_RECONCILE_CHUNK = 25
 
 
def _fetch_rows_by_keys(src_conn, col_names: List[str], source: str, source_table: str,
                         key_column: str, chunk: list) -> list:
    """Busca na origem as linhas de um lote de chaves faltantes.
    Se a consulta falhar (limite de itens em cláusula IN do banco de origem,
    limite de parâmetros do driver ODBC, etc. - cada banco tem um limite
    diferente e nem sempre documentado com clareza), tenta de novo dividindo
    o lote pela metade, recursivamente, até um tamanho mínimo seguro - assim
    o script se adapta sozinho em vez de quebrar a reconciliação inteira por
    causa de um único lote grande demais.

    Erros de QUEDA DE CONEXÃO não disparam a divisão (dividir não resolve
    rede caída): sobem direto pra quem chamou reconectar e repetir o lote."""
    sql = _source_rows_by_keys_query(source, source_table, key_column, col_names, len(chunk))
    cur = src_conn.cursor()
    try:
        if source == "oracle":
            bind = {f"k{j}": v for j, v in enumerate(chunk)}
            cur.execute(sql, bind)
        else:
            cur.execute(sql, tuple(chunk))
        return cur.fetchall()
    except Exception as exc:
        if _is_connection_error(exc) or len(chunk) <= MIN_RECONCILE_CHUNK:
            raise
        log.warning(
            "Falha ao buscar lote de %d chave(s) (%s: %s) - dividindo o lote e "
            "tentando de novo com lotes menores...",
            len(chunk), type(exc).__name__, exc,
        )
    finally:
        _safe_close(cur)

    mid = len(chunk) // 2
    return (
        _fetch_rows_by_keys(src_conn, col_names, source, source_table, key_column, chunk[:mid])
        + _fetch_rows_by_keys(src_conn, col_names, source, source_table, key_column, chunk[mid:])
    )


def _existing_target_keys(dst_conn, target_table: str, key_column: str, keys: list) -> set:
    """Quais das chaves informadas já existem no destino."""
    placeholders = ", ".join("?" for _ in keys)
    cur = dst_conn.cursor()
    try:
        cur.execute(f"SELECT [{key_column}] FROM dbo.{target_table} WHERE [{key_column}] IN ({placeholders})",
                    tuple(keys))
        return {r[0] for r in cur.fetchall()}
    finally:
        _safe_close(cur)
 
 
MIN_DELETE_CHUNK = 25
 
 
def _delete_chunk(dst_cursor, dst_conn, target_table: str, key_column: str, chunk: list) -> int:
    """Exclui um lote de linhas do destino pela chave. Se der erro de FK
    (a linha ainda é referenciada por outra tabela), isola e pula só a(s)
    linha(s) bloqueada(s) em vez de abortar tudo - dividindo o lote até
    achar exatamente quais chaves não podem ser excluídas. Erros de
    tamanho de lote também disparam a mesma divisão; queda de conexão
    sobe direto (quem chamou reconecta e repete o lote)."""
    placeholders = ", ".join("?" for _ in chunk)
    sql = f"DELETE FROM dbo.{target_table} WHERE [{key_column}] IN ({placeholders})"
    try:
        dst_cursor.execute(sql, tuple(chunk))
        deleted = dst_cursor.rowcount
        dst_conn.commit()
        return deleted
    except Exception as exc:
        if _is_connection_error(exc):
            raise
        dst_conn.rollback()
        if len(chunk) == 1:
            log.warning(
                "Não foi possível excluir a linha com %s=%s do destino (%s) - "
                "provavelmente ainda referenciada por FK de outra tabela. Pulando.",
                key_column, chunk[0], exc,
            )
            return 0
        if len(chunk) <= MIN_DELETE_CHUNK:
            # lote pequeno: tenta uma a uma pra isolar exatamente quais
            # chaves estão bloqueadas, sem perder as demais do lote
            total = 0
            for k in chunk:
                total += _delete_chunk(dst_cursor, dst_conn, target_table, key_column, [k])
            return total
        log.warning(
            "Falha ao excluir lote de %d chave(s) (%s: %s) - dividindo o lote e "
            "tentando de novo...", len(chunk), type(exc).__name__, exc,
        )
        mid = len(chunk) // 2
        return (
            _delete_chunk(dst_cursor, dst_conn, target_table, key_column, chunk[:mid])
            + _delete_chunk(dst_cursor, dst_conn, target_table, key_column, chunk[mid:])
        )
 
 
def delete_extra_rows(dst_ref: ReconnectingConnection, target_table: str, key_column: str,
                       extra_keys: list, chunk_size: int = 500) -> int:
    """Exclui do destino todas as linhas cuja chave está em extra_keys, em
    lotes. Retorna quantas linhas foram efetivamente excluídas (pode ser
    menos que len(extra_keys) se alguma estiver protegida por FK). Se a
    conexão cair, reconecta e repete o lote (DELETE é idempotente)."""
    state = {"cursor": dst_ref.conn.cursor()}

    def reopen_cursor():
        state["cursor"] = dst_ref.conn.cursor()

    total = 0
    try:
        for i in range(0, len(extra_keys), chunk_size):
            chunk = extra_keys[i:i + chunk_size]
            total += _retry_on_disconnect(
                lambda attempt: _delete_chunk(state["cursor"], dst_ref.conn, target_table, key_column, chunk),
                [dst_ref], "excluir lote no destino", on_reconnect=reopen_cursor,
            )
            log.info("  -> %d/%d linha(s) excluídas", total, len(extra_keys))
    finally:
        _safe_close(state["cursor"])
    return total


def _compare_keys(src_conn, dst_conn, source: str, source_table: str,
                  target_table: str, key_column: str):
    """Lê as chaves do destino (compacto) e compara com as da origem em
    streaming. Retorna (source_count, target_count, missing_list, extra_keys)."""
    log.info("Lendo chaves ('%s') do destino de forma compacta...", key_column)
    kind, target_data = _fetch_target_keys_compact(dst_conn, f"SELECT [{key_column}] FROM dbo.{target_table}")
    target_count = len(target_data)
    seen = bytearray(target_count) if kind == "int" else None  # marca as chaves batidas

    log.info("Comparando com a origem em streaming (sem carregar a origem inteira em memória)...")
    missing_list = []
    source_count = 0
    src_cursor = src_conn.cursor()
    try:
        src_cursor.arraysize = BATCH_SIZE
        src_cursor.execute(_source_key_query(source, source_table, key_column))
        while True:
            batch = src_cursor.fetchmany(BATCH_SIZE)
            if not batch:
                break
            for row in batch:
                v = row[0]
                source_count += 1
                if kind == "int" and isinstance(v, int):
                    idx = bisect.bisect_left(target_data, v)
                    if idx < target_count and target_data[idx] == v:
                        seen[idx] = 1
                    else:
                        missing_list.append(v)
                elif kind == "set":
                    if v in target_data:
                        target_data.discard(v)
                    else:
                        missing_list.append(v)
                else:
                    # kind == "int" mas apareceu um valor não-inteiro na origem
                    # (bem raro/inconsistente) - trata como faltante pra não
                    # arriscar perder a linha
                    missing_list.append(v)
    finally:
        _safe_close(src_cursor)

    # Chaves que sobraram no destino sem bater com nada na origem = "extra".
    # No modo "int", são as posições do array que nunca foram marcadas; no
    # modo "set", é o que sobrou depois de descartar as que bateram.
    if kind == "int":
        extra_keys = [target_data[i] for i in range(target_count) if not seen[i]]
    else:
        extra_keys = list(target_data)
    return source_count, target_count, missing_list, extra_keys


def reconcile_missing_rows(src_ref: ReconnectingConnection, dst_ref: ReconnectingConnection,
                            source_table: str, target_table: str,
                            columns: List[Tuple[str, str]], key_column: str, source: str,
                            chunk_size: int = 500, delete_extra: bool = False):
    """Compara todas as chaves da origem e do destino, insere no destino as
    linhas que estão faltando e - se delete_extra=True - EXCLUI do destino
    as linhas cuja chave não existe mais na origem.

    Otimizado pra consumir bem menos memória em tabelas grandes:
    - As chaves do DESTINO ficam guardadas de forma compacta (array de
      inteiros ordenado quando possível, ver _fetch_target_keys_compact).
    - As chaves da ORIGEM NUNCA são carregadas todas de uma vez: são lidas
      em lotes (streaming) e comparadas contra o destino na hora, mantendo
      só a lista de chaves que estão faltando (essa lista tende a ser bem
      menor que a tabela inteira, ao contrário de guardar as duas tabelas
      completas em memória ao mesmo tempo como antes).

    Resiliente a quedas de conexão em todas as fases: reconecta (origem e/ou
    destino) com backoff e repete só a etapa que falhou - ver
    _retry_on_disconnect."""
    col_names = [c[0] for c in columns]

    # Se a conexão cair no meio da comparação, ela recomeça do zero: o
    # estado parcial (marcações/chaves já descartadas) não é confiável.
    source_count, target_count, missing_list, extra_keys = _retry_on_disconnect(
        lambda attempt: _compare_keys(src_ref.conn, dst_ref.conn, source, source_table,
                                      target_table, key_column),
        [src_ref, dst_ref], "comparar as chaves entre origem e destino (a comparação recomeça do zero)",
    )
    extra_count = len(extra_keys)
 
    log.info(
        "Origem: %d linha(s) | Destino: %d linha(s) | Faltando no destino: %d | "
        "Só no destino (não existem mais na origem): %d",
        source_count, target_count, len(missing_list), extra_count,
    )
 
    if extra_count and not delete_extra:
        log.warning(
            "%d linha(s) existem no destino mas não foram encontradas na origem "
            "(podem ter sido excluídas na origem). O script NÃO remove nada "
            "automaticamente - rode de novo com --delete-extra se quiser excluir "
            "essas linhas do destino.",
            extra_count,
        )
 
    if missing_list:
        log.info("Extraindo e inserindo %d linha(s) que faltam no destino...", len(missing_list))
 
        # Estado da sessão de INSERT no destino (cursor, IDENTITY_INSERT...) -
        # vale só pra conexão atual, por isso é recriado a cada reconexão.
        ins = {}

        def prepare_insert():
            ins["cursor"], ins["sql"], ins["keep_idx"], ins["identity"] = _prepare_bulk_insert(
                dst_ref.conn, target_table, col_names,
            )

        key_lower = key_column.lower()
        key_idx = next((i for i, c in enumerate(col_names) if c.lower() == key_lower), None)

        def insert_rows(rows, attempt):
            if attempt and key_idx is not None:
                # Repetição após reconexão: não dá pra saber se o COMMIT do
                # lote chegou a ser aplicado antes da queda - descarta as
                # linhas que já estão no destino pra não violar a PK.
                existing = _existing_target_keys(dst_ref.conn, target_table, key_column,
                                                 [r[key_idx] for r in rows])
                rows = [r for r in rows if r[key_idx] not in existing]
            if not rows:
                return 0
            keep_idx = ins["keep_idx"]
            if len(keep_idx) == len(col_names):
                data = [tuple(r) for r in rows]
            else:
                data = [tuple(r[k] for k in keep_idx) for r in rows]
            ins["cursor"].executemany(ins["sql"], data)
            dst_ref.conn.commit()
            return len(data)

        def finalize_insert(attempt):
            _finalize_bulk_insert(ins["cursor"], dst_ref.conn, target_table, ins["identity"])

        def reopen_for_finalize():
            # IDENTITY_INSERT vale só pra sessão (já morreu com a conexão
            # antiga), mas o NOCHECK fica gravado na tabela - precisa ser
            # reabilitado na conexão nova.
            ins["cursor"] = dst_ref.conn.cursor()
            ins["identity"] = False

        _retry_on_disconnect(lambda attempt: prepare_insert(), [dst_ref],
                             "preparar a inserção no destino")

        total = 0
        try:
            for i in range(0, len(missing_list), chunk_size):
                chunk = missing_list[i:i + chunk_size]
                rows = _retry_on_disconnect(
                    lambda attempt: _fetch_rows_by_keys(src_ref.conn, col_names, source,
                                                        source_table, key_column, chunk),
                    [src_ref], "buscar lote de linhas na origem",
                )
                if not rows:
                    continue
                inserted = _retry_on_disconnect(
                    lambda attempt: insert_rows(rows, attempt),
                    [dst_ref], "inserir lote no destino", on_reconnect=prepare_insert,
                )
                if inserted:
                    total += inserted
                    log.info("  -> %d/%d linha(s) faltantes inseridas", total, len(missing_list))
        finally:
            _retry_on_disconnect(finalize_insert, [dst_ref],
                                 "reabilitar as constraints da tabela de destino",
                                 on_reconnect=reopen_for_finalize)

        log.info("Inserção concluída: %d linha(s) inserida(s) que estavam faltando.", total)
    else:
        log.info("Nenhuma linha faltando para inserir.")

    if delete_extra and extra_count:
        log.info("Excluindo %d linha(s) do destino que não existem mais na origem...", extra_count)
        deleted = delete_extra_rows(dst_ref, target_table, key_column, extra_keys)
        log.info("Exclusão concluída: %d/%d linha(s) removida(s) do destino.", deleted, extra_count)
        if deleted < extra_count:
            log.warning(
                "%d linha(s) não puderam ser excluídas (provavelmente ainda referenciadas "
                "por FK de outra tabela que aponta pra esta) - foram puladas, veja os avisos "
                "acima com o detalhe de cada uma.",
                extra_count - deleted,
            )
 

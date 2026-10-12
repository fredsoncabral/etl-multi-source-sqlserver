"""Gestão da tabela de destino no SQL Server: criação (com fidelidade
estrutural), e, quando a tabela já existe, cópia estrutural incremental
("self-healing") - adiciona colunas e constraints da origem que ainda
faltam no destino, sem nunca recriar ou perder dados por padrão.

Recriação total só acontece explicitamente via `ensure_target_table(...,
recreate=True)` (flag `--recreate` da CLI).
"""
from typing import List, Optional, Tuple

from .introspection import SCHEMA_EXTRACTORS
from .ddl import create_table_full_fidelity, add_table_constraints
from .logging_config import log

# =============================================================================
# CRIAÇÃO DA TABELA NO SQL SERVER
# =============================================================================
 
def sanitize_table_name(table: str) -> str:
    """Normaliza SCHEMA.TABELA (Oracle/HANA) para um nome de tabela plano no
    SQL Server, ex: SCHEMA.TABELA -> SCHEMA_TABELA. Só é usado se o usuário
    não informar --target-table explicitamente."""
    return table.replace('"', "").replace(".", "_").upper()
 
 
def table_exists_sqlserver(conn, target_table: str) -> bool:
    """Verifica no catálogo do SQL Server se dbo.<target_table> já existe."""
    cur = conn.cursor()
    cur.execute("SELECT OBJECT_ID(?, 'U')", (f"dbo.{target_table}",))
    exists = cur.fetchone()[0] is not None
    cur.close()
    return exists
 
 
def get_target_column_names(conn, target_table: str) -> set:
    """Nomes de todas as colunas que já existem hoje na tabela de destino."""
    cur = conn.cursor()
    cur.execute("SELECT c.name FROM sys.columns c WHERE c.object_id = OBJECT_ID(?)",
                (f"dbo.{target_table}",))
    rows = cur.fetchall()
    cur.close()
    return {r[0] for r in rows}
 
 
def sync_missing_columns(conn, target_table: str, columns: List[Tuple[str, str]]) -> None:
    """Garante cópia INTEGRAL da estrutura da origem: se a tabela de destino
    já existia (por isso não foi recriada do zero) e está faltando alguma
    coluna que existe na origem - seja ela comum, PK, FK ou SK - essa coluna
    é adicionada agora via ALTER TABLE ADD, sem mexer nas colunas já
    existentes nem nos dados já carregados."""
    existing = get_target_column_names(conn, target_table)
    missing = [(name, sql_type) for name, sql_type in columns if name not in existing]
    if not missing:
        return
 
    cur = conn.cursor()
    for name, sql_type in missing:
        log.info(
            "Coluna '%s' existe na origem mas não no destino - adicionando "
            "via ALTER TABLE (tipo %s).", name, sql_type,
        )
        cur.execute(f"ALTER TABLE dbo.{target_table} ADD [{name}] {sql_type} NULL;")
    conn.commit()
    cur.close()
    log.info(
        "%d coluna(s) adicionada(s) em dbo.%s para garantir que TODAS as "
        "colunas da origem sejam copiadas.", len(missing), target_table,
    )
 
 
def get_target_constraint_names(conn, target_table: str) -> set:
    """Nomes de todas as constraints (PK, UNIQUE, FK) que já existem hoje na
    tabela de destino."""
    cur = conn.cursor()
    cur.execute(
        "SELECT name FROM sys.objects WHERE parent_object_id = OBJECT_ID(?) "
        "AND type IN ('PK','UQ','F')",
        (f"dbo.{target_table}",),
    )
    rows = cur.fetchall()
    cur.close()
    return {r[0] for r in rows}
 
 
def sync_missing_constraints(dst_conn, target_table: str, src_conn, source_table: str, source: str) -> None:
    """Complemento de sync_missing_columns: quando a tabela de destino já
    existia e por isso não passou pela criação com fidelidade total, esta
    função verifica quais PK/UNIQUE/FK da origem ainda NÃO existem no
    destino e tenta criá-las agora.
 
    Isso resolve o caso comum de cópia tabela-a-tabela: na primeira vez que
    uma tabela é copiada, as FKs para tabelas ainda não copiadas são puladas
    (o SQL Server não permite referenciar uma tabela inexistente). Depois
    que essas tabelas referenciadas forem copiadas para o destino, rodar o
    script de novo para a tabela original (sem --recreate) agora detecta e
    cria as FKs que antes tinham ficado pendentes - sem precisar recriar
    nada nem perder dados já carregados. Funciona igual para Oracle, HANA e
    SQL Server, já que usa o mesmo SCHEMA_EXTRACTORS da criação inicial."""
    if source not in SCHEMA_EXTRACTORS:
        return
 
    extractors = SCHEMA_EXTRACTORS[source]
    primary_key = extractors["pk"](src_conn, source_table)
    unique_constraints = extractors["unique"](src_conn, source_table)
    foreign_keys = extractors["fks"](src_conn, source_table)
 
    existing_names = get_target_constraint_names(dst_conn, target_table)
 
    pending_pk = primary_key if primary_key and primary_key[0] not in existing_names else None
    pending_uniques = [u for u in unique_constraints if u[0] not in existing_names]
    pending_fks = [f for f in foreign_keys if f["name"] not in existing_names]
 
    if not (pending_pk or pending_uniques or pending_fks):
        return  # já está tudo criado, nada a fazer
 
    log.info(
        "Tabela dbo.%s já existia - verificando constraints pendentes da origem "
        "(PK%s, %d UNIQUE, %d FK a tentar criar agora)...",
        target_table,
        " " + pending_pk[0] if pending_pk else ": nenhuma pendente",
        len(pending_uniques), len(pending_fks),
    )
    add_table_constraints(dst_conn, target_table, pending_pk, pending_uniques, pending_fks)
 
 
def ensure_target_table(conn, target_table: str, columns: List[Tuple[str, str]],
                         recreate: bool = False,
                         source: Optional[str] = None, src_conn=None,
                         source_table: Optional[str] = None) -> bool:
    """Garante que a tabela de destino exista, decidindo entre criar do zero
    ou aproveitar a tabela já existente (carga incremental).
 
    A criação replica fielmente a estrutura da origem - Oracle, HANA ou SQL
    Server - via create_table_full_fidelity: nullability, IDENTITY (quando
    aplicável), PRIMARY KEY, UNIQUE e FOREIGN KEY (quando a tabela
    referenciada já existir no destino). Colunas computadas só são
    traduzidas automaticamente quando a origem também é SQL Server (mesmo
    dialeto T-SQL); para Oracle/HANA elas viram coluna comum, com aviso.
 
    Retorna True se a tabela foi criada agora (do zero); False se já existia
    e foi mantida como está (a carga será feita por cima, incrementalmente).
    """
    exists = table_exists_sqlserver(conn, target_table)
 
    if exists and not recreate:
        log.info(
            "Tabela dbo.%s já existe - mantendo estrutura atual e fazendo "
            "carga incremental (sem DROP/CREATE).", target_table,
        )
        # Mesmo reaproveitando a tabela, garante que nenhuma coluna da
        # origem tenha ficado de fora (cópia estrutural integral). Atenção:
        # isso só ADICIONA colunas faltantes com tipo+NULL; não corrige
        # tipos de colunas que já existiam com estrutura diferente da
        # origem - para isso, rode uma vez com --recreate.
        sync_missing_columns(conn, target_table, columns)
 
        # Além das colunas, tenta criar agora qualquer PK/UNIQUE/FK da
        # origem que ainda não exista no destino - cobre o caso de FKs que
        # ficaram pendentes numa execução anterior porque a tabela
        # referenciada ainda não tinha sido copiada.
        if source in SCHEMA_EXTRACTORS and src_conn is not None and source_table is not None:
            sync_missing_constraints(conn, target_table, src_conn, source_table, source)
 
        return False
 
    cur = conn.cursor()
    if exists and recreate:
        log.info("--recreate informado: derrubando tabela existente dbo.%s...", target_table)
        cur.execute(f"DROP TABLE dbo.{target_table};")
        conn.commit()
    cur.close()
 
    if source in SCHEMA_EXTRACTORS and src_conn is not None and source_table is not None:
        create_table_full_fidelity(conn, target_table, src_conn, source_table, columns, source)
    else:
        cols_ddl = ",\n    ".join(f"[{name}] {sql_type} NULL" for name, sql_type in columns)
        ddl = f"CREATE TABLE dbo.{target_table} (\n    {cols_ddl}\n);"
        log.info("Criando tabela dbo.%s no SQL Server (todas as %d colunas da origem)...",
                  target_table, len(columns))
        log.debug("DDL:\n%s", ddl)
        cur = conn.cursor()
        cur.execute(ddl)
        conn.commit()
        cur.close()
 
    return True
 
 
def get_target_max_value(conn, target_table: str, column: str):
    """Usado na carga incremental: retorna o maior valor já carregado de uma
    coluna de controle (ex: ID, DATA_ATUALIZACAO) na tabela de destino, para
    filtrar na origem apenas o que ainda não foi carregado."""
    cur = conn.cursor()
    cur.execute(f"SELECT MAX([{column}]) FROM dbo.{target_table}")
    row = cur.fetchone()
    cur.close()
    return row[0] if row else None
 
 
def get_primary_key_column(conn, target_table: str) -> Optional[str]:
    """Retorna o nome da coluna de chave primária da tabela de destino,
    SOMENTE se a PK for composta por uma única coluna (o caso mais comum,
    ex: um ID identity). Usado como fallback automático de coluna
    incremental quando a tabela já existe e o usuário não informou
    --incremental-column explicitamente, para evitar erros de chave
    duplicada em cargas repetidas."""
    cur = conn.cursor()
    cur.execute(
        """
        SELECT c.name
        FROM sys.indexes i
        JOIN sys.index_columns ic ON ic.object_id = i.object_id AND ic.index_id = i.index_id
        JOIN sys.columns c ON c.object_id = ic.object_id AND c.column_id = ic.column_id
        WHERE i.object_id = OBJECT_ID(?) AND i.is_primary_key = 1
        ORDER BY ic.key_ordinal
        """,
        (f"dbo.{target_table}",),
    )
    rows = cur.fetchall()
    cur.close()
    if len(rows) == 1:
        return rows[0][0]
    return None  # sem PK, ou PK composta -> não arriscar um fallback errado
 

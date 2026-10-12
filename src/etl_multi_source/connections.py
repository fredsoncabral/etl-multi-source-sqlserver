"""Abertura de conexão com cada banco de origem e com o SQL Server de
destino.

Oracle usa python-oracledb (modo "thin" por padrão; "thick" via Instant
Client quando ORACLE_CLIENT_LIB_DIR está definido, necessário para versões
de Oracle Database anteriores a 12.1 - erro DPY-3010 no modo thin). SAP HANA
usa o driver oficial hdbcli. SQL Server (origem e destino) usa pyodbc.
"""
import pyodbc
import oracledb
from hdbcli import dbapi as hana_dbapi

from .config import OracleConfig, HanaConfig, SqlServerConfig, SqlServerSourceConfig
from .logging_config import log

def connect_oracle(cfg: OracleConfig):
    """Conecta via python-oracledb. Por padrão em modo "thin" (não requer
    Oracle Client instalado), que funciona apenas com Oracle Database 12.1+.
 
    Para Oracle Database mais antigo (erro DPY-3010 no modo thin), defina a
    variável de ambiente ORACLE_CLIENT_LIB_DIR apontando para a pasta do
    Oracle Instant Client (ex: /opt/oracle/instantclient_21_13) - o script
    então inicializa o modo "thick" automaticamente, que é compatível com
    versões antigas de Oracle."""
    global _oracle_thick_mode_initialized
 
    if cfg.client_lib_dir and not _oracle_thick_mode_initialized:
        log.info("Inicializando python-oracledb em modo thick (Instant Client: %s)...", cfg.client_lib_dir)
        oracledb.init_oracle_client(lib_dir=cfg.client_lib_dir)
        _oracle_thick_mode_initialized = True
 
    log.info("Conectando ao Oracle (%s)...", cfg.dsn)
    try:
        conn = oracledb.connect(user=cfg.user, password=cfg.password, dsn=cfg.dsn)
    except oracledb.NotSupportedError as exc:
        if "DPY-3010" in str(exc):
            log.error(
                "Esse Oracle Database é uma versão anterior a 12.1, que o modo 'thin' do "
                "python-oracledb não suporta. Instale o Oracle Instant Client no host e "
                "defina a variável de ambiente ORACLE_CLIENT_LIB_DIR com o caminho da pasta "
                "dele (ex: export ORACLE_CLIENT_LIB_DIR=/opt/oracle/instantclient_21_13) para "
                "habilitar o modo 'thick' automaticamente e tentar de novo. Detalhes: "
                "https://python-oracledb.readthedocs.io/en/latest/user_guide/troubleshooting.html#dpy-3010"
            )
        raise
    return conn
 
 
def connect_hana(cfg: HanaConfig):
    """Conecta via hdbcli (driver oficial SAP HANA)."""
    log.info("Conectando ao SAP HANA (%s:%s)...", cfg.address, cfg.port)
    conn = hana_dbapi.connect(
        address=cfg.address,
        port=cfg.port,
        user=cfg.user,
        password=cfg.password,
    )
    return conn
 
 
def connect_sqlserver(cfg: SqlServerConfig):
    conn_str = (
        f"DRIVER={{{cfg.driver}}};"
        f"SERVER={cfg.server};"
        f"DATABASE={cfg.database};"
        f"UID={cfg.user};"
        f"PWD={cfg.password};"
        f"Encrypt={cfg.encrypt};"
        f"TrustServerCertificate={cfg.trust_cert};"
    )
    log.info("Conectando ao SQL Server (%s/%s)...", cfg.server, cfg.database)
    conn = pyodbc.connect(conn_str, autocommit=False)
    return conn


def connect_sqlserver_source(cfg: SqlServerSourceConfig):
    conn = connect_sqlserver(cfg)
    # Evita que o script fique preso indefinidamente esperando um
    # lock na tabela de origem (ex: outra transação segurando a
    # tabela) - com NOLOCK no SELECT isso já deve ser raro, mas o
    # timeout garante uma falha clara em vez de travar para sempre.
    conn.cursor().execute("SET LOCK_TIMEOUT 30000;")  # 30s
    return conn

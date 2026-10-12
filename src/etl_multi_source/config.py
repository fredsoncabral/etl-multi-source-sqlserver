"""Configuração de conexão (Oracle, SAP HANA, SQL Server) via variáveis de
ambiente, usando dataclasses.

Nota de portfólio: no projeto original, essas classes vinham de um
`config.py` interno (com nomes de host/credenciais de produção), que não
faz parte deste repositório por conter dados de um ambiente real. Este
arquivo reconstrói a MESMA interface (os MESMOS atributos) consumida pelo
restante do código em `connections.py`, mas lendo tudo de variáveis de
ambiente/arquivo `.env` - veja `.env.example` na raiz do repositório.
"""
import os
from dataclasses import dataclass
from typing import Optional


def _env(name: str, default: Optional[str] = None) -> Optional[str]:
    return os.getenv(name, default)


def _env_bool(name: str, default: bool) -> bool:
    val = os.getenv(name)
    if val is None:
        return default
    return val.strip().lower() in ("1", "true", "yes", "y", "on")


@dataclass
class OracleConfig:
    """Credenciais e DSN de conexão com o Oracle de origem.

    client_lib_dir: caminho do Oracle Instant Client (ORACLE_CLIENT_LIB_DIR).
    Só é necessário para Oracle Database anterior a 12.1 (ver connect_oracle
    em connections.py, que cai para modo "thick" quando esse valor é
    informado).
    """
    user: str = _env("ORACLE_USER", "")
    password: str = _env("ORACLE_PASSWORD", "")
    dsn: str = _env("ORACLE_DSN", "")  # ex: host:1521/ORCLPDB1
    client_lib_dir: Optional[str] = _env("ORACLE_CLIENT_LIB_DIR")


@dataclass
class HanaConfig:
    user: str = _env("HANA_USER", "")
    password: str = _env("HANA_PASSWORD", "")
    address: str = _env("HANA_ADDRESS", "")
    port: int = int(_env("HANA_PORT", "30015"))


@dataclass
class SqlServerConfig:
    """Conexão com o SQL Server de DESTINO (onde as tabelas são criadas e
    carregadas)."""
    driver: str = _env("SQLSERVER_DRIVER", "ODBC Driver 18 for SQL Server")
    server: str = _env("SQLSERVER_SERVER", "")
    database: str = _env("SQLSERVER_DATABASE", "")
    user: str = _env("SQLSERVER_USER", "")
    password: str = _env("SQLSERVER_PASSWORD", "")
    encrypt: str = _env("SQLSERVER_ENCRYPT", "yes")
    trust_cert: str = _env("SQLSERVER_TRUST_SERVER_CERTIFICATE", "yes")


@dataclass
class SqlServerSourceConfig(SqlServerConfig):
    """Conexão com um SQL Server de ORIGEM (--source sqlserver). Mesma
    forma de SqlServerConfig; por padrão lê variáveis com prefixo
    SQLSERVER_SOURCE_*, caindo para as variáveis SQLSERVER_* de destino se
    as específicas de origem não estiverem definidas (útil em testes locais
    com um único servidor)."""
    driver: str = _env("SQLSERVER_SOURCE_DRIVER", _env("SQLSERVER_DRIVER", "ODBC Driver 18 for SQL Server"))
    server: str = _env("SQLSERVER_SOURCE_SERVER", _env("SQLSERVER_SERVER", ""))
    database: str = _env("SQLSERVER_SOURCE_DATABASE", "")
    user: str = _env("SQLSERVER_SOURCE_USER", _env("SQLSERVER_USER", ""))
    password: str = _env("SQLSERVER_SOURCE_PASSWORD", _env("SQLSERVER_PASSWORD", ""))
    encrypt: str = _env("SQLSERVER_SOURCE_ENCRYPT", "yes")
    trust_cert: str = _env("SQLSERVER_SOURCE_TRUST_SERVER_CERTIFICATE", "yes")

"""Resiliência a quedas de conexão (Communication link failure, erro 10060,
ORA-03113, sessão HANA derrubada, etc.) durante operações longas.

Estratégia: detecta se uma exceção representa perda de conexão (e não um
erro de SQL/dados, que não deve disparar retry), reconecta com backoff
exponencial e repete a operação. `ReconnectingConnection` guarda a conexão
atual e sabe recriá-la via factory - quem consome deve sempre acessar
`.conn` na hora do uso (nunca guardar a referência antiga), pois ela é
substituída a cada reconexão.
"""
import os
import time
from typing import List

import pyodbc

from .logging_config import log

# =============================================================================
# RESILIÊNCIA A QUEDAS DE CONEXÃO (Communication link failure, erro 10060,
# ORA-03113, etc.): detecta o erro, reconecta com backoff e repete a operação
# =============================================================================

MAX_RETRIES = int(os.getenv("ETL_MAX_RETRIES", "5"))
RETRY_BASE_DELAY = float(os.getenv("ETL_RETRY_BASE_DELAY", "5"))  # segundos
RETRY_MAX_DELAY = 120  # segundos

_CONNECTION_ERROR_SQLSTATES = {"08S01", "08001", "08003", "08004", "08007"}
_HANA_CONNECTION_ERROR_CODES = {-10709, -10807, -10108}
_CONNECTION_ERROR_MARKERS = (
    # SQL Server / ODBC (0x274C = 10060, 0x2746 = 10054, 0x2745 = 10053)
    "communication link failure", "tcp provider", "0x274c", "0x2746", "0x2745",
    # Oracle
    "dpy-1001", "dpy-4011", "dpy-6005", "dpi-1080", "ora-03113", "ora-03114",
    "ora-03135", "ora-12170", "ora-12537", "ora-12541", "ora-12547",
    # SAP HANA
    "connection failed", "connection down", "socket closed", "session reconnected",
)


def _is_connection_error(exc: Exception) -> bool:
    """True se o erro indica queda/perda da conexão (e não erro de SQL/dados)."""
    if isinstance(exc, ConnectionError):
        return True
    if isinstance(exc, pyodbc.Error) and exc.args and exc.args[0] in _CONNECTION_ERROR_SQLSTATES:
        return True
    if getattr(exc, "errorcode", None) in _HANA_CONNECTION_ERROR_CODES:
        return True
    msg = str(exc).lower()
    return any(m in msg for m in _CONNECTION_ERROR_MARKERS)


def _safe_close(obj) -> None:
    """Fecha conexão/cursor ignorando erro (ex: já fechado ou conexão morta)."""
    if obj is None:
        return
    try:
        obj.close()
    except Exception:
        pass


class ReconnectingConnection:
    """Guarda a conexão atual e sabe recriá-la (via factory) após uma queda.
    Quem usa deve sempre acessar .conn na hora (nunca guardar a referência
    antiga), pois ela é trocada a cada reconexão."""

    def __init__(self, label: str, factory, conn):
        self.label = label
        self.factory = factory
        self.conn = conn
        self._original = conn

    def reconnect(self) -> None:
        _safe_close(self.conn)
        log.info("Reconectando ao banco de %s...", self.label)
        self.conn = self.factory()

    def close_if_replaced(self) -> None:
        """Fecha a conexão recriada (a original é fechada por quem a abriu)."""
        if self.conn is not self._original:
            _safe_close(self.conn)


def _retry_on_disconnect(action, refs: List[ReconnectingConnection], what: str, on_reconnect=None):
    """Executa action(attempt). Se falhar por queda de conexão, espera
    (backoff exponencial), reconecta todas as conexões em refs, chama
    on_reconnect() (pra recriar estado de sessão: cursor, IDENTITY_INSERT...)
    e executa action de novo - até MAX_RETRIES tentativas. attempt > 0 indica
    que é uma repetição (a operação anterior pode ter sido aplicada ou não).
    Erros que não são de conexão sobem imediatamente, sem retry."""
    attempt = 0
    while True:
        try:
            return action(attempt)
        except Exception as exc:
            if not _is_connection_error(exc):
                raise
            last_exc = exc

        while True:
            attempt += 1
            if attempt > MAX_RETRIES:
                log.error("Conexão perdida ao %s e não foi possível recuperar após %d tentativa(s).",
                          what, MAX_RETRIES)
                raise last_exc
            delay = min(RETRY_BASE_DELAY * 2 ** (attempt - 1), RETRY_MAX_DELAY)
            log.warning(
                "Conexão perdida ao %s (%s: %s) - tentativa %d/%d de reconexão em %.0fs...",
                what, type(last_exc).__name__, last_exc, attempt, MAX_RETRIES, delay,
            )
            time.sleep(delay)
            try:
                for ref in refs:
                    ref.reconnect()
                if on_reconnect:
                    on_reconnect()
                break
            except Exception as exc:
                if not _is_connection_error(exc):
                    raise
                last_exc = exc

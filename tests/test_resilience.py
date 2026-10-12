"""Testes da detecção de erro de conexão (vs. erro de SQL/dados) e do
backoff de reconexão - o núcleo da resiliência do ETL.

A distinção importa: um erro de SQL (ex: coluna inexistente, violação de
constraint) deve subir imediatamente; só uma queda de conexão real deve
disparar reconexão + retry.
"""
import pytest

from etl_multi_source.resilience import _is_connection_error, _retry_on_disconnect, ReconnectingConnection


class FakePyodbcError(Exception):
    """Simula pyodbc.Error sem depender do driver real estar instalado."""
    def __init__(self, sqlstate, msg=""):
        super().__init__(sqlstate, msg)
        self.args = (sqlstate, msg)


def test_erro_generico_nao_e_erro_de_conexao():
    assert _is_connection_error(ValueError("coluna inexistente")) is False


def test_connection_error_builtin_e_erro_de_conexao():
    assert _is_connection_error(ConnectionError("conexão perdida")) is True


def test_mensagem_communication_link_failure_e_erro_de_conexao():
    assert _is_connection_error(Exception("Communication link failure")) is True


def test_mensagem_ora_03113_e_erro_de_conexao():
    assert _is_connection_error(Exception("ORA-03113: end-of-file on communication channel")) is True


def test_mensagem_hana_connection_down_e_erro_de_conexao():
    assert _is_connection_error(Exception("connection down, reconnect required")) is True


def test_violacao_de_constraint_nao_e_erro_de_conexao():
    assert _is_connection_error(Exception("Violation of PRIMARY KEY constraint")) is False


class TestRetryOnDisconnect:
    def test_sucede_na_primeira_tentativa_sem_reconectar(self):
        calls = {"n": 0}

        def action(attempt):
            calls["n"] += 1
            return "ok"

        result = _retry_on_disconnect(action, [], "testar")
        assert result == "ok"
        assert calls["n"] == 1

    def test_erro_nao_relacionado_a_conexao_sobe_sem_retry(self):
        def action(attempt):
            raise ValueError("erro de dados, não deve ter retry")

        with pytest.raises(ValueError):
            _retry_on_disconnect(action, [], "testar")

    def test_reconecta_e_repete_apos_queda_de_conexao(self, monkeypatch):
        monkeypatch.setattr("etl_multi_source.resilience.time.sleep", lambda s: None)

        attempts = {"n": 0}

        def action(attempt):
            attempts["n"] += 1
            if attempts["n"] == 1:
                raise ConnectionError("caiu uma vez")
            return "recuperado"

        reconnects = {"n": 0}

        class DummyConn:
            def close(self):
                pass

        def factory():
            reconnects["n"] += 1
            return DummyConn()

        ref = ReconnectingConnection("teste", factory, DummyConn())
        result = _retry_on_disconnect(action, [ref], "testar reconexão")

        assert result == "recuperado"
        assert reconnects["n"] == 1

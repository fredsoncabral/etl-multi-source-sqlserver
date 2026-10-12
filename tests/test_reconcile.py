"""Testes da comparação de chaves do modo --reconcile.

`_compare_keys` é a função que decide, sem depender de watermark/MAX, quais
linhas faltam no destino e quais sobram (existem no destino mas não mais na
origem). Aqui ela é testada com cursores "fake" em memória, cobrindo tanto
o caminho rápido (chaves inteiras -> array.array + bisect) quanto o
fallback genérico (chave não inteira -> set()).
"""
from etl_multi_source.reconcile import _compare_keys


class FakeCursor:
    """Cursor mínimo o suficiente para _fetch_target_keys_compact e
    _compare_keys: suporta .execute(), .arraysize, .fetchmany(n) e .close()."""

    def __init__(self, rows):
        self._rows = list(rows)
        self.arraysize = None

    def execute(self, sql, *args, **kwargs):
        pass

    def fetchmany(self, n):
        batch, self._rows = self._rows[:n], self._rows[n:]
        return batch

    def close(self):
        pass


class FakeConn:
    def __init__(self, rows):
        self._rows = rows

    def cursor(self):
        return FakeCursor(self._rows)


def test_chaves_inteiras_identifica_faltantes_e_extras():
    # destino tem 1, 2, 4 ; origem tem 1, 2, 3 -> falta o 3, sobra o 4
    dst_conn = FakeConn([(1,), (2,), (4,)])
    src_conn = FakeConn([(1,), (2,), (3,)])

    source_count, target_count, missing, extra = _compare_keys(
        src_conn, dst_conn, source="oracle", source_table="T", target_table="T", key_column="ID",
    )

    assert source_count == 3
    assert target_count == 3
    assert missing == [3]
    assert extra == [4]


def test_nenhuma_diferenca_quando_chaves_sao_identicas():
    rows = [(1,), (2,), (3,)]
    dst_conn = FakeConn(list(rows))
    src_conn = FakeConn(list(rows))

    _, _, missing, extra = _compare_keys(
        src_conn, dst_conn, source="hana", source_table="T", target_table="T", key_column="ID",
    )
    assert missing == []
    assert extra == []


def test_chaves_textuais_usa_caminho_generico_set():
    dst_conn = FakeConn([("A",), ("B",)])
    src_conn = FakeConn([("A",), ("C",)])

    _, _, missing, extra = _compare_keys(
        src_conn, dst_conn, source="sqlserver", source_table="T", target_table="T", key_column="COD",
    )
    assert missing == ["C"]
    assert extra == ["B"]

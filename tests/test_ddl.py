"""Testes da construção do DDL de criação de tabela.

Cobre as três situações que motivaram `build_create_table_ddl`: coluna
comum com nullability respeitada, coluna IDENTITY com seed/increment
corretos e coluna computada (fórmula + PERSISTED quando aplicável) - sem
precisar de nenhuma conexão de banco real.
"""
from etl_multi_source.ddl import build_create_table_ddl


def _extras(**overrides):
    base = dict(nullable=True, is_identity=False, identity_seed=1,
                identity_increment=1, is_computed=False,
                computed_definition=None, is_persisted=False)
    base.update(overrides)
    return base


def test_coluna_comum_nullable():
    ddl = build_create_table_ddl("CLIENTES", [("NOME", "NVARCHAR(100)")],
                                  {"NOME": _extras(nullable=True)})
    assert "[NOME] NVARCHAR(100) NULL" in ddl


def test_coluna_not_null():
    ddl = build_create_table_ddl("CLIENTES", [("ID", "INT")],
                                  {"ID": _extras(nullable=False)})
    assert "[ID] INT NOT NULL" in ddl


def test_coluna_identity_inclui_seed_e_increment(self=None):
    ddl = build_create_table_ddl(
        "CLIENTES", [("ID", "INT")],
        {"ID": _extras(nullable=False, is_identity=True, identity_seed=100, identity_increment=5)},
    )
    assert "IDENTITY(100,5)" in ddl


def test_coluna_computada_persisted():
    ddl = build_create_table_ddl(
        "PEDIDOS", [("TOTAL", "DECIMAL(10,2)")],
        {"TOTAL": _extras(is_computed=True, computed_definition="[QTD]*[PRECO]", is_persisted=True)},
    )
    assert "[TOTAL] AS ([QTD]*[PRECO]) PERSISTED" in ddl


def test_coluna_computada_nao_persisted_sem_sufixo():
    ddl = build_create_table_ddl(
        "PEDIDOS", [("TOTAL", "DECIMAL(10,2)")],
        {"TOTAL": _extras(is_computed=True, computed_definition="[QTD]*[PRECO]", is_persisted=False)},
    )
    assert "[TOTAL] AS ([QTD]*[PRECO])" in ddl
    assert "PERSISTED" not in ddl


def test_create_table_usa_dbo_e_nome_da_tabela():
    ddl = build_create_table_ddl("CLIENTES", [("ID", "INT")], {"ID": _extras(nullable=False)})
    assert ddl.startswith("CREATE TABLE dbo.CLIENTES")

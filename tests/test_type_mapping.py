"""Testes do mapeamento de tipos Oracle/HANA/SQL Server -> T-SQL.

Cobre os casos discutidos durante o desenvolvimento original: NUMBER sem
escala vira o inteiro T-SQL do menor tamanho que comporta a precisão;
VARCHAR2/CLOB sem tamanho não truncam (vão para NVARCHAR(MAX)); tipos
desconhecidos caem num fallback seguro (NVARCHAR(MAX)) em vez de falhar.
"""
import pytest

from etl_multi_source.type_mapping import map_oracle_type, map_hana_type, map_sqlserver_type


class TestMapOracleType:
    def test_number_sem_escala_pequeno_vira_smallint(self):
        assert map_oracle_type("NUMBER", 4, 0, None) == "SMALLINT"

    def test_number_sem_escala_medio_vira_int(self):
        assert map_oracle_type("NUMBER", 9, 0, None) == "INT"

    def test_number_sem_escala_grande_vira_bigint(self):
        assert map_oracle_type("NUMBER", 18, 0, None) == "BIGINT"

    def test_number_sem_escala_maior_que_bigint_vira_decimal(self):
        assert map_oracle_type("NUMBER", 30, 0, None) == "DECIMAL(30,0)"

    def test_number_sem_precisao_nenhuma_vira_decimal_38_0(self):
        assert map_oracle_type("NUMBER", None, None, None) == "DECIMAL(38,0)"

    def test_number_com_escala_vira_decimal_precisao_escala(self):
        assert map_oracle_type("NUMBER", 10, 2, None) == "DECIMAL(10,2)"

    def test_varchar2_com_tamanho(self):
        assert map_oracle_type("VARCHAR2", None, None, 50) == "NVARCHAR(50)"

    def test_varchar2_sem_tamanho_fallback_4000(self):
        assert map_oracle_type("VARCHAR2", None, None, None) == "NVARCHAR(4000)"

    def test_clob_vira_nvarchar_max(self):
        assert map_oracle_type("CLOB", None, None, None) == "NVARCHAR(MAX)"

    def test_blob_vira_varbinary_max(self):
        assert map_oracle_type("BLOB", None, None, None) == "VARBINARY(MAX)"

    def test_date_vira_datetime2(self):
        assert map_oracle_type("DATE", None, None, None) == "DATETIME2"

    def test_tipo_desconhecido_cai_no_fallback_seguro(self):
        assert map_oracle_type("XMLTYPE", None, None, None) == "NVARCHAR(MAX)"


class TestMapHanaType:
    def test_tipos_diretos(self):
        assert map_hana_type("INTEGER", None, None) == "INT"
        assert map_hana_type("BOOLEAN", None, None) == "BIT"
        assert map_hana_type("DOUBLE", None, None) == "FLOAT"

    def test_decimal_com_precisao_e_escala(self):
        assert map_hana_type("DECIMAL", 20, 4) == "DECIMAL(20,4)"

    def test_decimal_sem_parametros_usa_default(self):
        assert map_hana_type("DECIMAL", None, None) == "DECIMAL(34,0)"

    def test_nvarchar_com_tamanho(self):
        assert map_hana_type("NVARCHAR", 100, None) == "NVARCHAR(100)"

    def test_tipo_desconhecido_cai_no_fallback_seguro(self):
        assert map_hana_type("ST_GEOMETRY", None, None) == "NVARCHAR(MAX)"


class TestMapSqlServerType:
    def test_tipos_sem_parametro_passthrough(self):
        assert map_sqlserver_type("int", None, None, None) == "INT"
        assert map_sqlserver_type("datetime2", None, None, None) == "DATETIME2"

    def test_varchar_max(self):
        assert map_sqlserver_type("varchar", -1, None, None) == "VARCHAR(MAX)"

    def test_varchar_com_tamanho(self):
        assert map_sqlserver_type("nvarchar", 50, None, None) == "NVARCHAR(50)"

    def test_decimal_com_precisao_e_escala(self):
        assert map_sqlserver_type("decimal", None, 12, 3) == "DECIMAL(12,3)"

    def test_decimal_sem_parametros_usa_default(self):
        assert map_sqlserver_type("numeric", None, None, None) == "DECIMAL(18,0)"

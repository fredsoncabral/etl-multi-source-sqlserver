"""Mapeamento de tipos de coluna das origens (Oracle, SAP HANA, SQL Server)
para o dialeto T-SQL do SQL Server de destino.

Cada função recebe os metadados de tipo tal como expostos pelo catálogo
nativo de cada banco (ALL_TAB_COLUMNS no Oracle, TABLE_COLUMNS no HANA,
INFORMATION_SCHEMA.COLUMNS no SQL Server) e devolve a string de tipo T-SQL
mais próxima. Onde não há equivalente exato, o fallback é sempre um tipo
"grande o suficiente" (NVARCHAR(MAX), DECIMAL(38,0) etc.) para nunca truncar
ou rejeitar dado da origem - fidelidade de dado tem prioridade sobre
otimização de armazenamento.
"""

# =============================================================================
# MAPEAMENTO DE TIPOS -> SQL SERVER
# =============================================================================
 
def map_oracle_type(data_type: str, precision, scale, length) -> str:
    """Converte um tipo de coluna Oracle (ALL_TAB_COLUMNS) para o tipo
    equivalente mais próximo em T-SQL."""
    data_type = (data_type or "").upper()
 
    if data_type == "VARCHAR2" or data_type == "VARCHAR":
        return f"NVARCHAR({length if length else 4000})"
    if data_type == "NVARCHAR2":
        return f"NVARCHAR({length if length else 4000})"
    if data_type == "CHAR":
        return f"NCHAR({length if length else 1})"
    if data_type == "NUMBER":
        if scale is None or scale == 0:
            # sem casas decimais -> usa inteiro apropriado ao tamanho
            if precision is None:
                return "DECIMAL(38,0)"
            if precision <= 4:
                return "SMALLINT"
            if precision <= 9:
                return "INT"
            if precision <= 18:
                return "BIGINT"
            return f"DECIMAL({precision},0)"
        return f"DECIMAL({precision or 38},{scale})"
    if data_type in ("FLOAT", "BINARY_FLOAT"):
        return "REAL"
    if data_type == "BINARY_DOUBLE":
        return "FLOAT"
    if data_type == "DATE":
        return "DATETIME2"
    if data_type.startswith("TIMESTAMP"):
        return "DATETIME2"
    if data_type == "CLOB":
        return "NVARCHAR(MAX)"
    if data_type == "BLOB":
        return "VARBINARY(MAX)"
    if data_type == "RAW":
        return "VARBINARY(MAX)" if not length or length > 8000 else f"VARBINARY({length})"
    # fallback seguro
    return "NVARCHAR(MAX)"
 
 
def map_hana_type(data_type: str, length, scale) -> str:
    """Converte um tipo de coluna SAP HANA (TABLE_COLUMNS) para T-SQL."""
    data_type = (data_type or "").upper()
 
    mapping_direct = {
        "TINYINT": "TINYINT",
        "SMALLINT": "SMALLINT",
        "INTEGER": "INT",
        "BIGINT": "BIGINT",
        "REAL": "REAL",
        "DOUBLE": "FLOAT",
        "BOOLEAN": "BIT",
        "DATE": "DATE",
        "TIME": "TIME",
        "SECONDDATE": "DATETIME2",
        "TIMESTAMP": "DATETIME2",
    }
    if data_type in mapping_direct:
        return mapping_direct[data_type]
 
    if data_type == "DECIMAL":
        return f"DECIMAL({length or 34},{scale or 0})"
    if data_type in ("VARCHAR", "ALPHANUM", "SHORTTEXT"):
        return f"VARCHAR({length if length else 8000})"
    if data_type == "NVARCHAR":
        return f"NVARCHAR({length if length else 4000})"
    if data_type == "CHAR":
        return f"CHAR({length or 1})"
    if data_type == "NCLOB" or data_type == "TEXT":
        return "NVARCHAR(MAX)"
    if data_type == "CLOB":
        return "VARCHAR(MAX)"
    if data_type == "BLOB":
        return "VARBINARY(MAX)"
    return "NVARCHAR(MAX)"
 
 
def map_sqlserver_type(data_type: str, char_max_length, numeric_precision, numeric_scale) -> str:
    """Reconstrói a definição de tipo T-SQL a partir do
    INFORMATION_SCHEMA.COLUMNS de um SQL Server de origem. Na prática é
    quase um "passthrough" (origem e destino falam o mesmo dialeto), só
    precisa remontar tamanho/precisão/escala corretamente."""
    data_type = (data_type or "").lower()
 
    types_sem_parametro = {
        "int", "bigint", "smallint", "tinyint", "bit", "float", "real",
        "date", "time", "datetime", "datetime2", "smalldatetime",
        "datetimeoffset", "money", "smallmoney", "uniqueidentifier",
        "text", "ntext", "image", "xml",
    }
    if data_type in types_sem_parametro:
        return data_type.upper()
 
    if data_type in ("varchar", "char", "nvarchar", "nchar", "varbinary", "binary"):
        if char_max_length == -1:
            return f"{data_type.upper()}(MAX)"
        # nvarchar/nchar armazenam length em bytes (UTF-16) no catálogo;
        # como já assumimos NVARCHAR no destino para texto, length é ok 1:1.
        return f"{data_type.upper()}({char_max_length})"
 
    if data_type in ("decimal", "numeric"):
        return f"DECIMAL({numeric_precision or 18},{numeric_scale or 0})"
 
    # fallback seguro
    return "NVARCHAR(MAX)"
 

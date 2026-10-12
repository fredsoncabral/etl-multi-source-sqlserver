"""Configuração central de logging do ETL.

Todo módulo do pacote importa o logger `log` daqui, garantindo um único
formato e um único handler em toda a aplicação, inclusive quando o script
é usado como biblioteca (não só via CLI).
"""
import logging

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(message)s",
)
log = logging.getLogger("etl")

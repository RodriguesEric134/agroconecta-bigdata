"""Configurações do pipeline, lidas de variáveis de ambiente (ver .env.example)."""
from __future__ import annotations

import os
from pathlib import Path

# Diretório de chegada (landing zone) — montado no container do Airflow
CHEGADA_DIR = Path(os.getenv("AGRO_CHEGADA_DIR", "/opt/airflow/chegada"))

# Lista de municípios atendidos
MUNICIPIOS_CONFIG = Path(
    os.getenv("AGRO_MUNICIPIOS_CONFIG", "/opt/airflow/config/municipios_atendidos.json")
)

# Arquivo com cadastro de produtores exportado do MVP (fonte com dados pessoais -> LGPD)
PRODUTORES_CSV = Path(
    os.getenv("AGRO_PRODUTORES_CSV", "/opt/airflow/dados_exemplo/produtores_cadastrados.csv")
)

# APIs públicas
IBGE_LOCALIDADES_URL = "https://servicodados.ibge.gov.br/api/v1/localidades/municipios/{codigo}"
IBGE_MALHAS_METADADOS_URL = "https://servicodados.ibge.gov.br/api/v3/malhas/municipios/{codigo}/metadados"
OPEN_METEO_URL = "https://api.open-meteo.com/v1/forecast"

OPEN_METEO_VARIAVEIS_DIARIAS = [
    "temperature_2m_max",
    "temperature_2m_min",
    "temperature_2m_mean",
    "precipitation_sum",
    "precipitation_probability_max",
    "relative_humidity_2m_mean",
    "relative_humidity_2m_max",
    "relative_humidity_2m_min",
    "et0_fao_evapotranspiration",
    "wind_speed_10m_max",
]
OPEN_METEO_PAST_DAYS = int(os.getenv("AGRO_OPEN_METEO_PAST_DAYS", "2"))
OPEN_METEO_FORECAST_DAYS = int(os.getenv("AGRO_OPEN_METEO_FORECAST_DAYS", "7"))

HTTP_TIMEOUT = int(os.getenv("AGRO_HTTP_TIMEOUT", "30"))
USER_AGENT = "AgroConecta-DataPipeline/1.0 (projeto academico FIAP)"

# Databricks (opcional: se não configurado, o DAG apenas grava em chegada/)
DATABRICKS_HOST = os.getenv("DATABRICKS_HOST", "").rstrip("/")
DATABRICKS_TOKEN = os.getenv("DATABRICKS_TOKEN", "")
DATABRICKS_JOB_ID = os.getenv("DATABRICKS_JOB_ID", "")
# Volume do Unity Catalog onde os arquivos brutos chegam
DATABRICKS_VOLUME_PATH = os.getenv(
    "DATABRICKS_VOLUME_PATH", "/Volumes/workspace/agroconecta_bronze/chegada"
).rstrip("/")


def databricks_configurado() -> bool:
    return bool(DATABRICKS_HOST and DATABRICKS_TOKEN)

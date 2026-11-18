"""DAG diário do AgroConecta: coleta IBGE + Open-Meteo (+ cadastro do MVP),
grava em chegada/, envia ao Volume do Databricks e dispara o Job do Lakehouse
(Bronze -> Prata -> Ouro).

    coletar_ibge ──► coletar_open_meteo ──┐
    coletar_produtores_mvp ───────────────┼─► validar_chegada ─► databricks_configurado?
                                          │        ─► enviar_para_volume ─► executar_lakehouse
"""
from __future__ import annotations

import logging
from datetime import timedelta
from pathlib import Path

import pendulum
from airflow.decorators import dag, task
from airflow.operators.python import get_current_context

from agroconecta import coletores, config, databricks_client

log = logging.getLogger(__name__)
TZ = pendulum.timezone("America/Sao_Paulo")


def _data_referencia() -> str:
    """Data a que a coleta se refere (fim do intervalo, no fuso de Brasília)."""
    ctx = get_current_context()
    return ctx["data_interval_end"].in_timezone(TZ).to_date_string()


@dag(
    dag_id="agroconecta_pipeline_clima",
    description="Coleta diária IBGE + Open-Meteo e carga no Lakehouse (Databricks)",
    schedule="0 6 * * *",  # todos os dias às 06:00 (horário de Brasília)
    start_date=pendulum.datetime(2026, 11, 1, tz=TZ),
    catchup=False,
    max_active_runs=1,
    default_args={
        "owner": "agroconecta-dados",
        "retries": 2,
        "retry_delay": timedelta(minutes=5),
        "retry_exponential_backoff": True,
    },
    tags=["agroconecta", "big-data", "lakehouse"],
)
def agroconecta_pipeline_clima():

    @task
    def coletar_ibge() -> dict:
        data_ref = _data_referencia()
        codigos = coletores.carregar_municipios()
        arquivo, coordenadas = coletores.coletar_ibge(codigos, config.CHEGADA_DIR, data_ref)
        if len(coordenadas) < len(codigos):
            log.warning("%s de %s municípios coletados", len(coordenadas), len(codigos))
        return {"arquivo": str(arquivo), "coordenadas": coordenadas}

    @task
    def coletar_open_meteo(ibge: dict) -> str:
        data_ref = _data_referencia()
        arquivo = coletores.coletar_open_meteo(ibge["coordenadas"], config.CHEGADA_DIR, data_ref)
        return str(arquivo)

    @task
    def coletar_produtores_mvp() -> str:
        data_ref = _data_referencia()
        return str(coletores.coletar_produtores(config.PRODUTORES_CSV, config.CHEGADA_DIR, data_ref))

    @task
    def validar_chegada(ibge: dict, clima: str, produtores: str) -> list[str]:
        arquivos = [ibge["arquivo"], clima, produtores]
        for arq in arquivos:
            qtd = coletores.validar_arquivo(Path(arq))
            log.info("OK %s (%s registros)", arq, qtd)
        return arquivos

    @task.short_circuit
    def databricks_disponivel() -> bool:
        if not config.databricks_configurado():
            log.warning("DATABRICKS_HOST/TOKEN não definidos: arquivos ficam apenas em chegada/.")
            return False
        return True

    @task
    def enviar_para_volume(arquivos: list[str]) -> list[str]:
        return [databricks_client.enviar_para_volume(Path(a), config.CHEGADA_DIR) for a in arquivos]

    @task
    def executar_lakehouse(_enviados: list[str]) -> dict:
        if not config.DATABRICKS_JOB_ID:
            raise ValueError("Defina DATABRICKS_JOB_ID com o id do Job 'agroconecta_lakehouse'")
        return databricks_client.executar_job(
            config.DATABRICKS_JOB_ID, {"data_referencia": _data_referencia()}
        )

    ibge = coletar_ibge()
    clima = coletar_open_meteo(ibge)
    produtores = coletar_produtores_mvp()
    arquivos = validar_chegada(ibge, clima, produtores)
    enviados = enviar_para_volume(arquivos)
    arquivos >> databricks_disponivel() >> enviados
    executar_lakehouse(enviados)


agroconecta_pipeline_clima()

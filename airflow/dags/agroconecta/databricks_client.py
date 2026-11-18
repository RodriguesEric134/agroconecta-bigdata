"""Cliente mínimo da API REST do Databricks: upload para Volume e execução de Job."""
from __future__ import annotations

import logging
import time
from pathlib import Path

import requests

from . import config

log = logging.getLogger(__name__)


def _headers() -> dict:
    return {"Authorization": f"Bearer {config.DATABRICKS_TOKEN}"}


def enviar_para_volume(arquivo_local: Path, chegada_dir: Path) -> str:
    """Envia o arquivo ao Volume mantendo a mesma estrutura de pastas de chegada/.

    Usa a Files API: PUT /api/2.0/fs/files{caminho}
    """
    relativo = Path(arquivo_local).relative_to(chegada_dir).as_posix()
    destino = f"{config.DATABRICKS_VOLUME_PATH}/{relativo}"
    url = f"{config.DATABRICKS_HOST}/api/2.0/fs/files{destino}"
    with Path(arquivo_local).open("rb") as f:
        resp = requests.put(url, headers=_headers(), params={"overwrite": "true"},
                            data=f, timeout=120)
    resp.raise_for_status()
    log.info("Enviado %s -> %s", arquivo_local, destino)
    return destino


def executar_job(job_id: str, parametros: dict | None = None,
                 intervalo: int = 20, limite_seg: int = 3600) -> dict:
    """Dispara o Job (Bronze -> Prata -> Ouro) e aguarda a conclusão."""
    url = f"{config.DATABRICKS_HOST}/api/2.1/jobs/run-now"
    corpo = {"job_id": int(job_id)}
    if parametros:
        corpo["job_parameters"] = parametros
    resp = requests.post(url, headers=_headers(), json=corpo, timeout=60)
    resp.raise_for_status()
    run_id = resp.json()["run_id"]
    log.info("Job %s iniciado (run_id=%s)", job_id, run_id)

    inicio = time.time()
    while time.time() - inicio < limite_seg:
        time.sleep(intervalo)
        estado = requests.get(f"{config.DATABRICKS_HOST}/api/2.1/jobs/runs/get",
                              headers=_headers(), params={"run_id": run_id},
                              timeout=60).json()["state"]
        log.info("run_id=%s estado=%s", run_id, estado)
        if estado.get("life_cycle_state") in {"TERMINATED", "SKIPPED", "INTERNAL_ERROR"}:
            if estado.get("result_state") != "SUCCESS":
                raise RuntimeError(f"Job Databricks falhou: {estado}")
            return {"run_id": run_id, **estado}
    raise TimeoutError(f"Job {job_id} (run {run_id}) excedeu {limite_seg}s")

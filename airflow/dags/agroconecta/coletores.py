"""Coletores das fontes de dados do AgroConecta.

Cada coletor grava um arquivo JSON Lines (um registro por linha) no diretório de
chegada, particionado por fonte e data de referência:

    chegada/<fonte>/dt=YYYY-MM-DD/<fonte>_<timestamp>.jsonl

Cada linha preserva a resposta ORIGINAL da API (campo ``payload``) e recebe
metadados de linhagem (``_fonte``, ``_coletado_em``, ``_url``), que serão
carregados como estão na camada Bronze.
"""
from __future__ import annotations

import csv
import json
import logging
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable

import requests

from . import config

log = logging.getLogger(__name__)

try:  # usa o repositório de certificados do SO (antivírus/proxy corporativo)
    import truststore

    truststore.inject_into_ssl()
except ImportError:  # pragma: no cover
    pass


# --------------------------------------------------------------------------- #
# Utilitários
# --------------------------------------------------------------------------- #
def _agora_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def _get_json(sessao: requests.Session, url: str, params: dict | None = None,
              tentativas: int = 3) -> Any:
    """GET com retentativas e backoff exponencial."""
    ultimo_erro: Exception | None = None
    for tentativa in range(1, tentativas + 1):
        try:
            resp = sessao.get(url, params=params, timeout=config.HTTP_TIMEOUT)
            resp.raise_for_status()
            return resp.json()
        except (requests.RequestException, ValueError) as exc:
            ultimo_erro = exc
            espera = 2 ** tentativa
            log.warning("Falha em %s (tentativa %s/%s): %s. Nova tentativa em %ss",
                        url, tentativa, tentativas, exc, espera)
            time.sleep(espera)
    raise RuntimeError(f"Não foi possível obter {url}: {ultimo_erro}")


def _sessao() -> requests.Session:
    s = requests.Session()
    s.headers.update({"User-Agent": config.USER_AGENT, "Accept": "application/json"})
    return s


def _gravar_jsonl(registros: Iterable[dict], chegada_dir: Path, fonte: str,
                  data_ref: str) -> Path:
    destino = chegada_dir / fonte / f"dt={data_ref}"
    destino.mkdir(parents=True, exist_ok=True)
    carimbo = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    arquivo = destino / f"{fonte}_{carimbo}.jsonl"
    qtd = 0
    with arquivo.open("w", encoding="utf-8") as f:
        for reg in registros:
            f.write(json.dumps(reg, ensure_ascii=False) + "\n")
            qtd += 1
    if qtd == 0:
        arquivo.unlink()
        raise ValueError(f"Nenhum registro coletado para a fonte '{fonte}'")
    log.info("Fonte %s: %s registros gravados em %s", fonte, qtd, arquivo)
    return arquivo


def carregar_municipios(caminho: Path = config.MUNICIPIOS_CONFIG) -> list[int]:
    dados = json.loads(Path(caminho).read_text(encoding="utf-8"))
    codigos = [int(m["codigo_ibge"]) for m in dados["municipios"]]
    if not codigos:
        raise ValueError("Lista de municípios atendidos está vazia")
    return codigos


# --------------------------------------------------------------------------- #
# Fonte 1 — IBGE (localidades + metadados de malha com centróide)
# --------------------------------------------------------------------------- #
def coletar_ibge(codigos: list[int], chegada_dir: Path, data_ref: str) -> tuple[Path, list[dict]]:
    """Coleta dados administrativos e o centróide (lat/lon) de cada município.

    Retorna o caminho do arquivo gravado e a lista de coordenadas, usada em
    seguida pela coleta do Open-Meteo.
    """
    sessao = _sessao()
    registros, coordenadas = [], []
    for codigo in codigos:
        url_loc = config.IBGE_LOCALIDADES_URL.format(codigo=codigo)
        url_malha = config.IBGE_MALHAS_METADADOS_URL.format(codigo=codigo)
        try:
            localidade = _get_json(sessao, url_loc)
            malha = _get_json(sessao, url_malha)
        except RuntimeError as exc:
            log.error("Município %s ignorado: %s", codigo, exc)
            continue
        if not localidade or not malha:
            log.error("Município %s não encontrado no IBGE", codigo)
            continue
        centroide = malha[0]["centroide"]
        registros.append({
            "_fonte": "ibge",
            "_coletado_em": _agora_iso(),
            "_url": url_loc,
            "codigo_ibge": codigo,
            "payload": {"localidade": localidade, "malha_metadados": malha[0]},
        })
        coordenadas.append({
            "codigo_ibge": codigo,
            "nome": localidade["nome"],
            "latitude": centroide["latitude"],
            "longitude": centroide["longitude"],
        })
    arquivo = _gravar_jsonl(registros, chegada_dir, "ibge", data_ref)
    return arquivo, coordenadas


# --------------------------------------------------------------------------- #
# Fonte 2 — Open-Meteo (clima observado recente + previsão)
# --------------------------------------------------------------------------- #
def coletar_open_meteo(coordenadas: list[dict], chegada_dir: Path, data_ref: str) -> Path:
    sessao = _sessao()
    registros = []
    for c in coordenadas:
        params = {
            "latitude": c["latitude"],
            "longitude": c["longitude"],
            "daily": ",".join(config.OPEN_METEO_VARIAVEIS_DIARIAS),
            "timezone": "America/Sao_Paulo",
            "past_days": config.OPEN_METEO_PAST_DAYS,
            "forecast_days": config.OPEN_METEO_FORECAST_DAYS,
        }
        try:
            payload = _get_json(sessao, config.OPEN_METEO_URL, params=params)
        except RuntimeError as exc:
            log.error("Clima de %s (%s) não coletado: %s", c["nome"], c["codigo_ibge"], exc)
            continue
        registros.append({
            "_fonte": "open_meteo",
            "_coletado_em": _agora_iso(),
            "_url": config.OPEN_METEO_URL,
            "codigo_ibge": c["codigo_ibge"],
            "data_referencia": data_ref,
            "parametros": params,
            "payload": payload,
        })
        time.sleep(0.2)  # cortesia com a API pública
    return _gravar_jsonl(registros, chegada_dir, "open_meteo", data_ref)


# --------------------------------------------------------------------------- #
# Fonte 3 — Cadastro de produtores do MVP (contém dados pessoais -> LGPD)
# --------------------------------------------------------------------------- #
def coletar_produtores(csv_path: Path, chegada_dir: Path, data_ref: str) -> Path:
    """Exporta o cadastro de produtores do MVP. O CPF existe apenas na Bronze
    (acesso restrito); a Prata guarda só o hash e a Ouro mascara nome/contato."""
    with Path(csv_path).open(encoding="utf-8") as f:
        linhas = list(csv.DictReader(f))
    coletado = _agora_iso()
    registros = ({"_fonte": "mvp_produtores", "_coletado_em": coletado,
                  "_url": str(csv_path), "payload": linha} for linha in linhas)
    return _gravar_jsonl(registros, chegada_dir, "mvp_produtores", data_ref)


# --------------------------------------------------------------------------- #
# Verificação de qualidade na chegada (antes de enviar ao Lakehouse)
# --------------------------------------------------------------------------- #
def validar_arquivo(arquivo: Path, campos_obrigatorios: tuple[str, ...] = ("_fonte", "_coletado_em", "payload")) -> int:
    """Confere se o arquivo é JSONL válido e contém os campos de linhagem."""
    qtd = 0
    with Path(arquivo).open(encoding="utf-8") as f:
        for n, linha in enumerate(f, start=1):
            reg = json.loads(linha)
            faltando = [c for c in campos_obrigatorios if c not in reg]
            if faltando:
                raise ValueError(f"{arquivo}:{n} sem campos {faltando}")
            qtd += 1
    if qtd == 0:
        raise ValueError(f"{arquivo} está vazio")
    return qtd

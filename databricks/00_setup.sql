-- Databricks notebook source
-- MAGIC %md
-- MAGIC # AgroConecta — 00 Setup do Lakehouse
-- MAGIC Cria os schemas das camadas **Bronze / Prata / Ouro** e os Volumes de chegada e de controle.
-- MAGIC Execute uma única vez. No Databricks Free Edition o catálogo padrão é `workspace`.

-- COMMAND ----------

USE CATALOG workspace;

CREATE SCHEMA IF NOT EXISTS agroconecta_bronze COMMENT 'Dados brutos, como recebidos das fontes (acesso restrito)';
CREATE SCHEMA IF NOT EXISTS agroconecta_prata  COMMENT 'Dados limpos, padronizados e validados';
CREATE SCHEMA IF NOT EXISTS agroconecta_ouro   COMMENT 'Modelo dimensional consumido pelo AgroConecta';

-- COMMAND ----------

-- Volume onde o Airflow deposita os arquivos (espelho do diretório chegada/)
CREATE VOLUME IF NOT EXISTS agroconecta_bronze.chegada COMMENT 'Landing zone — arquivos JSONL enviados pelo Airflow';

-- Volume de controle: checkpoints do Auto Loader / Structured Streaming (ingestão incremental)
CREATE VOLUME IF NOT EXISTS agroconecta_bronze.controle COMMENT 'Checkpoints de ingestão incremental';

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Segredo para pseudonimização (LGPD)
-- MAGIC Crie o *secret scope* pela CLI (o sal nunca fica no código):
-- MAGIC ```
-- MAGIC databricks secrets create-scope agroconecta
-- MAGIC databricks secrets put-secret agroconecta lgpd_salt
-- MAGIC ```

-- COMMAND ----------

SHOW SCHEMAS LIKE 'agroconecta*';

-- Databricks notebook source
-- MAGIC %md
-- MAGIC # AgroConecta — 03 Camada Ouro (modelo dimensional)
-- MAGIC
-- MAGIC ```
-- MAGIC             dim_tempo                 dim_localidade
-- MAGIC                 │                            │
-- MAGIC                 └──────────► fato_clima ◄────┘
-- MAGIC                                   ▲
-- MAGIC                      dim_produtor (mascarada) ── via sk_localidade
-- MAGIC ```
-- MAGIC * **fato_clima** — grão: 1 linha por município × dia (observado ou previsão), com indicadores agroclimáticos.
-- MAGIC * **dim_localidade** — município, UF, região, coordenadas.
-- MAGIC * **dim_tempo** — calendário com estação do ano e ano-safra.
-- MAGIC * **dim_produtor** — produtores pseudonimizados e com dados de contato mascarados (LGPD).

-- COMMAND ----------

USE CATALOG workspace;
USE SCHEMA agroconecta_ouro;

-- COMMAND ----------

-- MAGIC %md ## dim_localidade

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS dim_localidade (
  sk_localidade    INT     COMMENT 'Chave substituta = código IBGE do município',
  codigo_ibge      INT,
  municipio        STRING,
  municipio_normalizado STRING,
  uf               STRING,
  estado           STRING,
  regiao           STRING,
  mesorregiao      STRING,
  microrregiao     STRING,
  latitude         DOUBLE,
  longitude        DOUBLE,
  area_km2         DOUBLE,
  atualizado_em    TIMESTAMP
) COMMENT 'Dimensão de localidades atendidas (fonte: IBGE)';

MERGE INTO dim_localidade AS t
USING (
  SELECT codigo_ibge AS sk_localidade, codigo_ibge, municipio, municipio_normalizado, uf, estado, regiao,
         mesorregiao, microrregiao, latitude, longitude, area_km2, current_timestamp() AS atualizado_em
  FROM agroconecta_prata.municipios
) AS s
ON t.sk_localidade = s.sk_localidade
WHEN MATCHED THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *;

-- COMMAND ----------

-- MAGIC %md ## dim_tempo
-- MAGIC Gerada a partir do intervalo de datas existente na Prata (com folga de 1 ano à frente).

-- COMMAND ----------

CREATE OR REPLACE TABLE dim_tempo COMMENT 'Dimensão calendário (estação do ano no hemisfério sul e ano-safra jul-jun)' AS
WITH limites AS (
  SELECT least(min(data), date'2026-01-01') AS ini, greatest(max(data), current_date()) + 365 AS fim
  FROM agroconecta_prata.clima_diario
),
dias AS (SELECT explode(sequence(ini, fim, interval 1 day)) AS data FROM limites)
SELECT
  CAST(date_format(data, 'yyyyMMdd') AS INT)                 AS sk_tempo,
  data,
  year(data)                                                 AS ano,
  month(data)                                                AS mes,
  CASE month(data) WHEN 1 THEN 'Janeiro' WHEN 2 THEN 'Fevereiro' WHEN 3 THEN 'Março' WHEN 4 THEN 'Abril'
       WHEN 5 THEN 'Maio' WHEN 6 THEN 'Junho' WHEN 7 THEN 'Julho' WHEN 8 THEN 'Agosto' WHEN 9 THEN 'Setembro'
       WHEN 10 THEN 'Outubro' WHEN 11 THEN 'Novembro' ELSE 'Dezembro' END AS nome_mes,
  quarter(data)                                              AS trimestre,
  weekofyear(data)                                           AS semana_ano,
  dayofmonth(data)                                           AS dia,
  CASE dayofweek(data) WHEN 1 THEN 'Domingo' WHEN 2 THEN 'Segunda' WHEN 3 THEN 'Terça' WHEN 4 THEN 'Quarta'
       WHEN 5 THEN 'Quinta' WHEN 6 THEN 'Sexta' ELSE 'Sábado' END   AS dia_semana,
  dayofweek(data) IN (1, 7)                                  AS fim_de_semana,
  CASE
    WHEN (month(data) = 12 AND day(data) >= 21) OR month(data) IN (1, 2) OR (month(data) = 3 AND day(data) < 20) THEN 'Verão'
    WHEN month(data) IN (3, 4, 5) OR (month(data) = 6 AND day(data) < 21) THEN 'Outono'
    WHEN month(data) IN (6, 7, 8) OR (month(data) = 9 AND day(data) < 22) THEN 'Inverno'
    ELSE 'Primavera' END                                     AS estacao_ano,
  -- Ano-safra brasileiro: julho a junho (ex.: 2026/27)
  CASE WHEN month(data) >= 7 THEN concat(year(data), '/', right(CAST(year(data) + 1 AS STRING), 2))
       ELSE concat(year(data) - 1, '/', right(CAST(year(data) AS STRING), 2)) END AS ano_safra
FROM dias;

-- COMMAND ----------

-- MAGIC %md ## fato_clima
-- MAGIC Indicadores agroclimáticos derivados (regras de negócio):
-- MAGIC | Alerta | Regra |
-- MAGIC |---|---|
-- MAGIC | Geada | temperatura mínima ≤ 3 °C |
-- MAGIC | Chuva forte | precipitação ≥ 50 mm/dia |
-- MAGIC | Calor extremo | temperatura máxima ≥ 35 °C |
-- MAGIC | Estresse hídrico | chuva < 1 mm **e** evapotranspiração ≥ 5 mm |
-- MAGIC | Vento forte | vento máx. ≥ 15 m/s (~54 km/h) |

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS fato_clima (
  sk_tempo              INT,
  sk_localidade         INT,
  tipo_registro         STRING  COMMENT 'observado | previsao',
  temp_max_c            DOUBLE,
  temp_min_c            DOUBLE,
  temp_media_c          DOUBLE,
  amplitude_termica_c   DOUBLE,
  precipitacao_mm       DOUBLE,
  prob_chuva_pct        DOUBLE,
  umidade_media_pct     DOUBLE,
  umidade_min_pct       DOUBLE,
  umidade_max_pct       DOUBLE,
  evapotranspiracao_mm  DOUBLE,
  balanco_hidrico_mm    DOUBLE  COMMENT 'precipitação - evapotranspiração',
  vento_max_ms          DOUBLE,
  alerta_geada          BOOLEAN,
  alerta_chuva_forte    BOOLEAN,
  alerta_calor          BOOLEAN,
  alerta_estresse_hidrico BOOLEAN,
  alerta_vento          BOOLEAN,
  nivel_risco           STRING  COMMENT 'baixo | moderado | alto',
  coletado_em           TIMESTAMP,
  atualizado_em         TIMESTAMP
) COMMENT 'Fato clima diário por município (grão: município x dia)';

MERGE INTO fato_clima AS t
USING (
  SELECT *,
    CASE WHEN (CAST(alerta_geada AS INT) + CAST(alerta_chuva_forte AS INT) + CAST(alerta_calor AS INT)
             + CAST(alerta_estresse_hidrico AS INT) + CAST(alerta_vento AS INT)) >= 2 THEN 'alto'
         WHEN alerta_geada OR alerta_chuva_forte OR alerta_calor OR alerta_estresse_hidrico OR alerta_vento THEN 'moderado'
         ELSE 'baixo' END AS nivel_risco
  FROM (
    SELECT
      CAST(date_format(c.data, 'yyyyMMdd') AS INT)       AS sk_tempo,
      c.codigo_ibge                                      AS sk_localidade,
      c.tipo_registro,
      c.temp_max_c, c.temp_min_c, c.temp_media_c,
      round(c.temp_max_c - c.temp_min_c, 1)              AS amplitude_termica_c,
      c.precipitacao_mm, c.prob_chuva_pct,
      c.umidade_media_pct, c.umidade_min_pct, c.umidade_max_pct,
      c.evapotranspiracao_mm,
      round(c.precipitacao_mm - coalesce(c.evapotranspiracao_mm, 0), 2) AS balanco_hidrico_mm,
      c.vento_max_ms,
      c.temp_min_c <= 3                                   AS alerta_geada,
      c.precipitacao_mm >= 50                             AS alerta_chuva_forte,
      c.temp_max_c >= 35                                  AS alerta_calor,
      c.precipitacao_mm < 1 AND coalesce(c.evapotranspiracao_mm, 0) >= 5 AS alerta_estresse_hidrico,
      coalesce(c.vento_max_ms, 0) >= 15                   AS alerta_vento,
      c._coletado_em                                      AS coletado_em,
      current_timestamp()                                 AS atualizado_em
    FROM agroconecta_prata.clima_diario c
    JOIN dim_localidade l ON l.sk_localidade = c.codigo_ibge   -- integridade referencial
  )
) AS s
ON t.sk_tempo = s.sk_tempo AND t.sk_localidade = s.sk_localidade
WHEN MATCHED AND s.coletado_em >= t.coletado_em THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *;

-- COMMAND ----------

-- MAGIC %md ## dim_produtor — mascaramento LGPD
-- MAGIC * `id_produtor`: hash SHA-256 + sal (pseudônimo; CPF nunca chega à Ouro)
-- MAGIC * `nome_mascarado`: só o primeiro nome + iniciais (ex.: *Rita S. S.*)
-- MAGIC * `telefone_mascarado`: DDD + 4 últimos dígitos (ex.: *(15) \*\*\*\*\*-2535*)
-- MAGIC * `email_mascarado`: 1ª letra + domínio (ex.: *r\*\*\*@exemplo.com*)

-- COMMAND ----------

CREATE OR REPLACE TABLE dim_produtor COMMENT 'Produtores pseudonimizados e mascarados (LGPD)' AS
SELECT
  p.id_produtor,
  concat_ws(' ',
    element_at(split(p.nome, ' '), 1),
    array_join(transform(slice(split(p.nome, ' '), 2, 10), x -> concat(left(x, 1), '.')), ' ')
  )                                                              AS nome_mascarado,
  CASE WHEN length(p.telefone) >= 10
       THEN concat('(', left(p.telefone, 2), ') *****-', right(p.telefone, 4)) END AS telefone_mascarado,
  regexp_replace(p.email, '^(.)[^@]*', '$1***')                  AS email_mascarado,
  p.codigo_ibge                                                  AS sk_localidade,
  p.cultura_principal,
  CASE WHEN p.area_ha < 5 THEN 'até 5 ha'
       WHEN p.area_ha < 20 THEN '5 a 20 ha'
       ELSE 'acima de 20 ha' END                                 AS faixa_area,
  p.area_ha,
  p.data_cadastro,
  current_timestamp()                                            AS atualizado_em
FROM agroconecta_prata.produtores p;

-- COMMAND ----------

-- MAGIC %md ## Visão de consumo pelo app: alertas por produtor (próximos 7 dias)

-- COMMAND ----------

CREATE OR REPLACE VIEW vw_alertas_produtor COMMENT 'Alertas climáticos dos próximos 7 dias por produtor (sem dados pessoais)' AS
SELECT
  p.id_produtor, p.cultura_principal, l.municipio, l.uf, t.data, t.dia_semana,
  f.temp_min_c, f.temp_max_c, f.precipitacao_mm, f.prob_chuva_pct, f.nivel_risco,
  concat_ws(', ',
    CASE WHEN f.alerta_geada THEN 'geada' END,
    CASE WHEN f.alerta_chuva_forte THEN 'chuva forte' END,
    CASE WHEN f.alerta_calor THEN 'calor extremo' END,
    CASE WHEN f.alerta_estresse_hidrico THEN 'estresse hídrico' END,
    CASE WHEN f.alerta_vento THEN 'vento forte' END) AS alertas
FROM fato_clima f
JOIN dim_tempo t      ON t.sk_tempo = f.sk_tempo
JOIN dim_localidade l ON l.sk_localidade = f.sk_localidade
JOIN dim_produtor p   ON p.sk_localidade = f.sk_localidade
WHERE f.tipo_registro = 'previsao'
  AND t.data BETWEEN current_date() AND current_date() + 7
  AND f.nivel_risco <> 'baixo';

-- COMMAND ----------

SELECT 'dim_localidade' AS tabela, count(*) AS linhas FROM dim_localidade
UNION ALL SELECT 'dim_tempo', count(*) FROM dim_tempo
UNION ALL SELECT 'dim_produtor', count(*) FROM dim_produtor
UNION ALL SELECT 'fato_clima', count(*) FROM fato_clima;

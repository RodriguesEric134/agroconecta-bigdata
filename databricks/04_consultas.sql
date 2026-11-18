-- Databricks notebook source
-- MAGIC %md
-- MAGIC # AgroConecta — 04 Consultas analíticas (camada Ouro)
-- MAGIC As 5 consultas respondem perguntas reais do produtor e alimentam telas do MVP.

-- COMMAND ----------

USE CATALOG workspace;
USE SCHEMA agroconecta_ouro;

-- COMMAND ----------

-- MAGIC %md ### Consulta 1 — Previsão dos próximos 7 dias por município, com nível de risco
-- MAGIC *Tela "Monitoramento de Campo": o que esperar do tempo nesta semana?*

-- COMMAND ----------

SELECT l.municipio, l.uf, t.data, t.dia_semana,
       f.temp_min_c, f.temp_max_c, f.precipitacao_mm, f.prob_chuva_pct, f.umidade_media_pct,
       f.nivel_risco
FROM fato_clima f
JOIN dim_tempo t      ON t.sk_tempo = f.sk_tempo
JOIN dim_localidade l ON l.sk_localidade = f.sk_localidade
WHERE f.tipo_registro = 'previsao'
  AND t.data BETWEEN current_date() AND current_date() + 7
ORDER BY l.municipio, t.data;

-- COMMAND ----------

-- MAGIC %md ### Consulta 2 — Produtores afetados por alertas nos próximos 3 dias, por cultura e município
-- MAGIC *Base para notificações push/WhatsApp (somente pseudônimos — LGPD).*

-- COMMAND ----------

SELECT l.municipio, l.uf, p.cultura_principal,
       count(DISTINCT p.id_produtor)                                         AS produtores_afetados,
       -- count(DISTINCT data): o JOIN com dim_produtor repete cada dia uma vez por produtor
       count(DISTINCT CASE WHEN f.alerta_geada THEN t.data END)              AS dias_geada,
       count(DISTINCT CASE WHEN f.alerta_chuva_forte THEN t.data END)        AS dias_chuva_forte,
       count(DISTINCT CASE WHEN f.alerta_calor THEN t.data END)              AS dias_calor,
       count(DISTINCT CASE WHEN f.alerta_estresse_hidrico THEN t.data END)   AS dias_estresse_hidrico,
       CASE WHEN max(CASE f.nivel_risco WHEN 'alto' THEN 2 WHEN 'moderado' THEN 1 ELSE 0 END) = 2
            THEN 'alto' ELSE 'moderado' END                                  AS pior_risco
FROM fato_clima f
JOIN dim_tempo t      ON t.sk_tempo = f.sk_tempo
JOIN dim_localidade l ON l.sk_localidade = f.sk_localidade
JOIN dim_produtor p   ON p.sk_localidade = f.sk_localidade
WHERE t.data BETWEEN current_date() AND current_date() + 3
  AND f.nivel_risco <> 'baixo'
GROUP BY l.municipio, l.uf, p.cultura_principal
ORDER BY produtores_afetados DESC;

-- COMMAND ----------

-- MAGIC %md ### Consulta 3 — Balanço hídrico semanal (chuva − evapotranspiração)
-- MAGIC *Indica quando irrigar: saldo negativo = déficit hídrico na semana.*

-- COMMAND ----------

SELECT l.municipio, l.uf, t.ano, t.semana_ano,
       round(sum(f.precipitacao_mm), 1)       AS chuva_mm,
       round(sum(f.evapotranspiracao_mm), 1)  AS evapotranspiracao_mm,
       round(sum(f.balanco_hidrico_mm), 1)    AS balanco_mm,
       CASE WHEN sum(f.balanco_hidrico_mm) < -20 THEN 'déficit severo — irrigar'
            WHEN sum(f.balanco_hidrico_mm) < 0   THEN 'déficit leve — monitorar'
            ELSE 'sem déficit' END            AS recomendacao
FROM fato_clima f
JOIN dim_tempo t      ON t.sk_tempo = f.sk_tempo
JOIN dim_localidade l ON l.sk_localidade = f.sk_localidade
GROUP BY l.municipio, l.uf, t.ano, t.semana_ano
ORDER BY t.ano DESC, t.semana_ano DESC, balanco_mm;

-- COMMAND ----------

-- MAGIC %md ### Consulta 4 — Média móvel de 7 dias da temperatura e chuva acumulada (window functions)
-- MAGIC *Tendência térmica por município — usada no gráfico do dashboard.*

-- COMMAND ----------

SELECT l.municipio, t.data, f.tipo_registro, f.temp_media_c,
       round(avg(f.temp_media_c) OVER (PARTITION BY f.sk_localidade ORDER BY t.data
                                       ROWS BETWEEN 6 PRECEDING AND CURRENT ROW), 1) AS media_movel_7d_c,
       round(sum(f.precipitacao_mm) OVER (PARTITION BY f.sk_localidade ORDER BY t.data
                                          ROWS BETWEEN 6 PRECEDING AND CURRENT ROW), 1) AS chuva_acum_7d_mm
FROM fato_clima f
JOIN dim_tempo t      ON t.sk_tempo = f.sk_tempo
JOIN dim_localidade l ON l.sk_localidade = f.sk_localidade
ORDER BY l.municipio, t.data;

-- COMMAND ----------

-- MAGIC %md ### Consulta 5 — Ranking de risco climático por região e estação do ano
-- MAGIC *Visão gerencial: quais regiões concentram mais dias de risco?*

-- COMMAND ----------

SELECT l.regiao, l.uf, t.estacao_ano, t.ano_safra,
       count(*)                                                    AS dias_analisados,
       sum(CASE WHEN f.nivel_risco = 'alto' THEN 1 ELSE 0 END)     AS dias_risco_alto,
       sum(CASE WHEN f.nivel_risco = 'moderado' THEN 1 ELSE 0 END) AS dias_risco_moderado,
       round(100 * avg(CASE WHEN f.nivel_risco <> 'baixo' THEN 1 ELSE 0 END), 1) AS pct_dias_com_risco,
       dense_rank() OVER (ORDER BY avg(CASE WHEN f.nivel_risco <> 'baixo' THEN 1 ELSE 0 END) DESC) AS ranking
FROM fato_clima f
JOIN dim_tempo t      ON t.sk_tempo = f.sk_tempo
JOIN dim_localidade l ON l.sk_localidade = f.sk_localidade
GROUP BY l.regiao, l.uf, t.estacao_ano, t.ano_safra
ORDER BY ranking;

-- COMMAND ----------

-- MAGIC %md ### Extra — Monitor de qualidade de dados (quarentena da Prata)

-- COMMAND ----------

SELECT entidade, motivo, count(*) AS registros, max(_quarentenado_em) AS ultima_ocorrencia
FROM agroconecta_prata.quarentena
GROUP BY entidade, motivo
ORDER BY registros DESC;

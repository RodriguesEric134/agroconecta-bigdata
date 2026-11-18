# Databricks notebook source
# MAGIC %md
# MAGIC # AgroConecta — 02 Camada Prata
# MAGIC Lê **apenas os registros novos** da Bronze (Structured Streaming + `availableNow`) e, para cada lote:
# MAGIC 1. Converte o JSON bruto em colunas tipadas (schema explícito);
# MAGIC 2. Padroniza datas, nomes de municípios e unidades de medida;
# MAGIC 3. Aplica **regras de qualidade**: registros reprovados vão para `quarentena` com o motivo;
# MAGIC 4. Remove duplicados (fica a coleta mais recente);
# MAGIC 5. Atualiza a tabela Prata com **MERGE** (upsert).
# MAGIC
# MAGIC **LGPD:** o CPF não sai da Bronze. Na Prata o produtor é identificado por um hash SHA-256 com sal secreto
# MAGIC (pseudonimização).

# COMMAND ----------

dbutils.widgets.text("catalogo", "workspace")
dbutils.widgets.text("data_referencia", "")
CATALOGO = dbutils.widgets.get("catalogo")

BRONZE = f"{CATALOGO}.agroconecta_bronze"
PRATA = f"{CATALOGO}.agroconecta_prata"
VOLUME_CONTROLE = f"/Volumes/{CATALOGO}/agroconecta_bronze/controle"

try:
    LGPD_SALT = dbutils.secrets.get("agroconecta", "lgpd_salt")
except Exception:  # ambiente acadêmico sem secret scope configurado
    LGPD_SALT = "agroconecta-sal-dev"
    print("AVISO: secret scope 'agroconecta' não encontrado — usando sal de desenvolvimento.")

# COMMAND ----------

from delta.tables import DeltaTable
from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F
from pyspark.sql import types as T

TZ = "America/Sao_Paulo"


def motivos(*regras):
    """Recebe pares (condição_de_erro, mensagem) e devolve um array só com os erros encontrados."""
    return F.filter(F.array(*[F.when(cond, F.lit(msg)) for cond, msg in regras]), lambda x: x.isNotNull())


def ultima_versao(df: DataFrame, chaves: list[str], ordem: str = "_coletado_em") -> DataFrame:
    """Deduplicação: mantém o registro mais recente por chave.
    Empate (mesmo arquivo) -> vence a última linha do arquivo."""
    w = Window.partitionBy(*chaves).orderBy(F.col(ordem).desc(), F.col("_ordem").desc())
    return (df.withColumn("_ordem", F.monotonically_increasing_id())
              .withColumn("_rn", F.row_number().over(w))
              .filter("_rn = 1").drop("_rn", "_ordem"))


def gravar_quarentena(df: DataFrame, entidade: str):
    """Grava reprovados com MERGE (idempotente): se um lote for reprocessado após uma falha,
    o mesmo registro não é duplicado na quarentena."""
    tabela = f"{PRATA}.quarentena"
    novos = (df.select(F.lit(entidade).alias("entidade"),
                       F.to_json(F.struct(*[c for c in df.columns if c != "motivos"])).alias("registro"),
                       F.array_join("motivos", "; ").alias("motivo"),
                       F.current_timestamp().alias("_quarentenado_em"))
               .withColumn("id_quarentena", F.sha2(F.concat_ws("|", "entidade", "registro"), 256))
               .dropDuplicates(["id_quarentena"]))
    if not spark.catalog.tableExists(tabela):
        novos.limit(0).write.saveAsTable(tabela)
    elif "id_quarentena" not in spark.table(tabela).columns:   # tabela criada pela versão anterior
        spark.sql(f"ALTER TABLE {tabela} ADD COLUMN id_quarentena STRING")
        spark.sql(f"UPDATE {tabela} SET id_quarentena = sha2(concat_ws('|', entidade, registro), 256)")
    (DeltaTable.forName(spark, tabela).alias("t")
        .merge(novos.alias("s"), "t.id_quarentena = s.id_quarentena")
        .whenNotMatchedInsertAll()
        .execute())


def merge(df: DataFrame, tabela: str, chaves: list[str]):
    """Upsert na Prata. Só atualiza se o registro novo for mais recente."""
    if not spark.catalog.tableExists(tabela):
        df.limit(0).write.saveAsTable(tabela)
    cond = " AND ".join(f"t.{c} = s.{c}" for c in chaves)
    (DeltaTable.forName(spark, tabela).alias("t")
        .merge(df.alias("s"), cond)
        .whenMatchedUpdateAll(condition="s._coletado_em >= t._coletado_em")
        .whenNotMatchedInsertAll()
        .execute())


def processar_incremental(tabela_bronze: str, nome: str, funcao_lote):
    """Lê só o que é novo na Bronze e processa com foreachBatch (padrão incremental)."""
    q = (spark.readStream.table(f"{BRONZE}.{tabela_bronze}")
         .writeStream
         .foreachBatch(funcao_lote)
         .option("checkpointLocation", f"{VOLUME_CONTROLE}/prata_{nome}")
         .trigger(availableNow=True)
         .start())
    q.awaitTermination()

# COMMAND ----------

# MAGIC %md ## 1. Municípios (IBGE)

# COMMAND ----------

SCHEMA_IBGE = T.StructType([
    T.StructField("localidade", T.StructType([
        T.StructField("id", T.LongType()),
        T.StructField("nome", T.StringType()),
        T.StructField("microrregiao", T.StructType([
            T.StructField("nome", T.StringType()),
            T.StructField("mesorregiao", T.StructType([
                T.StructField("nome", T.StringType()),
                T.StructField("UF", T.StructType([
                    T.StructField("sigla", T.StringType()),
                    T.StructField("nome", T.StringType()),
                    T.StructField("regiao", T.StructType([
                        T.StructField("sigla", T.StringType()),
                        T.StructField("nome", T.StringType())])),
                ])),
            ])),
        ])),
        T.StructField("regiao-imediata", T.StructType([T.StructField("nome", T.StringType())])),
    ])),
    T.StructField("malha_metadados", T.StructType([
        T.StructField("centroide", T.StructType([
            T.StructField("latitude", T.DoubleType()),
            T.StructField("longitude", T.DoubleType())])),
        T.StructField("area", T.StructType([
            T.StructField("dimensao", T.StringType())])),
    ])),
])


def lote_municipios(df_lote: DataFrame, batch_id: int):
    p = F.from_json("payload", SCHEMA_IBGE)
    loc = p["localidade"]
    meso = loc["microrregiao"]["mesorregiao"]
    df = (df_lote.select(
            loc["id"].cast("int").alias("codigo_ibge"),
            F.initcap(F.trim(loc["nome"])).alias("municipio"),
            # nome sem acento e em maiúsculas: chave de junção com outras fontes (ex.: CEASA)
            F.upper(F.translate(F.trim(loc["nome"]), "áàâãäéèêëíìîïóòôõöúùûüçÁÀÂÃÄÉÈÊËÍÌÎÏÓÒÔÕÖÚÙÛÜÇ",
                                "aaaaaeeeeiiiiooooouuuucAAAAAEEEEIIIIOOOOOUUUUC")).alias("municipio_normalizado"),
            F.upper(meso["UF"]["sigla"]).alias("uf"),
            meso["UF"]["nome"].alias("estado"),
            meso["UF"]["regiao"]["nome"].alias("regiao"),
            meso["nome"].alias("mesorregiao"),
            loc["microrregiao"]["nome"].alias("microrregiao"),
            loc["regiao-imediata"]["nome"].alias("regiao_imediata"),
            F.round(p["malha_metadados"]["centroide"]["latitude"], 4).alias("latitude"),
            F.round(p["malha_metadados"]["centroide"]["longitude"], 4).alias("longitude"),
            F.when(p["malha_metadados"]["area"]["dimensao"].rlike(r"^-?\d+(\.\d+)?$"),
                   p["malha_metadados"]["area"]["dimensao"].cast("double")).alias("area_km2"),
            "_coletado_em", "_arquivo_origem")
          .withColumn("motivos", motivos(
            (F.col("codigo_ibge").isNull(), "codigo_ibge nulo"),
            (F.length(F.col("codigo_ibge").cast("string")) != 7, "codigo_ibge deve ter 7 dígitos"),
            (F.col("municipio").isNull(), "nome do município nulo"),
            (~F.col("latitude").between(-34, 6) | ~F.col("longitude").between(-74, -34),
             "coordenada fora do território brasileiro"),
          )))
    gravar_quarentena(df.filter(F.size("motivos") > 0), "municipio")
    validos = ultima_versao(df.filter(F.size("motivos") == 0).drop("motivos"), ["codigo_ibge"])
    merge(validos, f"{PRATA}.municipios", ["codigo_ibge"])


processar_incremental("ibge_municipios", "municipios", lote_municipios)

# COMMAND ----------

# MAGIC %md ## 2. Clima diário (Open-Meteo)
# MAGIC A API devolve arrays paralelos (`daily.time[i]`, `daily.temperature_2m_max[i]`...). Cada posição vira uma linha
# MAGIC (município × dia). Unidades são conferidas em `daily_units` e convertidas para o padrão do projeto
# MAGIC (°C, mm, %, m/s).

# COMMAND ----------

VARIAVEIS = {
    # variável Open-Meteo : coluna Prata
    "temperature_2m_max": "temp_max_c",
    "temperature_2m_min": "temp_min_c",
    "temperature_2m_mean": "temp_media_c",
    "precipitation_sum": "precipitacao_mm",
    "precipitation_probability_max": "prob_chuva_pct",
    "relative_humidity_2m_mean": "umidade_media_pct",
    "relative_humidity_2m_max": "umidade_max_pct",
    "relative_humidity_2m_min": "umidade_min_pct",
    "et0_fao_evapotranspiration": "evapotranspiracao_mm",
    "wind_speed_10m_max": "vento_max_ms",
}

SCHEMA_OPEN_METEO = T.StructType([
    T.StructField("latitude", T.DoubleType()),
    T.StructField("longitude", T.DoubleType()),
    T.StructField("elevation", T.DoubleType()),
    T.StructField("daily_units", T.MapType(T.StringType(), T.StringType())),
    T.StructField("daily", T.StructType(
        [T.StructField("time", T.ArrayType(T.StringType()))] +
        [T.StructField(v, T.ArrayType(T.DoubleType())) for v in VARIAVEIS])),
])


def padronizar_temperatura(col, unidade):
    return F.when(unidade == "°F", (col - 32) * 5 / 9).otherwise(col)


def padronizar_precipitacao(col, unidade):
    return F.when(unidade == "inch", col * 25.4).otherwise(col)


def padronizar_vento(col, unidade):
    return (F.when(unidade == "km/h", col / 3.6)
             .when(unidade == "mp/h", col * 0.44704)
             .when(unidade == "kn", col * 0.514444)
             .otherwise(col))


def lote_clima(df_lote: DataFrame, batch_id: int):
    p = F.from_json("payload", SCHEMA_OPEN_METEO)
    base = (df_lote
            .select(F.expr("try_cast(codigo_ibge AS INT)").alias("codigo_ibge"),
                    "_coletado_em", "_arquivo_origem", p.alias("p"))
            .select("*", F.posexplode("p.daily.time").alias("i", "data_str")))

    u = lambda v: F.col("p.daily_units")[v]
    v = lambda nome: F.col(f"p.daily.{nome}")[F.col("i")]

    df = base.select(
        "codigo_ibge",
        F.expr("try_to_date(data_str)").alias("data"),
        F.round(padronizar_temperatura(v("temperature_2m_max"), u("temperature_2m_max")), 1).alias("temp_max_c"),
        F.round(padronizar_temperatura(v("temperature_2m_min"), u("temperature_2m_min")), 1).alias("temp_min_c"),
        F.round(padronizar_temperatura(v("temperature_2m_mean"), u("temperature_2m_mean")), 1).alias("temp_media_c"),
        F.round(padronizar_precipitacao(v("precipitation_sum"), u("precipitation_sum")), 1).alias("precipitacao_mm"),
        v("precipitation_probability_max").alias("prob_chuva_pct"),
        v("relative_humidity_2m_mean").alias("umidade_media_pct"),
        v("relative_humidity_2m_max").alias("umidade_max_pct"),
        v("relative_humidity_2m_min").alias("umidade_min_pct"),
        F.round(padronizar_precipitacao(v("et0_fao_evapotranspiration"), u("et0_fao_evapotranspiration")), 2)
            .alias("evapotranspiracao_mm"),
        F.round(padronizar_vento(v("wind_speed_10m_max"), u("wind_speed_10m_max")), 1).alias("vento_max_ms"),
        F.col("p.elevation").alias("altitude_m"),
        "_coletado_em", "_arquivo_origem",
    )
    # Observado x previsão: dias anteriores à data da coleta (no fuso de Brasília) são "observados"
    df = df.withColumn("tipo_registro", F.when(
        F.col("data") < F.to_date(F.from_utc_timestamp("_coletado_em", TZ)), "observado").otherwise("previsao"))

    df = df.withColumn("motivos", motivos(
        (F.col("codigo_ibge").isNull(), "codigo_ibge nulo"),
        (F.col("data").isNull(), "data inválida"),
        (F.col("temp_max_c").isNull() | F.col("temp_min_c").isNull(), "temperatura nula"),
        (F.col("precipitacao_mm").isNull(), "precipitação nula"),
        (~F.col("temp_max_c").between(-15, 50) | ~F.col("temp_min_c").between(-15, 50), "temperatura fora da faixa física"),
        (F.col("temp_min_c") > F.col("temp_max_c"), "temp mínima maior que máxima"),
        (F.col("precipitacao_mm") < 0, "precipitação negativa"),
        (~F.col("umidade_media_pct").between(0, 100), "umidade fora de 0-100%"),
    ))
    gravar_quarentena(df.filter(F.size("motivos") > 0), "clima_diario")
    validos = ultima_versao(df.filter(F.size("motivos") == 0).drop("motivos"), ["codigo_ibge", "data"])
    merge(validos, f"{PRATA}.clima_diario", ["codigo_ibge", "data"])


processar_incremental("open_meteo_clima", "clima_diario", lote_clima)

# COMMAND ----------

# MAGIC %md ## 3. Produtores (cadastro do MVP) — pseudonimização LGPD

# COMMAND ----------

SCHEMA_PRODUTOR = T.StructType([T.StructField(c, T.StringType()) for c in [
    "id_produtor_mvp", "nome", "cpf", "telefone", "email", "codigo_ibge",
    "cultura_principal", "area_ha", "data_cadastro"]])


def cpf_valido(coluna: str):
    """Valida os dígitos verificadores do CPF com expressões nativas do Spark (sem UDF Python).
    DV = (Σ dígito × peso) × 10 mod 11 mod 10; pesos 10..2 para o 1º DV e 11..2 para o 2º."""
    cpf = F.col(coluna)
    # try_cast: no modo ANSI (padrão do serverless) um cast inválido lança erro em vez de virar null
    d = lambda i: F.expr(f"try_cast(substring({coluna}, {i}, 1) AS INT)")
    soma1 = sum(d(i) * (11 - i) for i in range(1, 10))
    soma2 = sum(d(i) * (12 - i) for i in range(1, 11))
    valido = (cpf.rlike(r"^\d{11}$")
              & ~cpf.rlike(r"^(\d)\1{10}$")                 # 000.000.000-00, 111... etc.
              & ((soma1 * 10) % 11 % 10 == d(10))
              & ((soma2 * 10) % 11 % 10 == d(11)))
    return F.coalesce(valido, F.lit(False))                 # CPF nulo = inválido


def lote_produtores(df_lote: DataFrame, batch_id: int):
    p = F.from_json("payload", SCHEMA_PRODUTOR)
    df = (df_lote.select(p.alias("p"), "_coletado_em", "_arquivo_origem")
          .select(
            # try_cast/try_to_date: valores inválidos viram null e caem nas regras de qualidade
            F.expr("try_cast(p.id_produtor_mvp AS INT)").alias("id_produtor_mvp"),
            F.regexp_replace("p.cpf", r"\D", "").alias("_cpf"),
            F.initcap(F.regexp_replace(F.trim("p.nome"), r"\s+", " ")).alias("nome"),
            F.regexp_replace("p.telefone", r"\D", "").alias("telefone"),
            F.lower(F.trim("p.email")).alias("email"),
            F.expr("try_cast(p.codigo_ibge AS INT)").alias("codigo_ibge"),
            F.lower(F.trim("p.cultura_principal")).alias("cultura_principal"),
            F.expr("try_cast(p.area_ha AS DOUBLE)").alias("area_ha"),
            F.expr("try_to_date(p.data_cadastro)").alias("data_cadastro"),
            "_coletado_em", "_arquivo_origem")
          .withColumn("motivos", motivos(
            (~cpf_valido("_cpf"), "CPF inválido"),
            (F.col("codigo_ibge").isNull(), "município não informado"),
            (F.col("nome").isNull(), "nome nulo"),
            (F.col("area_ha") <= 0, "área deve ser positiva"),
          ))
          # Pseudonimização: hash irreversível (SHA-256 + sal secreto). O CPF é descartado.
          .withColumn("id_produtor", F.sha2(F.concat(F.col("_cpf"), F.lit(LGPD_SALT)), 256))
          .drop("_cpf"))

    gravar_quarentena(df.filter(F.size("motivos") > 0), "produtor")
    validos = ultima_versao(df.filter(F.size("motivos") == 0).drop("motivos"), ["id_produtor"])
    merge(validos, f"{PRATA}.produtores", ["id_produtor"])


processar_incremental("mvp_produtores", "produtores", lote_produtores)

# COMMAND ----------

# MAGIC %md ## Resumo de qualidade

# COMMAND ----------

display(spark.sql(f"""
  SELECT 'municipios' AS tabela, count(*) AS registros FROM {PRATA}.municipios
  UNION ALL SELECT 'clima_diario', count(*) FROM {PRATA}.clima_diario
  UNION ALL SELECT 'produtores', count(*) FROM {PRATA}.produtores
  UNION ALL SELECT concat('quarentena: ', entidade), count(*) FROM {PRATA}.quarentena GROUP BY entidade
"""))

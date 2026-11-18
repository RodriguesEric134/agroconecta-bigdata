# Databricks notebook source
# MAGIC %md
# MAGIC # AgroConecta — 01 Camada Bronze
# MAGIC Ingestão **incremental** dos arquivos JSONL do Volume `chegada` com **Auto Loader**
# MAGIC (`cloudFiles`). Cada arquivo é processado uma única vez — o checkpoint registra o que já foi lido.
# MAGIC
# MAGIC Na Bronze os dados ficam **como chegaram**: o conteúdo original da API é guardado em `payload`
# MAGIC (texto JSON bruto) junto com metadados de linhagem (fonte, data de coleta, arquivo de origem).

# COMMAND ----------

dbutils.widgets.text("catalogo", "workspace")
dbutils.widgets.text("data_referencia", "")
CATALOGO = dbutils.widgets.get("catalogo")

BRONZE = f"{CATALOGO}.agroconecta_bronze"
VOLUME_CHEGADA = f"/Volumes/{CATALOGO}/agroconecta_bronze/chegada"
VOLUME_CONTROLE = f"/Volumes/{CATALOGO}/agroconecta_bronze/controle"

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType

# Schema explícito: payload é lido como STRING => o JSON original é preservado byte a byte.
SCHEMA_CHEGADA = StructType([
    StructField("_fonte", StringType()),
    StructField("_coletado_em", StringType()),
    StructField("_url", StringType()),
    StructField("codigo_ibge", StringType()),
    StructField("data_referencia", StringType()),
    StructField("parametros", StringType()),
    StructField("payload", StringType()),
])

FONTES = {
    # fonte (pasta em chegada/) : tabela bronze
    "ibge": "ibge_municipios",
    "open_meteo": "open_meteo_clima",
    "mvp_produtores": "mvp_produtores",
}

# COMMAND ----------

def contar(tabela: str) -> int:
    return spark.table(tabela).count() if spark.catalog.tableExists(tabela) else 0


def ingerir_fonte(fonte: str, tabela: str) -> dict:
    origem = f"{VOLUME_CHEGADA}/{fonte}/"
    destino = f"{BRONZE}.{tabela}"
    checkpoint = f"{VOLUME_CONTROLE}/bronze_{tabela}"
    antes = contar(destino)

    df = (spark.readStream.format("cloudFiles")
          .option("cloudFiles.format", "json")
          .option("cloudFiles.schemaLocation", checkpoint)
          .option("recursiveFileLookup", "true")
          .schema(SCHEMA_CHEGADA)
          .load(origem)
          # try_*: no modo ANSI (serverless) conversão inválida lança erro; aqui vira null
          .withColumn("_coletado_em", F.expr("try_to_timestamp(_coletado_em)"))
          .withColumn("_arquivo_origem", F.col("_metadata.file_path"))
          .withColumn("_dt_str", F.regexp_extract("_metadata.file_path", r"dt=(\d{4}-\d{2}-\d{2})", 1))
          .withColumn("_dt_particao", F.expr("try_to_date(_dt_str)"))
          .drop("_dt_str")
          .withColumn("_ingerido_em", F.current_timestamp()))

    consulta = (df.writeStream
                .option("checkpointLocation", checkpoint)
                .option("mergeSchema", "true")
                .trigger(availableNow=True)   # processa só o que é novo e encerra
                .toTable(destino))
    consulta.awaitTermination()
    # Contagem antes/depois (lastProgress não é confiável no serverless / Spark Connect)
    depois = contar(destino)
    return (fonte, destino, depois - antes, depois)

# COMMAND ----------

resultado = [ingerir_fonte(f, t) for f, t in FONTES.items()]
display(spark.createDataFrame(resultado, "fonte STRING, tabela STRING, novas_linhas LONG, total_linhas LONG"))

# COMMAND ----------

for tabela in FONTES.values():
    spark.sql(f"COMMENT ON TABLE {BRONZE}.{tabela} IS 'Bronze: dado bruto da fonte, ingestão incremental via Auto Loader'")

display(spark.sql(f"""
  SELECT _fonte, _dt_particao, count(*) AS registros, max(_ingerido_em) AS ultima_ingestao
  FROM (
    SELECT _fonte, _dt_particao, _ingerido_em FROM {BRONZE}.ibge_municipios
    UNION ALL SELECT _fonte, _dt_particao, _ingerido_em FROM {BRONZE}.open_meteo_clima
    UNION ALL SELECT _fonte, _dt_particao, _ingerido_em FROM {BRONZE}.mvp_produtores
  ) GROUP BY ALL ORDER BY _dt_particao DESC, _fonte
"""))

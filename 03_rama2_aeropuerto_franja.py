# ============================================================
# Trabajo 2 - Parte A - RAMA 2
# Pregunta: ¿En que aeropuertos de origen y franjas horarias
# (DepTimeBlk) se concentra el riesgo de retraso?
# ============================================================
from pyspark.sql import SparkSession, functions as F

BUCKET = "PON-AQUI-EL-NOMBRE-DE-TU-BUCKET"   # <-- CAMBIAR SOLO ESTO

spark = SparkSession.builder.appName("t2-rama2").getOrCreate()
df = spark.read.parquet(f"s3://{BUCKET}/transformed/flights/")

operados = df.filter((F.col("Cancelled") == 0) & (F.col("Diverted") == 0))

agg = (operados.groupBy("Origin", "DepTimeBlk")
    .agg(F.count("*").alias("vuelos"),
         F.sum("DepDel15").alias("vuelos_retrasados"),
         F.round(F.avg("DepDel15") * 100, 2).alias("pct_retraso"))
    .filter(F.col("vuelos") >= 1000))   # solo combinaciones con volumen suficiente

agg.write.mode("overwrite").parquet(f"s3://{BUCKET}/gold/retraso_aeropuerto_franja/")

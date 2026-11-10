# ============================================================
# Trabajo 2 - Parte A - RAMA 1
# Pregunta: ¿Que aerolineas tienen mayor % de vuelos con retraso
# de salida >= 15 min, y como cambia mes a mes?
# ============================================================
from pyspark.sql import SparkSession, functions as F

BUCKET = "daniel-trabajo2-vuelos-2026"   # <-- CAMBIAR SOLO ESTO

spark = SparkSession.builder.appName("t2-rama1").getOrCreate()
df = spark.read.parquet(f"s3://{BUCKET}/transformed/flights/")

# Solo vuelos que si operaron (no cancelados ni desviados)
operados = df.filter((F.col("Cancelled") == 0) & (F.col("Diverted") == 0))

agg = (operados.groupBy("Reporting_Airline", "airline_name", "Month")
    .agg(F.count("*").alias("vuelos"),
         F.sum("DepDel15").alias("vuelos_retrasados"),
         F.round(F.avg("DepDel15") * 100, 2).alias("pct_retraso"),
         F.round(F.avg("DepDelay"), 2).alias("retraso_promedio_min")))

agg.write.mode("overwrite").parquet(f"s3://{BUCKET}/gold/retraso_aerolinea_mes/")

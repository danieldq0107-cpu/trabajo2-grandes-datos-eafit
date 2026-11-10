# ============================================================
# Trabajo 2 - Parte A - Job de Glue INICIAL
# Lee vuelos BTS 2025 (CSV) desde S3 + un archivo con ERRORES INDUCIDOS
# (duplicados, campos vacios, duraciones negativas), limpia, guarda los
# rechazados con su MOTIVO, une con aerolineas y escribe Parquet por Month.
# ============================================================
from pyspark.sql import SparkSession, functions as F, Window

BUCKET = "daniel-trabajo2-vuelos-2026"

spark = SparkSession.builder.appName("t2-limpieza-join").getOrCreate()

# 1) LEER los CSV crudos (zona raw = bronce)
vuelos = spark.read.option("header", "true").csv(f"s3://{BUCKET}/raw/flights/")
aerolineas = spark.read.option("header", "true").csv(f"s3://{BUCKET}/raw/airlines/")

# 2) TIPOS: convertir texto a numeros y fechas
vuelos = (vuelos
    .withColumn("FlightDate", F.to_date("FlightDate"))
    .withColumn("Year", F.col("Year").cast("int"))
    .withColumn("Month", F.col("Month").cast("int"))
    .withColumn("DayofMonth", F.col("DayofMonth").cast("int"))
    .withColumn("DayOfWeek", F.col("DayOfWeek").cast("int"))
    .withColumn("CRSDepTime", F.col("CRSDepTime").cast("int"))
    .withColumn("CRSElapsedTime", F.col("CRSElapsedTime").cast("double"))
    .withColumn("Distance", F.col("Distance").cast("double"))
    .withColumn("Cancelled", F.col("Cancelled").cast("double").cast("int"))
    .withColumn("Diverted", F.col("Diverted").cast("double").cast("int"))
    .withColumn("DepDelay", F.col("DepDelay").cast("double"))
    .withColumn("DepDel15", F.col("DepDel15").cast("double").cast("int")))

# Corregir: codigos en mayuscula y sin espacios
for c in ["Reporting_Airline", "Origin", "Dest"]:
    vuelos = vuelos.withColumn(c, F.upper(F.trim(F.col(c))))

# 3) REGLAS DE CALIDAD -> columna "motivo" (null = registro valido)
claves = ["FlightDate", "Reporting_Airline", "Flight_Number_Reporting_Airline", "Origin", "Dest"]
falta_clave = None
for c in claves:
    cond = F.col(c).isNull() | (F.col(c) == "")  if c != "FlightDate" else F.col(c).isNull()
    falta_clave = cond if falta_clave is None else (falta_clave | cond)

vuelos = vuelos.withColumn("motivo",
    F.when(falta_clave, F.lit("campo clave vacio"))
     .when(F.col("CRSElapsedTime").isNull(), F.lit("duracion programada vacia"))
     .when(F.col("CRSElapsedTime") <= 0, F.lit("duracion programada <= 0")))

# Regla de DUPLICADOS: mismo vuelo, mismo dia, misma ruta y hora -> se queda solo el primero
w = Window.partitionBy(*(claves + ["CRSDepTime"])).orderBy(F.lit(1))
vuelos = vuelos.withColumn("n", F.when(F.col("motivo").isNull(), F.row_number().over(w)))
vuelos = vuelos.withColumn("motivo",
    F.when(F.col("motivo").isNull() & (F.col("n") > 1), F.lit("duplicado")).otherwise(F.col("motivo"))
).drop("n")
vuelos.cache()

# 4) RECHAZADOS con su motivo
rechazados = vuelos.filter(F.col("motivo").isNotNull())
rechazados.write.mode("overwrite").parquet(f"s3://{BUCKET}/rejected/flights/")

# 5) VALIDOS + JOIN con aerolineas por la columna comun Reporting_Airline
validos = vuelos.filter(F.col("motivo").isNull()).drop("motivo")
resultado = validos.join(F.broadcast(aerolineas), on="Reporting_Airline", how="left")

# 6) ESCRIBIR en Parquet particionado por Month (zona transformed = plata)
resultado.write.mode("overwrite").partitionBy("Month").parquet(f"s3://{BUCKET}/transformed/flights/")

# 7) CONTEOS (quedan en los logs de CloudWatch)
total = vuelos.count()
n_val = validos.count()
print(f"REGISTROS CRUDOS: {total}")
print(f"REGISTROS VALIDOS: {n_val}")
print(f"REGISTROS RECHAZADOS: {total - n_val}")
for r in rechazados.groupBy("motivo").count().collect():
    print(f"  RECHAZADOS POR '{r['motivo']}': {r['count']}")

-- PASO 1: crear base de datos
CREATE DATABASE IF NOT EXISTS trabajo2;

-- PASO 2: tabla de la rama 1 (cambia el bucket)
CREATE EXTERNAL TABLE trabajo2.retraso_aerolinea_mes (
  Reporting_Airline string, airline_name string, Month int,
  vuelos bigint, vuelos_retrasados bigint, pct_retraso double, retraso_promedio_min double)
STORED AS PARQUET
LOCATION 's3://daniel-trabajo2-vuelos-2026/gold/retraso_aerolinea_mes/';

-- PASO 3: tabla de la rama 2 (cambia el bucket)
CREATE EXTERNAL TABLE trabajo2.retraso_aeropuerto_franja (
  Origin string, DepTimeBlk string, vuelos bigint, vuelos_retrasados bigint, pct_retraso double)
STORED AS PARQUET
LOCATION 's3://daniel-trabajo2-vuelos-2026/gold/retraso_aeropuerto_franja/';

-- PREGUNTA 1: ¿que aerolineas tienen mayor % de retraso en 2025?
SELECT airline_name,
       SUM(vuelos) AS vuelos,
       ROUND(100.0 * SUM(vuelos_retrasados) / SUM(vuelos), 2) AS pct_retraso
FROM trabajo2.retraso_aerolinea_mes
GROUP BY airline_name
ORDER BY pct_retraso DESC;

-- PREGUNTA 2: ¿que aeropuerto + franja horaria tiene mas riesgo?
SELECT Origin, DepTimeBlk, vuelos, pct_retraso
FROM trabajo2.retraso_aeropuerto_franja
ORDER BY pct_retraso DESC
LIMIT 15;

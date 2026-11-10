# Trabajo 2 – Pipeline batch y streaming en AWS

**Curso:** SI7006/SI6003 Almacenamiento y Procesamiento de Grandes Datos, EAFIT 2026-2
**Profesor:** Edwin Montoya
**Estudiante:** Victor Daniel Delgado
**Proyecto integrador:** Predicción del riesgo de retraso en vuelos comerciales de EE. UU. (BTS)
**Región AWS:** us-east-1 (AWS Academy Learner Lab)

---

## 1. Resumen

| Parte | Servicios | Estado |
|---|---|---|
| A – Batch | S3, AWS Glue (PySpark), Step Functions, EventBridge Scheduler, Athena | **Ejecutada con éxito en la nube** (9 nov 2026, 18:38) |
| B – Streaming | Kinesis Data Streams, Lambda, DynamoDB, Firehose → S3 | Código completo en `parteB/`, con los pasos de despliegue documentados |

Todos los recursos se crearon **manualmente en la consola de AWS**, sin CloudFormation, Terraform ni Serverless Framework.

---

## 2. Parte A – Pipeline batch

### 2.1 Arquitectura

```
EventBridge Scheduler (t2-horario, 18:38 America/Bogota)
        │ StartExecution
        ▼
Step Functions (t2-pipeline-vuelos)
        │ glue:startJobRun.sync
        ▼
Glue t2-limpieza-join   raw/ (CSV)  →  limpieza + join  →  transformed/ (Parquet, partitionBy Month)
        │ solo si termina con éxito
        ▼
   Parallel ──────────────┬──────────────────────────┐
                          ▼                          ▼
     Glue t2-rama1-aerolinea-mes      Glue t2-rama2-aeropuerto-franja
     gold/retraso_aerolinea_mes/      gold/retraso_aeropuerto_franja/
                          │                          │
                          └────────► Athena (base trabajo2) ◄┘
```

### 2.2 Datos de partida

- **Dataset 1:** BTS *Reporting Carrier On-Time Performance*, año 2025: **7.001.619 vuelos** (más de los 5.000.000 que se piden), en 12 CSV comprimidos (uno por mes), con 18 columnas. Fuente: https://www.transtats.bts.gov
- **Errores inducidos:** el archivo `flights_errores_inducidos.csv.gz` tiene 100.000 registros tomados de vuelos reales y dañados a propósito: 50.000 duplicados exactos, 15.000 sin `Origin`, 10.000 sin `Reporting_Airline`, 15.000 con `CRSElapsedTime` negativo y 10.000 con `CRSElapsedTime` vacío.
- **Dataset 2 (join):** `airlines.csv`, 14 aerolíneas (`Reporting_Airline` → `airline_name`, `airline_type`).
- **Columna común del join:** `Reporting_Airline`.

**¿Por qué 12 archivos?** Un `.gz` no se puede dividir por dentro, así que con 12 archivos Spark reparte la lectura entre los workers en paralelo. Además, así es como el BTS publica los datos (un archivo por mes) y permite reprocesar un solo mes.

### 2.3 Estructura en S3 (`s3://daniel-trabajo2-vuelos-2026/`)

| Zona | Prefijo | Capa Medallion |
|---|---|---|
| Crudos | `raw/flights/`, `raw/airlines/` | Bronce |
| Transformados | `transformed/flights/Month=1..12/` | Plata |
| Rechazados con motivo | `rejected/flights/` | Plata (rechazados) |
| Agrupaciones | `gold/retraso_aerolinea_mes/`, `gold/retraso_aeropuerto_franja/` | Oro |
| Resultados de Athena | `athena-results/` | – |

### 2.4 Job inicial `t2-limpieza-join` (`glue/01_limpieza_join.py`)

1. Lee los CSV desde `raw/`.
2. Convierte los tipos de dato (fechas y números).
3. Corrige los códigos: los pasa a mayúsculas y les quita los espacios.
4. Aplica las **reglas de calidad**, que marcan una columna `motivo`:
   - `campo clave vacio`: falta `FlightDate`, `Reporting_Airline`, `Flight_Number_Reporting_Airline`, `Origin` o `Dest`.
   - `duracion programada vacia`: `CRSElapsedTime` es nulo.
   - `duracion programada <= 0`: duración imposible. Este defecto ya se había encontrado en el Trabajo 1.
   - `duplicado`: mismo vuelo, fecha, ruta y hora programada. Con `row_number` se conserva solo la primera aparición.
5. Escribe los **rechazados con su motivo** en `rejected/flights/`.
6. Hace un **join** (broadcast, left) de los válidos con `airlines` por `Reporting_Airline`.
7. Escribe en **Parquet particionado por `Month`** en `transformed/flights/`.
8. Imprime en CloudWatch los conteos: crudos, válidos, rechazados y rechazados por motivo.

**Justificación de la partición por `Month`:**
- Las preguntas del proyecto son temporales (estacionalidad y división cronológica entre entrenamiento y prueba del modelo), así que Athena y Spark solo leen los meses que se filtran (*partition pruning*).
- La cardinalidad es baja (12 particiones de unos 580 mil registros cada una), lo que evita el problema de los archivos pequeños que generaría particionar por `Tail_Number` o `Flight_Number`.

### 2.5 Ramas en paralelo

| Rama | Job | Pregunta de analítica | Agrupación |
|---|---|---|---|
| 1 | `t2-rama1-aerolinea-mes` | ¿Qué aerolíneas tienen mayor % de vuelos con retraso de salida ≥ 15 min, y cómo cambia mes a mes? | `groupBy(Reporting_Airline, airline_name, Month)` → vuelos, retrasados, % retraso, retraso promedio |
| 2 | `t2-rama2-aeropuerto-franja` | ¿En qué aeropuertos de origen y franjas horarias se concentra el riesgo de retraso? | `groupBy(Origin, DepTimeBlk)` → vuelos, retrasados, % retraso (solo combinaciones con ≥ 1.000 vuelos) |

Las dos ramas excluyen los vuelos cancelados y desviados, porque no tienen retraso de salida.

### 2.6 Step Functions y EventBridge Scheduler

- La máquina de estados (`stepfunctions/maquina_estados.json`) es de tipo **Standard**. Tiene un `Task` con `glue:startJobRun.sync`, que espera a que el job termine, seguido de un estado `Parallel` con las dos ramas.
- El horario `t2-horario` es una programación única a las **18:38 (America/Bogota)**. Su destino es `StartExecution` y usa el rol `LabRole`.
- **Ejecución iniciada por el horario:** `ca6af259-5856-4494-a4b7-55ff7e2b26d6`, que empezó a las 18:38:01 y terminó a las 18:43:02 (**5 min 01 s**) con estado **Ejecutada correctamente**.
- Configuración de los jobs: Glue 6.0 (Spark 4.1), G.1X, 2 workers, rol `LabRole`. El job inicial tardó unos 2 minutos.

### 2.7 Consultas en Athena (`athena/consultas.sql`)

**Pregunta 1: % de retraso por aerolínea, 2025**

| Aerolínea | Vuelos | % retraso ≥ 15 min |
|---|---:|---:|
| Frontier Airlines | 194.192 | 26,93 |
| PSA Airlines | 236.682 | 25,61 |
| JetBlue Airways | 226.703 | 25,15 |
| American Airlines | 952.841 | 25,07 |
| Southwest Airlines | 1.376.837 | 24,58 |
| … | … | … |
| Hawaiian Airlines | 79.335 | 15,21 |

**Lectura:** la aerolínea influye en el riesgo de retraso. Frontier tiene casi el doble de tasa que Hawaiian, así que es una variable relevante para el modelo. La consulta escaneó solo **1,92 KB**, porque la rama ya dejó el resultado agregado.

**Pregunta 2: aeropuerto y franja horaria con más riesgo**

| Origen | Franja | Vuelos | % retraso |
|---|---|---:|---:|
| BWI | 2200-2259 | 2.992 | 52,17 |
| MDW | 2200-2259 | 1.752 | 51,94 |
| DAL | 2200-2259 | 1.939 | 49,05 |
| BNA | 2200-2259 | 1.098 | 46,90 |
| AUS | 2100-2159 | 1.849 | 45,92 |

**Lectura:** las franjas de la noche (21:00 a 22:59) superan el 45 % de retraso. Es un efecto en cascada: un avión hace varios vuelos al día y los retrasos se acumulan, así que la última salida es la más afectada.

---

## 3. Parte B – Streaming (`parteB/`)

### 3.1 Arquitectura

```
productor.py (CloudShell, 6 min, 10 ev/s)
   │ put_records (PartitionKey = airline)
   ▼
Kinesis Data Streams  t2-vuelos-stream (1 shard)
   ├──► Lambda t2-ventanas (consumidor 1)
   │       ├─ repetido (event_id ya visto)   → se descarta, contador "repetidos"
   │       ├─ inválido                        → t2-eventos (estado=invalido, motivo)
   │       └─ válido y nuevo                  → ventana de 1 min por aerolínea → t2-ventanas
   └──► Amazon Data Firehose (consumidor 2) → s3://daniel-trabajo2-vuelos-2026/raw-stream/ (eventos crudos)
```

### 3.2 Evento

```json
{"event_id": "uuid", "event_time": "2026-11-09T23:30:12.345Z",
 "airline": "WN", "origin": "ATL", "dep_delay_min": 17.4}
```

- **Categoría:** `airline`.
- **Campo numérico:** `dep_delay_min`.
- **Repetido:** el productor reenvía el mismo evento (mismo `event_id`) con una probabilidad del 5 %.
- **Inválido** (probabilidad del 4 %): falta `airline`, falta `dep_delay_min`, `dep_delay_min` está fuera del rango [-60, 1500] o no es numérico.

### 3.3 Cómo se reconoce un repetido

La Lambda escribe cada evento en `t2-eventos` con `ConditionExpression = attribute_not_exists(event_id)`. Si la condición falla, el `event_id` ya se había recibido: el evento se cuenta como repetido y **no se suma a la ventana**.

### 3.4 Ventanas de 1 minuto

Las ventanas son *tumbling* por tiempo de evento (`event_time[:16]`, es decir, `YYYY-MM-DDTHH:MM`). En la tabla `t2-ventanas` (PK `airline`, SK `window_start`) se acumulan `conteo` y `suma_retraso` con `ADD`, y se calcula `promedio_retraso`.

### 3.5 Tablas DynamoDB

| Tabla | Llave | Contenido |
|---|---|---|
| `t2-ventanas` | `airline` (S) + `window_start` (S) | conteo, suma y promedio del retraso por minuto |
| `t2-eventos` | `event_id` (S) | estado válido o inválido; los inválidos llevan su `motivo` |
| `t2-contadores` | `nombre` (S) | totales de `unicos`, `repetidos` e `invalidos` |

### 3.6 Conteos

`conteos.py` verifica: **publicados = únicos procesados + repetidos descartados + inválidos descartados**. Además responde la pregunta del proyecto: *¿qué aerolínea tuvo mayor retraso promedio en vivo?*

### 3.7 Pasos de despliegue (consola)

1. **Kinesis:** crear el stream `t2-vuelos-stream` en modo Provisioned con 1 shard.
2. **DynamoDB:** crear las 3 tablas de la sección 3.5, con capacidad bajo demanda.
3. **Lambda:** crear `t2-ventanas` (Python 3.12, rol `LabRole`, timeout de 1 min) con el código de `lambda_ventanas.py`. Agregar un desencadenador de Kinesis con batch de 100 y posición inicial *Trim horizon*.
4. **Firehose:** origen Kinesis `t2-vuelos-stream`, destino S3 `raw-stream/`, buffer de 60 s y rol `LabRole`.
5. **CloudShell:** subir `productor.py` y `conteos.py`, y ejecutar `python3 productor.py` y luego `python3 conteos.py`.

---

## 4. Reproducibilidad

1. Crear el bucket `daniel-trabajo2-vuelos-2026` (uso general, espacio de nombres global) y subir `datos/flights/*.csv.gz` a `raw/flights/` y `datos/airlines/airlines.csv` a `raw/airlines/`.
2. Crear los 3 jobs de Glue con los scripts de `glue/` (Glue 6.0, G.1X, 2 workers, `LabRole`).
3. Crear la máquina de estados con `stepfunctions/maquina_estados.json` (Standard, `LabRole`).
4. Crear el horario en EventBridge Scheduler con destino StartExecution y `LabRole`.
5. Configurar la ubicación de resultados de Athena en `s3://<bucket>/athena-results/` y ejecutar `athena/consultas.sql`.
6. Desplegar la Parte B siguiendo la sección 3.7.

---

## 5. Declaración explícita de uso de IA

Se usó IA (Claude, de Anthropic) como asistente en este trabajo:

| Parte | Uso de IA |
|---|---|
| Scripts PySpark de Glue (`glue/`) | Código generado por IA y revisado y ejecutado por el estudiante |
| Máquina de estados (`maquina_estados.json`) | Generado por IA |
| Consultas SQL de Athena | Generadas por IA |
| Preparación de los datos (extracción de 2025 del proyecto integrador e inducción de errores) | Script ejecutado por IA |
| Código de la Parte B (`productor.py`, `lambda_ventanas.py`, `conteos.py`) | Generado por IA |
| Guía paso a paso de la consola de AWS | IA |
| Este informe | Redactado por IA a partir de los resultados reales de la ejecución |
| Creación de los recursos en la consola, ejecución e interpretación de los resultados | Estudiante, guiado por IA |

El estudiante asume la responsabilidad de todo lo generado y puede explicar cada componente: limpieza, join, particionamiento, orquestación, paralelismo, ventanas, deduplicación y conteos.

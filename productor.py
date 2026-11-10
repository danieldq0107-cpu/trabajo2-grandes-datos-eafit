# ============================================================
# Trabajo 2 - Parte B - PRODUCTOR
# Publica eventos de vuelos en Kinesis de forma continua (por defecto 6 min).
# A proposito envia REPETIDOS (mismo event_id) e INVALIDOS.
# Uso en CloudShell:  python3 productor.py        (6 minutos)
#                     python3 productor.py 330    (330 segundos)
# ============================================================
import json
import random
import sys
import time
import uuid
from datetime import datetime, timezone

import boto3

STREAM = "t2-vuelos-stream"
DURACION_S = int(sys.argv[1]) if len(sys.argv) > 1 else 360
EVENTOS_POR_SEGUNDO = 10
PROB_REPETIDO = 0.05
PROB_INVALIDO = 0.04

# aerolinea -> retraso medio (min), inspirado en los resultados de la Parte A
AEROLINEAS = {"F9": 22, "OH": 19, "B6": 18, "AA": 17, "WN": 16, "G4": 15,
              "NK": 14, "AS": 12, "DL": 10, "UA": 10, "MQ": 9, "OO": 8,
              "YX": 8, "HA": 5}
AEROPUERTOS = ["ATL", "DFW", "DEN", "ORD", "LAX", "JFK", "LAS", "MCO", "MIA", "BWI", "MDW"]

kinesis = boto3.client("kinesis", region_name="us-east-1")


def ahora():
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"


def evento_valido():
    a = random.choice(list(AEROLINEAS))
    retraso = round(max(-30.0, random.gauss(AEROLINEAS[a], 30)), 1)
    return {"event_id": str(uuid.uuid4()), "event_time": ahora(), "airline": a,
            "origin": random.choice(AEROPUERTOS), "dep_delay_min": retraso}


def evento_invalido():
    e = evento_valido()
    tipo = random.choice(["sin_airline", "sin_retraso", "fuera_de_rango", "no_numerico"])
    if tipo == "sin_airline":
        del e["airline"]
    elif tipo == "sin_retraso":
        del e["dep_delay_min"]
    elif tipo == "fuera_de_rango":
        e["dep_delay_min"] = random.choice([9999, -500, 3000])
    else:
        e["dep_delay_min"] = "mucho"
    return e, tipo


def enviar(lote):
    """put_records y reintento de los que fallen (para no perder ninguno)."""
    pendientes = lote
    while pendientes:
        resp = kinesis.put_records(StreamName=STREAM, Records=pendientes)
        if resp["FailedRecordCount"] == 0:
            return
        pendientes = [r for r, res in zip(pendientes, resp["Records"]) if "ErrorCode" in res]
        time.sleep(0.5)


publicados = repetidos = invalidos = validos = 0
invalidos_por_tipo = {}
enviados = []
inicio = time.time()
print(f"Publicando en '{STREAM}' durante {DURACION_S} s ...")

while time.time() - inicio < DURACION_S:
    lote = []
    for _ in range(EVENTOS_POR_SEGUNDO):
        r = random.random()
        if r < PROB_REPETIDO and enviados:
            e = random.choice(enviados[-300:])          # el MISMO evento otra vez
            repetidos += 1
        elif r < PROB_REPETIDO + PROB_INVALIDO:
            e, tipo = evento_invalido()
            invalidos += 1
            invalidos_por_tipo[tipo] = invalidos_por_tipo.get(tipo, 0) + 1
        else:
            e = evento_valido()
            enviados.append(e)
            validos += 1
        lote.append({"Data": json.dumps(e).encode(), "PartitionKey": e.get("airline", "SIN")})
    enviar(lote)
    publicados += len(lote)
    seg = int(time.time() - inicio)
    if seg % 30 == 0:
        print(f"  {seg:>4} s  publicados={publicados}")
    time.sleep(1)

resumen = {"publicados": publicados, "validos_unicos": validos,
           "repetidos_enviados": repetidos, "invalidos_enviados": invalidos,
           "invalidos_por_tipo": invalidos_por_tipo,
           "duracion_s": round(time.time() - inicio, 1)}
with open("resumen_productor.json", "w") as f:
    json.dump(resumen, f, indent=2)
print(json.dumps(resumen, indent=2))

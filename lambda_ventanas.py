# ============================================================
# Trabajo 2 - Parte B - Lambda consumidora del stream de Kinesis
# - Reconoce REPETIDOS por event_id (escritura condicional en DynamoDB)
# - Aparta INVALIDOS con su MOTIVO (tabla t2-eventos, estado = invalido)
# - Calcula por VENTANAS DE 1 MINUTO y por AEROLINEA: conteo y promedio
#   del retraso (tabla t2-ventanas)
# - Lleva contadores globales: unicos, repetidos, invalidos (t2-contadores)
# ============================================================
import base64
import json
from decimal import Decimal, ROUND_HALF_UP

import boto3
from botocore.exceptions import ClientError

ddb = boto3.resource("dynamodb")
T_EVENTOS = ddb.Table("t2-eventos")
T_VENTANAS = ddb.Table("t2-ventanas")
T_CONTADORES = ddb.Table("t2-contadores")

RETRASO_MIN, RETRASO_MAX = -60, 1500   # rango valido de minutos de retraso


def validar(e):
    """Devuelve el motivo de descarte, o None si el evento es valido."""
    if not isinstance(e, dict) or not e:
        return "JSON invalido"
    if not e.get("airline"):
        return "falta airline (categoria)"
    if not e.get("event_time"):
        return "falta event_time"
    if e.get("dep_delay_min") is None:
        return "falta dep_delay_min (campo numerico)"
    try:
        d = float(e["dep_delay_min"])
    except (TypeError, ValueError):
        return "dep_delay_min no es numero"
    if d < RETRASO_MIN or d > RETRASO_MAX:
        return f"dep_delay_min fuera de rango [{RETRASO_MIN}, {RETRASO_MAX}]"
    return None


def lambda_handler(event, context):
    ventanas = {}
    cont = {"unicos": 0, "repetidos": 0, "invalidos": 0}

    for rec in event["Records"]:
        crudo = base64.b64decode(rec["kinesis"]["data"]).decode("utf-8", "replace")
        try:
            e = json.loads(crudo)
        except json.JSONDecodeError:
            e = {}
        eid = (e.get("event_id") if isinstance(e, dict) else None) or \
            "sin-id-" + rec["kinesis"]["sequenceNumber"]

        motivo = validar(e)
        if motivo:
            item = {"event_id": eid, "estado": "invalido", "motivo": motivo,
                    "evento_crudo": crudo[:1000]}
        else:
            ventana = e["event_time"][:16]          # 'YYYY-MM-DDTHH:MM' -> ventana de 1 minuto
            retraso = Decimal(str(e["dep_delay_min"]))
            item = {"event_id": eid, "estado": "valido", "airline": e["airline"],
                    "dep_delay_min": retraso, "ventana": ventana}

        # REPETIDOS: solo se escribe si el event_id NO existe todavia
        try:
            T_EVENTOS.put_item(Item=item, ConditionExpression="attribute_not_exists(event_id)")
        except ClientError as ex:
            if ex.response["Error"]["Code"] == "ConditionalCheckFailedException":
                cont["repetidos"] += 1          # ya lo habiamos visto: no se cuenta dos veces
                continue
            raise

        if motivo:
            cont["invalidos"] += 1
            continue

        cont["unicos"] += 1
        clave = (item["airline"], item["ventana"])
        n, s = ventanas.get(clave, (0, Decimal(0)))
        ventanas[clave] = (n + 1, s + item["dep_delay_min"])

    # Acumular cada ventana de 1 minuto por aerolinea
    for (aerolinea, ventana), (n, s) in ventanas.items():
        r = T_VENTANAS.update_item(
            Key={"airline": aerolinea, "window_start": ventana},
            UpdateExpression="ADD conteo :n, suma_retraso :s",
            ExpressionAttributeValues={":n": n, ":s": s},
            ReturnValues="UPDATED_NEW")
        total_n = r["Attributes"]["conteo"]
        total_s = r["Attributes"]["suma_retraso"]
        promedio = (total_s / total_n).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)
        T_VENTANAS.update_item(
            Key={"airline": aerolinea, "window_start": ventana},
            UpdateExpression="SET promedio_retraso = :p",
            ExpressionAttributeValues={":p": promedio})

    for nombre, valor in cont.items():
        if valor:
            T_CONTADORES.update_item(
                Key={"nombre": nombre},
                UpdateExpression="ADD total :v",
                ExpressionAttributeValues={":v": valor})

    print(json.dumps(cont))
    return cont

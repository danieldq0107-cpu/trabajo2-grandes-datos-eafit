# ============================================================
# Trabajo 2 - Parte B - VERIFICACION DE CONTEOS Y CONSULTA
# publicados = unicos procesados + repetidos descartados + invalidos descartados
# Uso en CloudShell (despues del productor):  python3 conteos.py
# ============================================================
import json
from collections import Counter, defaultdict

import boto3

ddb = boto3.resource("dynamodb", region_name="us-east-1")


def escanear(nombre):
    t = ddb.Table(nombre)
    r = t.scan()
    items = r["Items"]
    while "LastEvaluatedKey" in r:
        r = t.scan(ExclusiveStartKey=r["LastEvaluatedKey"])
        items += r["Items"]
    return items


prod = json.load(open("resumen_productor.json"))
eventos = escanear("t2-eventos")
ventanas = escanear("t2-ventanas")
contadores = {i["nombre"]: int(i["total"]) for i in escanear("t2-contadores")}

estados = Counter(i["estado"] for i in eventos)
motivos = Counter(i["motivo"] for i in eventos if i["estado"] == "invalido")
unicos = contadores.get("unicos", 0)
repetidos = contadores.get("repetidos", 0)
invalidos = contadores.get("invalidos", 0)
suma_ventanas = sum(int(v["conteo"]) for v in ventanas)

print("=" * 60)
print("CONTEOS")
print("=" * 60)
print(f"Publicados por el productor ........ {prod['publicados']}")
print(f"Unicos procesados (validos) ........ {unicos}   (en t2-eventos: {estados['valido']}, suma ventanas: {suma_ventanas})")
print(f"Repetidos descartados .............. {repetidos}   (enviados a proposito: {prod['repetidos_enviados']})")
print(f"Invalidos descartados .............. {invalidos}   (en t2-eventos: {estados['invalido']}, enviados: {prod['invalidos_enviados']})")
total = unicos + repetidos + invalidos
print(f"Unicos + repetidos + invalidos ..... {total}")
print("CUADRA" if total == prod["publicados"] else f"NO CUADRA (faltan {prod['publicados'] - total}; espere 1 min y repita)")
print("\nInvalidos por motivo:")
for m, n in motivos.most_common():
    print(f"  {m:<45} {n}")

print("\n" + "=" * 60)
print("PREGUNTA: ¿que aerolinea tuvo mayor retraso promedio en vivo?")
print("=" * 60)
acum = defaultdict(lambda: [0, 0.0])
for v in ventanas:
    acum[v["airline"]][0] += int(v["conteo"])
    acum[v["airline"]][1] += float(v["suma_retraso"])
filas = sorted(((a, n, s / n) for a, (n, s) in acum.items()), key=lambda x: -x[2])
print(f"{'aerolinea':<10}{'eventos':>10}{'retraso prom (min)':>22}")
for a, n, p in filas:
    print(f"{a:<10}{n:>10}{p:>22.2f}")
print(f"\nVentanas de 1 minuto guardadas en t2-ventanas: {len(ventanas)}")

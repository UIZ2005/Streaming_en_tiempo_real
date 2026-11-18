"""
Casos de prueba  para los jobs de Flink 
Envia a Kafka transacciones disenadas para producir un resultado conocido.

Para prbar de froam controlada lso requerimientos y casos de prueba
    docker compose run --rm pruebas enviar_casos.py <caso>     


Casos:
    monto      CP-09  pago de 50 millones                      -> MONTO_FUERA_DE_RANGO (umbral fijo)
    promedio   CP-10  4 pagos de 50.000 y uno de 900.000        -> MONTO_FUERA_DE_RANGO (18 veces el promedio)
    rafaga     CP-11  12 pagos seguidos de la misma tarjeta     -> ALTA_FRECUENCIA (una sola alerta)
    ciudad     CP-12  misma tarjeta en Cali y luego en Medellin -> UBICACION_IMPOSIBLE
    login      CP-13  6 inicios de sesion fallidos seguidos     -> FUERZA_BRUTA_LOGIN (una sola alerta)
    normal     CP-14  un pago normal de 80.000                  -> NINGUNA alerta
    dlq        CP-17  7 mensajes mal formados                   -> 7 mensajes en el topic dlq
    estado1    CP-22  pago en Cali (antes de reiniciar el TaskManager)
    estado2 ID CP-22  pago en Medellin con la misma tarjeta (despues del reinicio) -> UBICACION_IMPOSIBLE
    
"""
import json
import os
import sys
import time
import uuid
from datetime import datetime, timedelta, timezone

from kafka import KafkaProducer

BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:29092")  # puerto externo de Kafka
COL = timezone(timedelta(hours=-5))                          # hora de Colombia, tambien dentro de Docker
SUF = datetime.now(COL).strftime("%H%M%S")                                 # hace unicos los IDs de cada corrida

prod = KafkaProducer(bootstrap_servers=BOOTSTRAP)


def ms():
    return int(time.time() * 1000)


def enviar(topic, valor, key=None):
    data = valor if isinstance(valor, bytes) else json.dumps(valor, ensure_ascii=False).encode()
    prod.send(topic, key=key.encode() if key else None, value=data)


def pago(tarjeta, monto, ciudad):
    return {"event_id": str(uuid.uuid4()), "tipo": "pago_tarjeta", "id_tarjeta": tarjeta,
            "id_cliente": "C-DEMO", "monto": monto, "moneda": "COP", "comercio": "Comercio de prueba",
            "ciudad": ciudad, "ts_evento": ms()}


def mostrar(caso, buscar, esperado):
    print(f"\n[{caso}] enviado a las {datetime.now(COL).strftime('%H:%M:%S')}")
    print(f"  Buscar en Kafka UI : {buscar}")
    print(f"  Resultado esperado : {esperado}")


def caso_monto():
    t = f"T-DEMO1-{SUF}"
    enviar("pagos-tarjeta", pago(t, 50_000_000, "Cali"), t)
    mostrar("monto", f"alertas-fraude -> {t}",
            "1 alerta MONTO_FUERA_DE_RANGO: 'monto 50,000,000 supera el umbral 10,000,000'")


def caso_promedio():
    t = f"T-DEMO6-{SUF}"
    for _ in range(4):
        enviar("pagos-tarjeta", pago(t, 50_000, "Cali"), t)
        time.sleep(0.5)
    enviar("pagos-tarjeta", pago(t, 900_000, "Cali"), t)
    mostrar("promedio", f"alertas-fraude -> {t}",
            "1 alerta MONTO_FUERA_DE_RANGO: 'monto 900,000 es 18 veces el promedio 50,000'")


def caso_rafaga():
    t = f"T-DEMO2-{SUF}"
    for _ in range(12):
        enviar("pagos-tarjeta", pago(t, 45_000, "Cali"), t)
        time.sleep(0.2)
    mostrar("rafaga", f"alertas-fraude -> {t}",
            "1 sola alerta ALTA_FRECUENCIA: '11 pagos en 60 s' (el pago 12 no repite la alerta)")


def caso_ciudad():
    t = f"T-DEMO3-{SUF}"
    enviar("pagos-tarjeta", pago(t, 80_000, "Cali"), t)
    time.sleep(1)
    enviar("pagos-tarjeta", pago(t, 95_000, "Medellín"), t)
    mostrar("ciudad", f"alertas-fraude -> {t}",
            "1 alerta UBICACION_IMPOSIBLE: 'Cali -> Medellín en 0.0 min'")


def caso_login():
    c = f"C-DEMO4-{SUF}"
    for _ in range(6):
        enviar("logins", {"event_id": str(uuid.uuid4()), "tipo": "login", "id_cliente": c,
                          "exito": False, "ip": "200.10.20.30", "ciudad": "Cali", "ts_evento": ms()}, c)
        time.sleep(0.3)
    mostrar("login", f"alertas-fraude -> {c}",
            "1 sola alerta FUERZA_BRUTA_LOGIN: '5 intentos fallidos en 2 min'")


def caso_normal():
    t = f"T-DEMO5-{SUF}"
    enviar("pagos-tarjeta", pago(t, 80_000, "Cali"), t)
    mostrar("normal", f"eventos-validos -> {t}  y  alertas-fraude -> {t}",
            "1 mensaje en eventos-validos y NINGUNO en alertas-fraude")


def caso_dlq():
    tag = f"DLQ-{SUF}"
    enviar("pagos-tarjeta", f"{{esto no es json {tag}".encode())
    enviar("pagos-tarjeta", {"event_id": f"{tag}-2", "tipo": "pago_tarjeta", "id_tarjeta": "T-X",
                             "monto": "abc", "ciudad": "Cali", "ts_evento": ms()})
    enviar("pagos-tarjeta", {"event_id": f"{tag}-3", "tipo": "pago_tarjeta", "id_tarjeta": "T-X",
                             "monto": -500, "ciudad": "Cali", "ts_evento": ms()})
    enviar("transferencias", {"event_id": f"{tag}-4", "tipo": "transferencia", "monto": 1000})
    enviar("logins", {"event_id": f"{tag}-5", "tipo": "retiro_cajero", "ts_evento": ms()})
    enviar("pagos-tarjeta", {"event_id": f"{tag}-6", "tipo": "pago_tarjeta", "id_tarjeta": "T-X",
                             "monto": 1000, "ciudad": "Cali", "ts_evento": 1762300000})
    enviar("logins", {"event_id": f"{tag}-7", "tipo": "login", "id_cliente": "C-X",
                      "exito": "si", "ts_evento": ms()})
    mostrar("dlq", f"dlq -> {tag}   (y eventos-validos -> {tag} debe dar 0 resultados)",
            "7 mensajes en dlq, cada uno con su campo 'error'; los jobs siguen en RUNNING")


def caso_estado1():
    t = f"T-ESTADO-{SUF}"
    enviar("pagos-tarjeta", pago(t, 70_000, "Cali"), t)
    mostrar("estado1", f"eventos-validos -> {t}", "1 pago en Cali, sin alerta")
    print(f"\n  SIGUIENTE: espera 15 s, reinicia el TaskManager y, cuando el job vuelva a RUNNING, ejecuta:")
    print(f"  <mismo comando con> estado2 {t}")


def caso_estado2(t):
    enviar("pagos-tarjeta", pago(t, 75_000, "Medellín"), t)
    mostrar("estado2", f"alertas-fraude -> {t}",
            "1 alerta UBICACION_IMPOSIBLE 'Cali -> Medellín': Flink recordaba el pago de Cali tras el reinicio")


CASOS = {"monto": caso_monto, "promedio": caso_promedio, "rafaga": caso_rafaga,
         "ciudad": caso_ciudad, "login": caso_login, "normal": caso_normal, "dlq": caso_dlq}

if __name__ == "__main__":
    pedido = sys.argv[1] if len(sys.argv) > 1 else ""
    if pedido == "todos":
        for f in CASOS.values():
            f()
    elif pedido in CASOS:
        CASOS[pedido]()
    elif pedido == "estado1":
        caso_estado1()
    elif pedido == "estado2" and len(sys.argv) > 2:
        caso_estado2(sys.argv[2])
    else:
        print(__doc__)
        sys.exit(1)
    prod.flush()
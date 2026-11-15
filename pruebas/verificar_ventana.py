"""
Verificacion manual de R2.3.
Recalcula, a partir de los eventos crudos de 'eventos-validos', el conteo, promedio, maximo y
total de UNA ventana de 1 minuto, y lo compara con lo que Flink escribio en 'agregados-minuto'.

    docker compose run --rm pruebas verificar_ventana.py                 # verifica la ultima ventana cerrada
    docker compose run --rm pruebas verificar_ventana.py 1793899320000   # o una ventana concreta
    docker compose run --rm pruebas verificar_ventana.py lista           # muestra las ventanas disponibles
"""
import json
import os
import sys
from collections import defaultdict
from datetime import datetime, timedelta, timezone

from kafka import KafkaConsumer, TopicPartition

BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:29092")
MARGEN_MS = 60_000  # se leen tambien los mensajes de 1 minuto antes y despues, por si llegaron desordenados


def hora(epoch_ms):
    return datetime.fromtimestamp(epoch_ms / 1000, timezone(timedelta(hours=-5))).strftime("%H:%M:%S")  # hora de Colombia


def consumidor():
    return KafkaConsumer(bootstrap_servers=BOOTSTRAP, enable_auto_commit=False,
                         group_id=None, consumer_timeout_ms=10_000)


def leer_agregados():
    """Todos los mensajes de agregados-minuto, agrupados por ventana_inicio."""
    c = consumidor()
    tps = [TopicPartition("agregados-minuto", p) for p in c.partitions_for_topic("agregados-minuto")]
    c.assign(tps)
    c.seek_to_beginning(*tps)
    fin = c.end_offsets(tps)
    ventanas = defaultdict(dict)
    while any(c.position(tp) < fin[tp] for tp in tps):
        for registros in c.poll(timeout_ms=2000).values():
            for r in registros:
                a = json.loads(r.value)
                ventanas[a["ventana_inicio"]][f"{a['dimension']}:{a['valor']}"] = a
    c.close()
    return ventanas


def leer_eventos(inicio, fin_ventana):
    """Eventos de eventos-validos cuyo ts_evento cae en [inicio, fin_ventana)."""
    c = consumidor()
    tps = [TopicPartition("eventos-validos", p) for p in c.partitions_for_topic("eventos-validos")]
    c.assign(tps)
    fin = c.end_offsets(tps)
    desde = c.offsets_for_times({tp: inicio - MARGEN_MS for tp in tps})
    activos = set()
    for tp in tps:
        if desde[tp] is None:          # no hay mensajes tan recientes en esta particion
            continue
        c.seek(tp, desde[tp].offset)
        activos.add(tp)
    eventos, leidos = [], 0
    while activos:
        lote = c.poll(timeout_ms=2000)
        if not lote:
            break
        for tp, registros in lote.items():
            for r in registros:
                leidos += 1
                ev = json.loads(r.value)
                if inicio <= ev["ts_evento"] < fin_ventana:
                    eventos.append(ev)
                if (r.timestamp or 0) > fin_ventana + MARGEN_MS or r.offset + 1 >= fin[tp]:
                    activos.discard(tp)
                    c.pause(tp)
    c.close()
    return eventos, leidos


def calcular(eventos):
    
    grupos = defaultdict(list)
    for ev in eventos:
        if ev["tipo"] == "login":
            continue
        grupos[f"tipo:{ev['tipo']}"].append(float(ev["monto"]))
        if ev.get("ciudad"):
            grupos[f"ciudad:{ev['ciudad']}"].append(float(ev["monto"]))
    return {k: {"conteo": len(v), "monto_promedio": round(sum(v) / len(v), 2),
                "monto_maximo": max(v), "monto_total": round(sum(v), 2)} for k, v in grupos.items()}


def comparar(flink, manual):
    print(f"\n{'Grupo':<26}{'Campo':<16}{'Flink':>20}{'Manual':>20}  Resultado")
    print("-" * 90)
    fallos = 0
    for clave in sorted(set(flink) | set(manual)):
        f, m = flink.get(clave), manual.get(clave)
        if f is None or m is None:
            fallos += 1
            print(f"{clave:<26}{'(falta)':<16}{'si' if f else 'NO':>20}{'si' if m else 'NO':>20}  FALLA")
            continue
        for campo in ("conteo", "monto_promedio", "monto_maximo", "monto_total"):
            ok = abs(f[campo] - m[campo]) <= 0.02   # ambos redondean a 2 decimales
            fallos += 0 if ok else 1
            print(f"{clave:<26}{campo:<16}{f[campo]:>20,.2f}{m[campo]:>20,.2f}  {'OK' if ok else 'FALLA'}")
    return fallos


def main():
    print(f"Leyendo agregados-minuto desde {BOOTSTRAP} ...")
    ventanas = leer_agregados()
    if not ventanas:
        sys.exit("No hay agregados todavia. Espera a que cierre al menos un minuto.")
    disponibles = sorted(ventanas)
    if len(sys.argv) > 1 and sys.argv[1] == "lista":
        for v in disponibles:
            print(f"{v}  ({hora(v)} - {hora(v + 60_000)})  {len(ventanas[v])} agregados")
        return
    inicio = int(sys.argv[1]) if len(sys.argv) > 1 else disponibles[-1]
    if inicio not in ventanas:
        sys.exit(f"La ventana {inicio} no existe. Usa 'lista' para ver las disponibles.")
    fin_ventana = inicio + 60_000
    print(f"Ventana a verificar: {inicio}  ({hora(inicio)} - {hora(fin_ventana)})")
    print("Leyendo eventos-validos de esa ventana ...")
    eventos, leidos = leer_eventos(inicio, fin_ventana)
    print(f"Mensajes revisados: {leidos:,}   eventos dentro de la ventana: {len(eventos):,}")
    fallos = comparar(ventanas[inicio], calcular(eventos))
    print("-" * 90)
    if fallos == 0:
        print("RESULTADO: APROBADO. Los agregados de Flink coinciden con el calculo manual.")
    else:
        print(f"RESULTADO: {fallos} diferencias. Si son pocas unidades en el conteo, revisa si en esa ventana "
              "hubo un reinicio (eventos duplicados) o eventos atrasados mas de 2 s (Flink los descarta). "
              "Prueba con otra ventana.")


if __name__ == "__main__":
    main()
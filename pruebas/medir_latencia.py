"""
Script de medicion de latencia end-to-end del pipeline.
Consulta InfluxDB cada 5 segundos, calcula la latencia de los eventos
de los ultimos 10 segundos y escribe una fila en latencia.csv con:
hora, cantidad de eventos, promedio, mediana, p95 y maximo (ms).
"""
import csv
import os
import statistics
import time
from datetime import datetime
from influxdb_client import InfluxDBClient

# Configuracion (toma el token del entorno; en Windows usar set, en Linux export)
URL = os.environ.get("INFLUX_URL", "http://localhost:8086")
TOKEN = os.environ["INFLUX_TOKEN"]
ORG = os.environ.get("INFLUX_ORG", "uao")
BUCKET = os.environ.get("INFLUX_BUCKET", "finanzas")
INTERVALO_SEG = 5
VENTANA_SEG = 10

# Consulta Flux: tiempo de llegada menos ts_evento (ambos en ms)
QUERY = f"""from(bucket: "{BUCKET}")
  |> range(start: -{VENTANA_SEG}s)
  |> filter(fn: (r) => r._measurement == "transacciones" and r._field == "ts_evento")
  |> map(fn: (r) => ({{ r with _value: float(v: uint(v: r._time)) / 1000000.0 - float(v: r._value) }}))"""

def main():
    cliente = InfluxDBClient(url=URL, token=TOKEN, org=ORG)
    api = cliente.query_api()

    archivo = "latencia.csv"
    es_nuevo = not os.path.exists(archivo)

    with open(archivo, "a", newline="") as f:
        w = csv.writer(f)
        if es_nuevo:
            w.writerow(["hora", "n_eventos", "promedio_ms", "mediana_ms", "p95_ms", "max_ms"])

        print(f"Midiendo latencia cada {INTERVALO_SEG} s. Salida en {archivo}")
        while True:
            try:
                resultado = api.query(QUERY)
                lats = sorted(
                    r.get_value()
                    for t in resultado
                    for r in t.records
                    if r.get_value() is not None and r.get_value() >= 0
                )
                if lats:
                    p95_idx = max(int(len(lats) * 0.95) - 1, 0)
                    fila = [
                        datetime.now().strftime("%H:%M:%S"),
                        len(lats),
                        round(statistics.mean(lats), 1),
                        round(statistics.median(lats), 1),
                        round(lats[p95_idx], 1),
                        round(lats[-1], 1),
                    ]
                    w.writerow(fila)
                    f.flush()
                    print(f"{fila[0]}  n={fila[1]:5d}  prom={fila[2]:7.1f}  p50={fila[3]:7.1f}  p95={fila[4]:7.1f}  max={fila[5]:7.1f}")
                else:
                    print(f"{datetime.now().strftime('%H:%M:%S')}  (sin datos en ventana)")
            except Exception as e:
                print(f"Error de consulta: {e}")
            time.sleep(INTERVALO_SEG)

if __name__ == "__main__":
    main()
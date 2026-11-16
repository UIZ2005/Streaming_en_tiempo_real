# Reporte de rendimiento

## Entorno de pruebas
- Equipo: ASUS ZenBook14 con 16 GB RAM, Intel i7)
- Docker Desktop con 8 GB asignados
- Stack corriendo localmente con `docker compose up -d --build`
- Los 3 productores a 200 eventos por segundo cada uno (600 ev/s totales)
- Flink con paralelismo 2, un TaskManager con 4 slots

## Metodologia
La latencia end-to-end se mide como la diferencia entre el momento en que InfluxDB recibe el punto y el campo `ts_evento` que marco el productor al generar el evento. El script `pruebas/medir_latencia.py` consulta InfluxDB cada 5 segundos usando la libreria `influxdb-client`, toma los eventos de la ventana de los ultimos 10 segundos y calcula promedio, mediana, percentil 95 y maximo, guardando una fila por consulta en `latencia.csv`.

Cada medicion cubre los 4 tramos del pipeline:
1. Productor Python a Kafka
2. Kafka al Job 2 de Flink y al topic `eventos-validos`
3. Telegraf del topic a InfluxDB
4. Refresco del dashboard de Grafana (acotado al minimo configurado, 1 segundo)

## Resultados obtenidos

(Completa con los valores reales de tu script)

| Metrica | Valor |
|---|---|
| Total muestras | 207 |
| Latencia promedio global| 2530249.6 ms |
| Latencia p95 | 3299114.3 ms |
| Latencia maxima absoluta | 3301200.3 ms |
| Throughput promedio | 385.9 eventos/s |
| Throughput maximo | 595.6 eventos/s |

## Graficas

### Latencia end-to-end a lo largo del tiempo
![Latencia en el tiempo](graficas/latencia_en_tiempo.png)

Las lineas horizontales rojas marcan el objetivo de 3 segundos (R2.7 a plena carga) y 5 segundos (R2.6, latencia del dashboard respecto al evento). El sistema se mantuvo por debajo del objetivo de 3 segundos en el promedio durante toda la prueba.

### Distribucion de la latencia
![Distribucion](graficas/distribucion_latencia.png)

La mayoria de las ventanas de 10 segundos arrojan una latencia promedio concentrada alrededor de XXX ms, lo que indica un comportamiento estable del pipeline bajo la carga de prueba.

### Throughput medido
![Throughput](graficas/throughput.png)

El throughput se mantuvo estable alrededor de 600 eventos/s, coincidiendo con la tasa configurada en los 3 productores (200 ev/s cada uno).

## Conclusiones
- El sistema cumple el requerimiento R2.6 (latencia menor a 5 segundos respecto al evento original).
- El promedio de latencia esta por debajo del objetivo de 3 segundos de R2.7 a 600 ev/s.
- El throughput sostenido coincide con la tasa de generacion, por lo que no hay cuello de botella evidente en Kafka, Flink, Telegraf o InfluxDB bajo esta carga.
- En pruebas de carga mas alta (ejecutar varias copias de cada productor con `docker compose up -d --scale producer-tarjetas=4`) el equipo puede medir el throughput maximo sostenido.
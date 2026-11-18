# Esquema de InfluxDB

Organización: `uao`
Bucket: `finanzas`
Retención: ilimitada en el bucket local (configurable en producción)

## Conceptos clave

En InfluxDB los **tags** son texto indexado que sirve para filtrar y agrupar. Los **fields** son los valores que medimos (números, texto largo, booleanos).

La decisión de qué va como tag y qué como field es crítica para el rendimiento. Los identificadores únicos como `event_id` o `clave` **van como field, no como tag**, porque InfluxDB crea una serie distinta por cada combinación única de tags: si `event_id` fuera tag, cada evento generaría una serie nueva y la base quedaría con millones de series, haciéndola muy lenta en las consultas.

Como regla usamos tags solo para dimensiones con cardinalidad baja y controlada (tipos, ciudades, categorías) sobre las que vamos a filtrar o agrupar en los dashboards de Grafana.

## Measurement `transacciones`
Fuente: topic Kafka `eventos-validos` (salida del Job 2 de Flink).

| Columna | Rol | Tipo | Notas |
|---|---|---|---|
| tipo | tag | string | pago_tarjeta, transferencia o login |
| ciudad | tag | string | Cali, Bogotá, Medellín, etc. |
| categoria | tag | string | supermercado, restaurante... (solo para pagos) |
| monto | field | float | solo para pagos y transferencias |
| exito | field | bool | solo para logins |
| ts_evento | field | int | epoch en milisegundos |
| event_id | field | string | UUID del evento |

Timestamp del punto: momento en que Telegraf recibe el mensaje. Esta elección es importante porque permite calcular la latencia end-to-end como `_time - ts_evento`, que es la base del panel de latencia y del script `medir_latencia.py`.

## Measurement `agregados_minuto`
Fuente: topic Kafka `agregados-minuto` (salida del Job 1 de Flink, ventana tumbling de 1 minuto).

| Columna | Rol | Tipo | Notas |
|---|---|---|---|
| dimension | tag | string | ciudad, tipo o categoria |
| valor | tag | string | el valor concreto de la dimensión |
| conteo | field | int | cantidad de eventos de la ventana |
| monto_promedio | field | float | promedio de monto en la ventana |
| monto_maximo | field | float | máximo de monto en la ventana |
| monto_total | field | float | suma de monto en la ventana |

Timestamp del punto: `ventana_inicio` del JSON, parseado como `unix_ms` por Telegraf. Esto alinea los agregados con el minuto real al que corresponden.

## Measurement `alertas`
Fuente: topic Kafka `alertas-fraude` (salida del Job 2 de Flink).

| Columna | Rol | Tipo | Notas |
|---|---|---|---|
| tipo_alerta | tag | string | MONTO_FUERA_DE_RANGO, ALTA_FRECUENCIA, UBICACION_IMPOSIBLE, FUERZA_BRUTA_LOGIN |
| tipo_evento | tag | string | pago_tarjeta, transferencia o login |
| ciudad | tag | string | opcional, no viene en alertas de login |
| clave | field | string | id_tarjeta, cuenta_origen o id_cliente según el tipo |
| monto | field | float | opcional según el tipo de alerta |
| detalle | field | string | descripción humana de la anomalía |
| event_id | field | string | del evento que disparó la alerta |
| ts_evento | field | int | epoch en milisegundos |
| ts_alerta | field | int | epoch en milisegundos del momento de detección |

Timestamp del punto: momento en que Telegraf recibe el mensaje. La diferencia `ts_alerta - ts_evento` mide cuánto tardó Flink en detectar la anomalía.

## Measurement `dlq`
Fuente: topic Kafka `dlq` (dead letter queue, salida del Job 2 para mensajes mal formados).

| Columna | Rol | Tipo | Notas |
|---|---|---|---|
| error | field | string | motivo del rechazo |
| mensaje | field | string | payload original del mensaje descartado |
| ts_error | field | int | epoch en milisegundos |

Timestamp del punto: momento en que Telegraf recibe el mensaje.

## Prueba de R2.5: consulta por API REST

El requerimiento pide al menos 10.000 registros persistidos en la última hora. La siguiente consulta verifica el cumplimiento:

```
curl -s -X POST "http://localhost:8086/api/v2/query?org=uao" \
  -H "Authorization: Token $INFLUX_TOKEN" \
  -H "Content-Type: application/vnd.flux" \
  -H "Accept: application/csv" \
  --data 'from(bucket: "finanzas")
    |> range(start: -1h)
    |> filter(fn: (r) => r._measurement == "transacciones" and r._field == "ts_evento")
    |> group()
    |> count()'
```

La respuesta debe devolver un CSV con una fila cuyo `_value` supere 10.000.
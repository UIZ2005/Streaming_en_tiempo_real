# Esquema de InfluxDB

Organización: `uao`
Bucket: `finanzas`

En InfluxDB los tags son texto indexado para filtrar y agrupar; los fields son los valores numéricos o booleanos que medimos. Los identificadores únicos como event_id o clave van como field, no como tag, porque si fueran tags crearían millones de series distintas y la base se volvería lenta.

## Measurement `transacciones` (del topic eventos-validos)
| Columna | Rol | Tipo |
|---|---|---|
| tipo | tag | string |
| ciudad | tag | string |
| categoria | tag | string (opcional) |
| monto | field | float (opcional) |
| exito | field | bool (opcional) |
| ts_evento | field | int (epoch ms) |
| event_id | field | string |

Timestamp del punto: momento en que Telegraf recibe el mensaje. Esto permite calcular latencia como tiempo_llegada menos ts_evento.

## Measurement `agregados_minuto` (del topic agregados-minuto)
| Columna | Rol | Tipo |
|---|---|---|
| dimension | tag | string (ciudad, tipo o categoria) |
| valor | tag | string |
| conteo | field | int |
| monto_promedio | field | float |
| monto_maximo | field | float |
| monto_total | field | float |

Timestamp del punto: ventana_inicio del JSON.

## Measurement `alertas` (del topic alertas-fraude)
| Columna | Rol | Tipo |
|---|---|---|
| tipo_alerta | tag | string |
| tipo_evento | tag | string |
| ciudad | tag | string (opcional) |
| clave | field | string |
| monto | field | float (opcional) |
| detalle | field | string |
| event_id | field | string |
| ts_evento | field | int |
| ts_alerta | field | int |

## Measurement `dlq` (del topic dlq)
| Columna | Rol | Tipo |
|---|---|---|
| error | field | string |
| mensaje | field | string |
| ts_error | field | int |

## Prueba R2.5: consulta por API REST
Ver README y `curl` de ejemplo. Debe devolver más de 10.000 registros tras ejecutar unos minutos.
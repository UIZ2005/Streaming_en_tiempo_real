# Esquema de eventos y reglas de validación

Este documento describe los tres tipos de eventos JSON que publican los productores Python en Kafka, las reglas que los convierten en inválidos y las reglas que el Job 2 de Flink aplica para detectar fraude.

## Convenciones generales

Todos los eventos son objetos JSON con dos campos obligatorios compartidos:

- `event_id`: identificador único del evento (UUID v4 generado por el productor)
- `ts_evento`: timestamp del evento en epoch milisegundos (generado por el productor con `int(time.time() * 1000)`)

El campo `ts_evento` en milisegundos es crítico para la medición de latencia end-to-end en InfluxDB. Un evento con `ts_evento` en segundos en lugar de milisegundos se considera mal formado y va a la DLQ.

## Evento `pago_tarjeta`
Topic: `pagos-tarjeta`
Clave del mensaje de Kafka: `id_tarjeta` (para que todos los pagos de la misma tarjeta caigan en la misma partición y lleguen ordenados a Flink)

```json
{
  "event_id": "8f1c2a7e-...",
  "tipo": "pago_tarjeta",
  "id_tarjeta": "T-00421",
  "id_cliente": "C-00421",
  "monto": 185000.0,
  "moneda": "COP",
  "comercio": "Distribuidora Andina S.A.S.",
  "categoria": "supermercado",
  "ciudad": "Cali",
  "canal": "pos",
  "ts_evento": 1762127412345
}
```

Campos obligatorios: `event_id`, `tipo`, `id_tarjeta`, `id_cliente`, `monto`, `ciudad`, `categoria`, `ts_evento`.
Valores válidos de `canal`: `pos`, `web`, `app`.

## Evento `transferencia`
Topic: `transferencias`
Clave del mensaje de Kafka: `cuenta_origen`

```json
{
  "event_id": "b27d...",
  "tipo": "transferencia",
  "cuenta_origen": "AC-01029",
  "cuenta_destino": "AC-05531",
  "monto": 1200000.0,
  "ciudad": "Bogotá",
  "ts_evento": 1762127412400
}
```

Campos obligatorios: `event_id`, `tipo`, `cuenta_origen`, `cuenta_destino`, `monto`, `ts_evento`.

## Evento `login`
Topic: `logins`
Clave del mensaje de Kafka: `id_cliente`

```json
{
  "event_id": "c90e...",
  "tipo": "login",
  "id_cliente": "C-00193",
  "exito": false,
  "ip": "181.52.10.4",
  "ciudad": "Cali",
  "ts_evento": 1762127412500
}
```

Campos obligatorios: `event_id`, `tipo`, `id_cliente`, `exito`, `ts_evento`.
El campo `exito` es booleano: `true` indica login exitoso, `false` indica fallo.

## Reglas para mensajes mal formados (DLQ)

El Job 2 de Flink descarta un mensaje al topic `dlq` cuando ocurre alguna de estas condiciones:

| Motivo del rechazo | Mensaje de error de ejemplo |
|---|---|
| El payload no es JSON válido | `Expecting property name enclosed in double quotes` |
| El campo `tipo` no es `pago_tarjeta`, `transferencia` ni `login` | `tipo desconocido: retiro_cajero` |
| Falta un campo obligatorio para ese tipo | `faltan campos: ['cuenta_origen', 'cuenta_destino']` |
| `monto` no es numérico | `monto invalido: abc` |
| `monto` es menor o igual a cero | `monto invalido: -500` |
| `ts_evento` parece estar en segundos en vez de milisegundos | `ts_evento invalido (debe ser epoch en milisegundos): 1762300000` |
| `exito` en login no es booleano | `exito debe ser true/false: si` |

Cada mensaje de la DLQ incluye el campo `mensaje` con el payload original para análisis posterior.

## Reglas de detección de fraude (topic `alertas-fraude`)

El Job 2 de Flink mantiene estado por clave de evento y aplica cuatro reglas de detección. Cada regla, al dispararse, produce una alerta en el topic `alertas-fraude` con los campos descritos en `docs/esquema-influxdb.md` (measurement `alertas`).

### MONTO_FUERA_DE_RANGO
Dispara cuando:
- Un pago con tarjeta supera 10.000.000 COP (umbral fijo), o
- Una transferencia supera 20.000.000 COP (umbral fijo), o
- El monto es más de 10 veces el promedio de esa tarjeta o cuenta, con al menos 5 transacciones previas registradas en el estado

### ALTA_FRECUENCIA
Dispara cuando una tarjeta hace más de 10 pagos en una ventana de 60 segundos. Patrón típico de prueba de tarjetas robadas.

### UBICACION_IMPOSIBLE
Dispara cuando la misma tarjeta se usa en dos ciudades distintas con menos de 10 minutos de diferencia. Indica clonación.

### FUERZA_BRUTA_LOGIN
Dispara cuando un mismo `id_cliente` tiene 5 o más intentos de login fallidos (`exito: false`) en una ventana de 2 minutos.

Las reglas se evalúan evento por evento, no por ventana, por lo que las alertas aparecen en segundos después de la condición disparadora.
"""
JOB 2 - Validacion, Dead Letter Queue (DLQ) y deteccion de fraude.

"""
import json
import os
import time

from pyflink.common import Configuration, Types, WatermarkStrategy
from pyflink.common.restart_strategy import RestartStrategies
from pyflink.common.serialization import SimpleStringSchema
from pyflink.datastream import StreamExecutionEnvironment, OutputTag
from pyflink.datastream.functions import ProcessFunction, KeyedProcessFunction, RuntimeContext
from pyflink.datastream.state import ValueStateDescriptor
from pyflink.datastream.connectors.kafka import (
    KafkaSource, KafkaSink, KafkaOffsetsInitializer, KafkaRecordSerializationSchema)

# ---------------------------------------------------------------- configuracion
BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "kafka:9092")
REINTENTOS = int(os.getenv("REINTENTOS", "5"))      
ESPERA_MS = int(os.getenv("ESPERA_MS", "10000"))    

# Campos obligatorios por tipo de evento 
OBLIGATORIOS = {
    "pago_tarjeta": ["event_id", "id_tarjeta", "monto", "ciudad", "ts_evento"],
    "transferencia": ["event_id", "cuenta_origen", "cuenta_destino", "monto", "ts_evento"],
    "login": ["event_id", "id_cliente", "exito", "ts_evento"],
}

CAMPO_CLAVE = {"pago_tarjeta": "id_tarjeta", "transferencia": "cuenta_origen", "login": "id_cliente"}

# Parametros de las reglas de fraude
UMBRAL_FIJO = {"pago_tarjeta": 10_000_000, "transferencia": 20_000_000}  
VECES_PROMEDIO = 10          
MIN_HISTORIAL = 3           
MAX_PAGOS_MINUTO = 10       
VENTANA_CIUDAD_MS = 600_000  
MAX_FALLOS_LOGIN = 5        
VENTANA_LOGIN_MS = 120_000

DLQ = OutputTag("dlq", Types.STRING())  # salida lateral para los mensajes invalidos


def ahora_ms():
    return int(time.time() * 1000)


def es_numero(x):
   
    return isinstance(x, (int, float)) and not isinstance(x, bool)


def sink(topic):
    """Crea un destino (sink) que escribe strings en un topic de Kafka."""
    return (KafkaSink.builder()
            .set_bootstrap_servers(BOOTSTRAP)
            .set_record_serializer(
                KafkaRecordSerializationSchema.builder()
                .set_topic(topic)
                .set_value_serialization_schema(SimpleStringSchema())
                .build())
            .build())


#  validacion
class Validar(ProcessFunction):
    """Revisa cada mensaje. Valido -> sigue. Invalido -> side output DLQ (el job NO se cae)."""

    def process_element(self, valor, ctx):
        try:
            ev = json.loads(valor)
            if not isinstance(ev, dict):
                raise ValueError("el mensaje no es un objeto JSON")
            tipo = ev.get("tipo")
            if tipo not in OBLIGATORIOS:
                raise ValueError(f"tipo desconocido: {tipo}")
            faltan = [c for c in OBLIGATORIOS[tipo] if c not in ev]
            if faltan:
                raise ValueError(f"faltan campos: {faltan}")
            if "monto" in OBLIGATORIOS[tipo] and (not es_numero(ev["monto"]) or ev["monto"] <= 0):
                raise ValueError(f"monto invalido: {ev['monto']}")
            if not es_numero(ev["ts_evento"]) or ev["ts_evento"] < 1_000_000_000_000:
                raise ValueError(f"ts_evento invalido (debe ser epoch en milisegundos): {ev['ts_evento']}")
            if tipo == "login" and not isinstance(ev["exito"], bool):
                raise ValueError(f"exito debe ser true/false: {ev['exito']}")
            ev["ts_validado"] = ahora_ms()
            yield json.dumps(ev, ensure_ascii=False)
        except Exception as e:  # cualquier problema -> DLQ con el motivo
            yield DLQ, json.dumps({"error": str(e), "mensaje": str(valor)[:500],
                                   "ts_error": ahora_ms()}, ensure_ascii=False)


def clave(valor):
    """Clave = 'tipo:id'. Ej: 'pago_tarjeta:T-00421'. Todos los eventos con la misma clave
    van a la misma instancia de DetectarFraude y comparten estado."""
    ev = json.loads(valor)
    return f"{ev['tipo']}:{ev[CAMPO_CLAVE[ev['tipo']]]}"


# ----------------------------------------------------- deteccion de fraude
ESTADO_INICIAL = '{"n": 0, "suma": 0, "ts": [], "ciudad": null, "ts_ciudad": 0, "fallos": []}'


class DetectarFraude(KeyedProcessFunction):
    """Evalua 4 reglas evento por evento usando un estado guardado por clave."""

    def open(self, runtime_context: RuntimeContext):
       
        self.estado = runtime_context.get_state(ValueStateDescriptor("estado", Types.STRING()))

    def process_element(self, valor, ctx):
        ev = json.loads(valor)
        st = json.loads(self.estado.value() or ESTADO_INICIAL)
        t = ev["ts_evento"]
        alertas = []

        # Regla 1 MONTO FUERA DE RANGO (pagos y transferencias)
        if ev["tipo"] in UMBRAL_FIJO:
            m = ev["monto"]
            prom = st["suma"] / st["n"] if st["n"] else 0
            if m > UMBRAL_FIJO[ev["tipo"]]:
                alertas.append(("MONTO_FUERA_DE_RANGO", f"monto {m:,.0f} supera el umbral {UMBRAL_FIJO[ev['tipo']]:,.0f}"))
            elif st["n"] >= MIN_HISTORIAL and m > VECES_PROMEDIO * prom:
                alertas.append(("MONTO_FUERA_DE_RANGO", f"monto {m:,.0f} es {m / prom:.0f} veces el promedio {prom:,.0f}"))
            st["n"] += 1
            st["suma"] += m

        if ev["tipo"] == "pago_tarjeta":
            #  ALTA FRECUENCIA 
            st["ts"] = [x for x in st["ts"] if t - x < 60_000] + [t]
            
            if len(st["ts"]) == MAX_PAGOS_MINUTO + 1:
                alertas.append(("ALTA_FRECUENCIA", f"{len(st['ts'])} pagos en 60 s"))
            #  UBICACION IMPOSIBLE
            if st["ciudad"] and ev["ciudad"] != st["ciudad"] and t - st["ts_ciudad"] < VENTANA_CIUDAD_MS:
                minutos = (t - st["ts_ciudad"]) / 60_000
                alertas.append(("UBICACION_IMPOSIBLE", f"{st['ciudad']} -> {ev['ciudad']} en {minutos:.1f} min"))
                
                st["ts_ciudad"] = t
            else:
                st["ciudad"], st["ts_ciudad"] = ev["ciudad"], t

        # FUERZA BRUTA EN LOGIN
        if ev["tipo"] == "login" and not ev["exito"]:
            st["fallos"] = [x for x in st["fallos"] if t - x < VENTANA_LOGIN_MS] + [t]
            if len(st["fallos"]) == MAX_FALLOS_LOGIN:  # una alerta por ataque
                alertas.append(("FUERZA_BRUTA_LOGIN", f"{len(st['fallos'])} intentos fallidos en 2 min"))

        self.estado.update(json.dumps(st))

        for tipo_alerta, detalle in alertas:
            yield json.dumps({
                "tipo_alerta": tipo_alerta,
                "tipo_evento": ev["tipo"],
                "clave": ctx.get_current_key(),
                "ciudad": ev.get("ciudad"),
                "monto": ev.get("monto"),
                "detalle": detalle,
                "event_id": ev["event_id"],
                "ts_evento": t,
                "ts_alerta": ahora_ms(),
            }, ensure_ascii=False)


# ----- 3. armado del job
def main():
    config = Configuration()
    
    config.set_string("python.state.cache-size", os.getenv("STATE_CACHE", "100000"))
    env = StreamExecutionEnvironment.get_execution_environment(config)
    env.set_parallelism(int(os.getenv("PARALELISMO", "2")))  # 2 copias de cada operador en paralelo
    env.enable_checkpointing(10_000)  # foto del estado cada 10 s
    env.set_restart_strategy(RestartStrategies.fixed_delay_restart(REINTENTOS, ESPERA_MS))

    fuente = (KafkaSource.builder()
              .set_bootstrap_servers(BOOTSTRAP)
              .set_topics("pagos-tarjeta", "transferencias", "logins")
              .set_group_id("flink-fraude")
              .set_starting_offsets(KafkaOffsetsInitializer.latest())
              .set_value_only_deserializer(SimpleStringSchema())
              .build())

    
    crudos = env.from_source(fuente, WatermarkStrategy.no_watermarks(), "entrada-kafka")

    validos = crudos.process(Validar(), output_type=Types.STRING()).name("validar")
    validos.get_side_output(DLQ).sink_to(sink("dlq")).name("sink-dlq")
    validos.sink_to(sink("eventos-validos")).name("sink-eventos-validos")

    alertas = (validos
               .key_by(clave, key_type=Types.STRING())
               .process(DetectarFraude(), output_type=Types.STRING())
               .name("detectar-fraude"))
    alertas.sink_to(sink("alertas-fraude")).name("sink-alertas")
    alertas.print() 

    env.execute("job2-fraude-dlq")


if __name__ == "__main__":
    main()
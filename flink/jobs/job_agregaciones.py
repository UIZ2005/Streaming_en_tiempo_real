"""
JOB 1 - Agregaciones con ventana TUMBLING de 1 minuto 
"""
import json
import os

from pyflink.common import Types, WatermarkStrategy
from pyflink.common.restart_strategy import RestartStrategies
from pyflink.common.time import Duration, Time
from pyflink.common.watermark_strategy import TimestampAssigner
from pyflink.common.serialization import SimpleStringSchema
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.window import TumblingEventTimeWindows
from pyflink.datastream.functions import ProcessWindowFunction
from pyflink.datastream.connectors.kafka import (
    KafkaSource, KafkaSink, KafkaOffsetsInitializer, KafkaRecordSerializationSchema)

BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "kafka:9092")
VENTANA_SEG = int(os.getenv("VENTANA_SEG", "60"))  
REINTENTOS = int(os.getenv("REINTENTOS", "5"))
ESPERA_MS = int(os.getenv("ESPERA_MS", "10000"))


def sink(topic):
    return (KafkaSink.builder()
            .set_bootstrap_servers(BOOTSTRAP)
            .set_record_serializer(
                KafkaRecordSerializationSchema.builder()
                .set_topic(topic)
                .set_value_serialization_schema(SimpleStringSchema())
                .build())
            .build())


class TsEvento(TimestampAssigner):
    """Le dice a Flink cual es la hora REAL del evento (event time): el campo ts_evento."""

    def extract_timestamp(self, value, record_timestamp):
        return int(json.loads(value)["ts_evento"])


def dimensiones(valor):
    
    ev = json.loads(valor)
    if ev["tipo"] == "login":
        return
    monto = float(ev["monto"])
    yield ("tipo:" + ev["tipo"], monto)
    if ev.get("ciudad"):
        yield ("ciudad:" + ev["ciudad"], monto)


class Agregar(ProcessWindowFunction):
    """Se ejecuta UNA vez por clave cuando la ventana se cierra."""

    def process(self, clave, context, elementos):
        montos = [m for _, m in elementos]
        dimension, valor = clave.split(":", 1)
        yield json.dumps({
            "dimension": dimension,               # "tipo" o "ciudad"
            "valor": valor,                       #  "Cali" o "pago_tarjeta"
            "conteo": len(montos),
            "monto_promedio": round(sum(montos) / len(montos), 2),
            "monto_maximo": max(montos),
            "monto_total": round(sum(montos), 2),
            "ventana_inicio": context.window().start,  
            "ventana_fin": context.window().end,
        }, ensure_ascii=False)


def main():
    env = StreamExecutionEnvironment.get_execution_environment()
    env.enable_checkpointing(10_000)
    env.set_restart_strategy(RestartStrategies.fixed_delay_restart(REINTENTOS, ESPERA_MS))

    fuente = (KafkaSource.builder()
              .set_bootstrap_servers(BOOTSTRAP)
              .set_topics("eventos-validos")
              .set_group_id("flink-agregaciones")
              .set_starting_offsets(KafkaOffsetsInitializer.latest())
              .set_value_only_deserializer(SimpleStringSchema())
              .build())

    marcas = (WatermarkStrategy
              .for_bounded_out_of_orderness(Duration.of_seconds(2))  
              .with_timestamp_assigner(TsEvento())
              .with_idleness(Duration.of_seconds(10)))            

    agregados = (env.from_source(fuente, marcas, "eventos-validos")
                 .flat_map(dimensiones, output_type=Types.TUPLE([Types.STRING(), Types.DOUBLE()]))
                 .name("dimensiones")
                 .key_by(lambda r: r[0], key_type=Types.STRING())
                 .window(TumblingEventTimeWindows.of(Time.seconds(VENTANA_SEG)))
                 .process(Agregar(), output_type=Types.STRING())
                 .name(f"ventana-tumbling-{VENTANA_SEG}s"))

    agregados.sink_to(sink("agregados-minuto")).name("sink-agregados")
    agregados.print()

    env.execute("job1-agregaciones")


if __name__ == "__main__":
    main()

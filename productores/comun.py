import json, os, time, uuid
from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable
from faker import Faker

fake = Faker("es_CO")
CIUDADES = ["Cali", "Bogotá", "Medellín", "Barranquilla",
            "Cartagena", "Bucaramanga", "Pereira", "Pasto"]


def crear_productor():
    """Reintenta hasta que Kafka responda."""
    for intento in range(30):
        try:
            return KafkaProducer(
                bootstrap_servers=os.getenv("KAFKA_BOOTSTRAP", "kafka:9092"),
                value_serializer=lambda v: v if isinstance(v, bytes)
                else json.dumps(v).encode("utf-8"),
                acks="all", retries=5, linger_ms=5)
        except NoBrokersAvailable:
            print(f"Kafka no disponible, reintento {intento + 1}/30", flush=True)
            time.sleep(2)
    raise SystemExit("No se pudo conectar a Kafka")


def ahora_ms():
    return int(time.time() * 1000)


def nuevo_id():
    return str(uuid.uuid4())


def correr_a_tasa(tasa, generar, enviar):
    """generar() devuelve (evento, etiqueta) con etiqueta: normal|fraude|malformado."""
    intervalo, siguiente = 1.0 / tasa, time.time()
    inicio = time.time()
    cont = {"normal": 0, "fraude": 0, "malformado": 0}
    while True:
        ev, etiqueta = generar()
        enviar(ev)
        cont[etiqueta] += 1
        siguiente += intervalo
        espera = siguiente - time.time()
        if espera > 0:
            time.sleep(espera)
        if time.time() - inicio >= 1:
            total = sum(cont.values())
            print(f"{time.strftime('%H:%M:%S')} tasa_real={total} ev/s | "
                  f"normal={cont['normal']} fraude={cont['fraude']} "
                  f"malformado={cont['malformado']}", flush=True)
            cont = {k: 0 for k in cont}
            inicio = time.time()


def ejecutar(topic, tasa, generar, campo_clave):
    """Envía a Kafka. La clave particiona por entidad (tarjeta, cuenta, cliente)."""
    prod = crear_productor()

    def enviar(ev):
        clave = None
        if isinstance(ev, dict):
            if "ts_evento" in ev:          # el timestamp se fija al enviar
                ev["ts_evento"] = ahora_ms()
            valor = ev.get(campo_clave)
            if isinstance(valor, str):
                clave = valor.encode()
        prod.send(topic, key=clave, value=ev)

    print(f"Productor -> {topic} a {tasa} ev/s", flush=True)
    correr_a_tasa(tasa, generar, enviar)
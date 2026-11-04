import os, random
from comun import ejecutar, ahora_ms, nuevo_id, CIUDADES

TASA = int(os.getenv("TASA", "200"))
P_FRAUDE = float(os.getenv("P_FRAUDE", "0.02"))
P_MALFORMADO = float(os.getenv("P_MALFORMADO", "0.01"))
N_CUENTAS = int(os.getenv("N_CUENTAS", "20000"))
CUENTAS = [f"AC-{i:05d}" for i in range(N_CUENTAS)]


def transferencia(monto=None):
    origen, destino = random.sample(CUENTAS, 2)
    if monto is None:
        monto = round(random.uniform(50_000, 3_000_000), -2)
    return {"event_id": nuevo_id(), "tipo": "transferencia",
            "cuenta_origen": origen, "cuenta_destino": destino,
            "monto": float(monto), "ciudad": random.choice(CIUDADES),
            "ts_evento": ahora_ms()}


def generar():
    r = random.random()
    if r < P_MALFORMADO:
        ev = random.choice([
            b"<<<basura>>>",
            {"event_id": nuevo_id(), "tipo": "transferencia", "cuenta_origen": "AC-00001",
             "monto": 5000, "ts_evento": ahora_ms()},               # falta cuenta_destino
            {**transferencia(), "monto": 0},                         # monto no positivo
            {**transferencia(), "monto": "mucho"},                   # monto no numérico
            {**transferencia(), "tipo": "transferencia_rara"}])      # tipo desconocido
        return ev, "malformado"
    if r < P_MALFORMADO + P_FRAUDE:   # transferencia de 25 a 80 millones
        return transferencia(monto=round(random.uniform(25e6, 80e6), -3)), "fraude"
    return transferencia(), "normal"


if __name__ == "__main__":
    ejecutar("transferencias", TASA, generar, "cuenta_origen")
import os, random
from comun import ejecutar, ahora_ms, nuevo_id, fake, CIUDADES

TASA = int(os.getenv("TASA", "200"))
P_FRAUDE = float(os.getenv("P_FRAUDE", "0.01"))        # prob. de INICIAR un caso de fraude
P_MALFORMADO = float(os.getenv("P_MALFORMADO", "0.01"))
N_TARJETAS = int(os.getenv("N_TARJETAS", "20000"))

TARJETAS = [{"id_tarjeta": f"T-{i:05d}", "id_cliente": f"C-{i:05d}",
            "ciudad": random.choice(CIUDADES),
            "gasto_medio": random.choice([30000, 80000, 150000, 300000])}
            for i in range(N_TARJETAS)]
pendientes = []  # eventos ya decididos (ráfagas, segunda ciudad)


def pago(t, monto=None, ciudad=None):
    if monto is None:
        monto = max(1000, random.gauss(t["gasto_medio"], t["gasto_medio"] * 0.3))
    return {"event_id": nuevo_id(), "tipo": "pago_tarjeta",
            "id_tarjeta": t["id_tarjeta"], "id_cliente": t["id_cliente"],
            "monto": round(monto, 2), "moneda": "COP", "comercio": fake.company(),
            "ciudad": ciudad or t["ciudad"], "ts_evento": ahora_ms()}


def generar():
    if pendientes:
        return pendientes.pop(0)
    t, r = random.choice(TARJETAS), random.random()

    if r < P_MALFORMADO:  # casos para la DLQ
        ev = random.choice([
            b"{esto no es json",
            {"event_id": nuevo_id(), "tipo": "pago_tarjeta", "monto": "abc"},
            {"event_id": nuevo_id(), "tipo": "pago_tarjeta",
                "id_tarjeta": t["id_tarjeta"], "id_cliente": t["id_cliente"],
                "monto": -500, "moneda": "COP", "comercio": "X",
                "ciudad": "Cali", "ts_evento": ahora_ms()},
            {"event_id": nuevo_id(), "tipo": "pago_desconocido",
                "id_tarjeta": t["id_tarjeta"], "monto": 1000, "ts_evento": ahora_ms()}])
        return ev, "malformado"

    if r < P_MALFORMADO + P_FRAUDE:  # casos de fraude
        caso = random.choice(["monto", "rafaga", "ciudad"])
        if caso == "monto":      # monto 20-60 veces el habitual
            return pago(t, monto=t["gasto_medio"] * random.randint(20, 60)), "fraude"
        if caso == "rafaga":     # 12 compras seguidas de la misma tarjeta
            pendientes.extend((pago(t), "fraude") for _ in range(11))
            return pago(t), "fraude"
        # ciudad imposible: dos ciudades distintas en segundos
        otra = random.choice([c for c in CIUDADES if c != t["ciudad"]])
        pendientes.append((pago(t, ciudad=otra), "fraude"))
        return pago(t), "fraude"

    return pago(t), "normal"


if __name__ == "__main__":
    ejecutar("pagos-tarjeta", TASA, generar, "id_tarjeta")
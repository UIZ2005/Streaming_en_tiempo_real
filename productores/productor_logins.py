import os, random
from comun import ejecutar, ahora_ms, nuevo_id, fake, CIUDADES

TASA = int(os.getenv("TASA", "200"))
P_FRAUDE = float(os.getenv("P_FRAUDE", "0.005"))   # cada caso son 6 fallos seguidos
P_MALFORMADO = float(os.getenv("P_MALFORMADO", "0.01"))
P_FALLO = 0.05
N_CLIENTES = int(os.getenv("N_CLIENTES", "20000"))
CLIENTES = [f"C-{i:05d}" for i in range(N_CLIENTES)]
pendientes = []


def login(cliente, exito, ip=None, ciudad=None):
    return {"event_id": nuevo_id(), "tipo": "login", "id_cliente": cliente,
            "exito": exito, "ip": ip or fake.ipv4_public(),
            "ciudad": ciudad or random.choice(CIUDADES), "ts_evento": ahora_ms()}


def generar():
    if pendientes:
        return pendientes.pop(0)
    c, r = random.choice(CLIENTES), random.random()

    if r < P_MALFORMADO:
        ev = random.choice([
            b"{\"tipo\": \"login\"",                                 # JSON cortado
            {"event_id": nuevo_id(), "tipo": "login", "id_cliente": c,
                "ip": "1.2.3.4", "ts_evento": ahora_ms()},              # falta exito
            {**login(c, True), "tipo": "logout_raro"}])              # tipo desconocido
        return ev, "malformado"

    if r < P_MALFORMADO + P_FRAUDE:   # 6 fallos seguidos, misma IP
        ip, ciudad = fake.ipv4_public(), random.choice(CIUDADES)
        pendientes.extend((login(c, False, ip, ciudad), "fraude") for _ in range(5))
        return login(c, False, ip, ciudad), "fraude"

    return login(c, random.random() >= P_FALLO), "normal"


if __name__ == "__main__":
    ejecutar("logins", TASA, generar, "id_cliente")
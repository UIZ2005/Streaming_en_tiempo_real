"""
Lee latencia.csv y genera 3 graficas PNG para el reporte de rendimiento.
"""
import matplotlib.pyplot as plt
import pandas as pd
import os

ARCHIVO = "latencia.csv"
SALIDA = "../docs/graficas"

os.makedirs(SALIDA, exist_ok=True)

df = pd.read_csv(ARCHIVO)
df["indice"] = range(len(df))

# Grafica 1: latencia en el tiempo (promedio, p95 y max)
fig, ax = plt.subplots(figsize=(10, 5))
ax.plot(df["indice"], df["promedio_ms"], label="Promedio", linewidth=2)
ax.plot(df["indice"], df["p95_ms"], label="Percentil 95", linewidth=2, linestyle="--")
ax.plot(df["indice"], df["max_ms"], label="Maximo", linewidth=1, alpha=0.6)
ax.axhline(3000, color="red", linestyle=":", label="Objetivo R2.7 (3 s)")
ax.axhline(5000, color="orange", linestyle=":", label="Objetivo R2.6 (5 s)")
ax.set_xlabel("Muestras (cada 5 segundos)")
ax.set_ylabel("Latencia (ms)")
ax.set_title("Latencia end-to-end a lo largo del tiempo")
ax.legend()
ax.grid(True, alpha=0.3)
fig.tight_layout()
fig.savefig(f"{SALIDA}/latencia_en_tiempo.png", dpi=150)
print("Guardada latencia_en_tiempo.png")

# Grafica 2: distribucion de latencia (histograma del promedio)
fig, ax = plt.subplots(figsize=(10, 5))
ax.hist(df["promedio_ms"], bins=30, color="steelblue", edgecolor="black", alpha=0.8)
ax.axvline(df["promedio_ms"].mean(), color="red", linestyle="--", linewidth=2, label=f"Media: {df['promedio_ms'].mean():.0f} ms")
ax.set_xlabel("Latencia promedio por ventana (ms)")
ax.set_ylabel("Frecuencia")
ax.set_title("Distribucion de la latencia promedio")
ax.legend()
ax.grid(True, alpha=0.3)
fig.tight_layout()
fig.savefig(f"{SALIDA}/distribucion_latencia.png", dpi=150)
print("Guardada distribucion_latencia.png")

# Grafica 3: throughput (eventos por ventana de 10 s)
fig, ax = plt.subplots(figsize=(10, 5))
# Convertimos n_eventos en eventos por segundo (ventana de 10s)
df["throughput_eps"] = df["n_eventos"] / 10
ax.plot(df["indice"], df["throughput_eps"], color="green", linewidth=2)
ax.fill_between(df["indice"], df["throughput_eps"], alpha=0.3, color="green")
ax.set_xlabel("Muestras (cada 5 segundos)")
ax.set_ylabel("Throughput (eventos/segundo)")
ax.set_title("Throughput medido en InfluxDB")
ax.grid(True, alpha=0.3)
fig.tight_layout()
fig.savefig(f"{SALIDA}/throughput.png", dpi=150)
print("Guardada throughput.png")

# Resumen estadistico
print("\n=== RESUMEN ===")
print(f"Total muestras: {len(df)}")
print(f"Latencia promedio global: {df['promedio_ms'].mean():.1f} ms")
print(f"Latencia p95 maximo:      {df['p95_ms'].max():.1f} ms")
print(f"Latencia maxima absoluta: {df['max_ms'].max():.1f} ms")
print(f"Throughput promedio:      {df['throughput_eps'].mean():.1f} eventos/s")
print(f"Throughput maximo:        {df['throughput_eps'].max():.1f} eventos/s")
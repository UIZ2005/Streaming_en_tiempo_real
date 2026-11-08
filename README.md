# Streaming_en_tiempo_real

# Pipeline de streaming financiero: detección de fraude en tiempo real

Kafka + Flink + Telegraf + InfluxDB + Grafana, todo con Docker Compose.

## Requisitos
- Docker Desktop con al menos 8 GB de RAM asignados
- Git

## Instalación y arranque
```bash
git clone <URL-DEL-REPO>
cd <carpeta>
cp .env.example .env     # edita los valores
docker compose up -d --build
```

## Servicios y URLs
| Servicio | URL |
|---|---|
| Kafka UI | http://localhost:8080 |
| Flink | http://localhost:8081 |
| Grafana | http://localhost:3000 (admin / admin) |
| InfluxDB | http://localhost:8086 |

## Comandos útiles
- Estado: `docker compose ps`
- Logs: `docker compose logs -f <servicio>`
- Apagar y borrar datos: `docker compose down -v`

## Arquitectura
Productores → Kafka → Flink → Kafka → Telegraf → InfluxDB → Grafana

(Aquí va el diagrama)

## Topics de Kafka
(Tabla de los 7 topics, quién escribe y quién lee)
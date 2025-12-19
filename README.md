# IoT Streaming Pipeline (Kafka → Spark → Postgres → Grafana)

## Overview

This project simulates IoT sensor readings (temperature/humidity), streams them through Kafka, processes them in real time using Spark Structured Streaming, stores both raw + aggregated data in PostgreSQL, and visualizes the results in Grafana.

### Data Flow

Producer → Kafka topic `iot_sensors` → Spark streaming job → PostgreSQL tables → Grafana dashboard

---

## Tech Stack

- **Kafka**: message broker (stream ingestion)
- **Spark Structured Streaming**: real-time processing + aggregations
- **PostgreSQL**: storage (raw + aggregate + alerts)
- **Grafana**: visualization dashboards

---

## Services

- `kafka` (broker)
- `producer` (Python KafkaProducer generates sensor readings)
- `spark` (PySpark streaming job reads Kafka + writes to Postgres)
- `iot_postgres` (database)
- `iot_grafana` (dashboard)

---

## Database Tables

- `raw_readings`: every reading (timestamp, device_id, temperature, humidity)
- `agg_readings_minute`: per-minute aggregates by device (avg temp/humidity, count)
- `alerts`: events that exceed a temperature threshold

---

## Prerequisites

- Docker + Docker Compose installed
- Recommended: WSL2 (Windows) or Linux/macOS

---

## Quick Start (Run Everything)

```bash
docker compose -f docker-compose.yml -f docker-compose.storage.yml up -d --build
```

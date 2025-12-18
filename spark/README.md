# Spark Structured Streaming - IoT Pipeline

This document provides complete instructions for running the Spark Structured Streaming application that processes IoT sensor data from Kafka.

## Table of Contents

1. [Overview](#overview)
2. [Architecture](#architecture)
3. [Prerequisites](#prerequisites)
4. [Setup Instructions](#setup-instructions)
5. [Running the Project](#running-the-project)
6. [Verification and Screenshots](#verification-and-screenshots)
7. [Understanding the Output](#understanding-the-output)
8. [Configuration](#configuration)
9. [Troubleshooting](#troubleshooting)
10. [Code Overview](#code-overview)
11. [Additional Resources](#additional-resources)

## Overview

The Spark application (`spark_stream.py`) reads JSON messages from a Kafka topic (`iot_sensors`), parses the sensor data, and outputs the processed results to the console using Structured Streaming.

### Data Flow

```
Kafka Topic (iot_sensors) → Spark Structured Streaming → Console Output
```

### Message Format

The application expects JSON messages with the following schema:

```json
{
  "sensor_id": "sensor-1",
  "temperature": 25.43,
  "humidity": 65.21,
  "timestamp": "2025-12-18T12:00:00"
}
```

## Architecture

- **Source**: Kafka topic `iot_sensors` on broker `kafka:9092`
- **Processing**: Spark Structured Streaming with JSON parsing
- **Output**: Console (stdout) with formatted table output
- **Checkpointing**: `/tmp/spark-checkpoint` (for fault tolerance)

## Prerequisites

1. **Docker Desktop** installed and running
2. **Docker Compose** (included with Docker Desktop)
3. **PowerShell** (Windows default shell)
4. The Spark image must be built (see Setup below)

## Setup Instructions

### Step 1: Build the Spark Image

The Spark image includes pre-bundled Kafka connector JARs to avoid runtime dependency issues.

```powershell
docker build -f Dockerfile.spark -t spark-kafka:3.5.1 .
```

**Expected Output:**
```
Successfully built <image-id>
Successfully tagged spark-kafka:3.5.1
```

### Step 2: Start All Services

Ensure the full pipeline is running:

```powershell
docker-compose up -d
```

### Step 3: Verify Services Status

Check all services are up and running:

```powershell
docker-compose ps
```

**Expected Output:**
```
NAME       IMAGE                STATUS
kafka      apache/kafka:3.7.0   Up (healthy)
producer   python:3.9-slim      Up
spark      spark-kafka:3.5.1    Up
```

<img width="842" height="99" alt="image" src="https://github.com/user-attachments/assets/2dad528d-4a5a-4e26-a6eb-2debe9a0a85b" />


### Step 4: Verify Kafka Topic Exists

```powershell
docker exec kafka /opt/kafka/bin/kafka-topics.sh --list --bootstrap-server localhost:9092
```

**Expected Output:**
```
iot_sensors
```

If the topic doesn't exist, create it:

```powershell
docker exec kafka /opt/kafka/bin/kafka-topics.sh --create --topic iot_sensors --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1
```

## Running the Project

### Quick Start

1. **Build Spark image:**
   ```powershell
   docker build -f Dockerfile.spark -t spark-kafka:3.5.1 .
   ```

2. **Start all services:**
   ```powershell
   docker-compose up -d
   ```

3. **View Spark output:**
   ```powershell
   docker-compose logs -f spark
   ```

4. **Stop all services:**
   ```powershell
   docker-compose down
   ```

### Running Only Spark

If Kafka and Producer are already running:

```powershell
docker-compose up -d spark
docker-compose logs -f spark
```

### Restarting Spark

```powershell
docker-compose restart spark
docker-compose logs -f spark
```

## Verification and Screenshots

### 1. View Spark Streaming Output

The main Spark streaming output shows real-time processing of IoT sensor data.

**Command:**
```powershell
docker-compose logs -f spark
```

**What to observe:**
- Streaming batch output showing sensor data
- Table format with columns: `sensor_id`, `temperature`, `humidity`, `event_time`
- Multiple batches processing sequentially

**Example Output:**
```
Batch: 45
+---------+-----------+--------+--------------------+
|sensor_id|temperature|humidity|         event_time|
+---------+-----------+--------+--------------------+
| sensor-1|      26.62|   71.07|2025-12-18 00:45:...|
+---------+-----------+--------+--------------------+

Batch: 46
+---------+-----------+--------+--------------------+
|sensor_id|temperature|humidity|         event_time|
+---------+-----------+--------+--------------------+
| sensor-2|      28.76|   52.52|2025-12-18 00:45:...|
+---------+-----------+--------+--------------------+
```

**Note:** Let it run for 10-15 seconds to capture multiple batches. Press `Ctrl+C` to stop following logs.

![Spark Streaming Output](image.png)

### 2. View Producer Logs

Verify the data source is sending messages correctly.

**Command:**
```powershell
docker-compose logs producer --tail 20
```

**What to observe:**
- Producer sending messages every second
- JSON message format with sensor_id, temperature, humidity, timestamp

**Example Output:**
```
producer  | Sent: {'sensor_id': 'sensor-1', 'temperature': 25.43, 'humidity': 65.21, 'timestamp': '2025-12-18T12:00:00.123456'}
producer  | Sent: {'sensor_id': 'sensor-2', 'temperature': 28.15, 'humidity': 66.13, 'timestamp': '2025-12-18T12:00:01.234567'}
```

![Producer Output](image.png)

### 3. Verify Kafka Topic Messages

Check raw messages in the Kafka topic to validate data format.

**Command:**
```powershell
docker exec kafka /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic iot_sensors --from-beginning --max-messages 5
```

**What to observe:**
- Raw JSON messages from Kafka topic
- Message format validation

**Example Output:**
```
{"sensor_id": "sensor-1", "temperature": 25.43, "humidity": 65.21, "timestamp": "2025-12-18T12:00:00"}
{"sensor_id": "sensor-2", "temperature": 28.15, "humidity": 66.13, "timestamp": "2025-12-18T12:00:01"}
```

**Note:** This will consume 5 messages and exit automatically. Press `Ctrl+C` to stop earlier.

![Kafka Messages](image.png)

### 4. View Complete Spark Application Logs

Review the complete Spark application lifecycle and initialization.

**Command:**
```powershell
docker-compose logs spark --tail 100
```

**What to observe:**
- Spark initialization messages
- Streaming query start
- Multiple batch processing outputs
- Clean formatted table output

![Complete Spark Logs](image.png)

### 5. Monitor Real-time Streaming

Filtered view of streaming data for focused monitoring.

**Command:**
```powershell
docker-compose logs -f spark | Select-String -Pattern "Batch|sensor|temperature|humidity" -Context 0,3
```

**What to observe:**
- Real-time streaming as messages are processed
- Filtered output showing only relevant data
- Continuous batch processing

![Filtered Streaming Output](image.png)

## Understanding the Output

### Batch Processing

Spark processes messages in micro-batches. Each batch shows:
- Batch number (e.g., `Batch: 45`)
- Formatted table with sensor data
- All messages received in that batch interval

### Output Columns

- **sensor_id**: Identifier of the sensor (e.g., "sensor-1", "sensor-2", "sensor-3")
- **temperature**: Temperature reading in Celsius (Double)
- **humidity**: Humidity percentage (Double)
- **event_time**: Parsed timestamp from ISO string (Timestamp)

### Stream Processing Behavior

- Spark reads from Kafka using `startingOffsets: latest` (only new messages)
- Uses append mode (shows only new data, not aggregated)
- Checkpointing enabled for fault tolerance at `/tmp/spark-checkpoint`

## Configuration

### Spark Configuration

Located in `docker-compose.yml`:

- **Master**: `local[*]` (uses all available cores)
- **Checkpoint Location**: `/tmp/spark-checkpoint`
- **Event Log**: Disabled (to avoid permission issues)
- **Adaptive Execution**: Enabled
- **Serializer**: KryoSerializer (faster serialization)

### Kafka Connection Settings

Located in `spark_stream.py`:

- **Bootstrap Servers**: `kafka:9092`
- **Topic**: `iot_sensors`
- **Starting Offset**: `latest` (only processes new messages)
- **Fail on Data Loss**: `false` (continues even if some data is lost)

## Troubleshooting

### Spark Not Processing Messages

**Check if Spark is running:**
```powershell
docker-compose ps spark
```

**Check Spark logs for errors:**
```powershell
docker-compose logs spark --tail 50
```

**Verify Kafka connection:**
```powershell
docker exec spark ping -c 2 kafka
```

### No Output in Spark Logs

1. **Verify producer is sending messages:**
   ```powershell
   docker-compose logs producer --tail 10
   ```

2. **Check Kafka topic has messages:**
   ```powershell
   docker exec kafka /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic iot_sensors --from-beginning --max-messages 1
   ```

3. **Restart Spark:**
   ```powershell
   docker-compose restart spark
   ```

### ClassNotFoundException Errors

If you see class not found errors, rebuild the Spark image:

```powershell
docker build -f Dockerfile.spark -t spark-kafka:3.5.1 .
docker-compose up -d --force-recreate spark
```

### Checkpoint Permission Errors

The entrypoint script handles permissions automatically. If issues persist:

```powershell
docker-compose down -v
docker-compose up -d
```

This will recreate volumes with proper permissions.

## Code Overview

### Key Components

**1. Schema Definition:**
```python
schema = StructType() \
    .add("sensor_id", StringType()) \
    .add("temperature", DoubleType()) \
    .add("humidity", DoubleType()) \
    .add("timestamp", StringType())
```

**2. Kafka Source:**
```python
df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:9092") \
    .option("subscribe", "iot_sensors") \
    .load()
```

**3. JSON Parsing:**
```python
parsed = df.selectExpr("CAST(value AS STRING) AS json") \
    .select(from_json(col("json"), schema).alias("data")) \
    .select(
        col("data.sensor_id"),
        col("data.temperature"),
        col("data.humidity"),
        to_timestamp(col("data.timestamp")).alias("event_time")
    )
```

**4. Console Sink:**
```python
query = parsed.writeStream \
    .format("console") \
    .outputMode("append") \
    .start()
```

## Additional Resources

- [Spark Structured Streaming Guide](https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html)
- [Kafka Integration](https://spark.apache.org/docs/latest/structured-streaming-kafka-integration.html)
- [Docker Compose Documentation](https://docs.docker.com/compose/)

## Learning Points

1. **Structured Streaming**: Real-time data processing with Spark
2. **Kafka Integration**: Reading from Kafka topics
3. **JSON Parsing**: Converting JSON strings to structured data
4. **Schema Definition**: Type-safe data processing
5. **Checkpointing**: Fault tolerance in streaming applications
6. **Docker Compose**: Orchestrating multi-container applications

---

**Note**: This setup is optimized for Windows Docker environment and avoids common issues like Ivy cache errors, NativeIO errors, and permission problems by pre-bundling dependencies and using Docker volumes.

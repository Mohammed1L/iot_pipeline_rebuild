from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    from_json, col, to_timestamp, window, avg, count, lit
)
from pyspark.sql.types import StructType, StringType, DoubleType

import os
import psycopg2
from psycopg2.extras import execute_values


# -------------------------
# Spark session
# -------------------------
spark = (
    SparkSession.builder
    .appName("IoT Streaming")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")


# -------------------------
# Schema
# -------------------------
schema = (
    StructType()
    .add("sensor_id", StringType())
    .add("temperature", DoubleType())
    .add("humidity", DoubleType())
    .add("timestamp", StringType())
)


# -------------------------
# Kafka source
# -------------------------
df = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "kafka:9092")
    .option("subscribe", "iot_sensors")
    .option("startingOffsets", "latest")
    .option("failOnDataLoss", "false")
    .load()
)


parsed = (
    df.selectExpr("CAST(value AS STRING) AS json")
      .select(from_json(col("json"), schema).alias("data"))
      .select(
          col("data.sensor_id").alias("sensor_id"),
          col("data.temperature").alias("temperature"),
          col("data.humidity").alias("humidity"),
          to_timestamp(col("data.timestamp")).alias("event_time")
      )
      .filter(col("event_time").isNotNull())
)


# -------------------------
# Postgres config
# -------------------------
JDBC_URL = "jdbc:postgresql://iot_postgres:5432/iotdb"
JDBC_PROPS = {
    "user": "iot",
    "password": "iotpass",
    "driver": "org.postgresql.Driver"
}

PG_HOST = "iot_postgres"
PG_PORT = 5432
PG_DB   = "iotdb"
PG_USER = "iot"
PG_PASS = "iotpass"


def pg_connect():
    return psycopg2.connect(
        host=PG_HOST,
        port=PG_PORT,
        dbname=PG_DB,
        user=PG_USER,
        password=PG_PASS,
    )


def upsert_agg_rows(rows):
    """
    rows: list of tuples
      (window_start, window_end, device_id, avg_temp, avg_humidity, cnt)
    """
    if not rows:
        return

    conn = pg_connect()
    conn.autocommit = True
    cur = conn.cursor()

    sql = """
        INSERT INTO agg_readings_minute
            (window_start, window_end, device_id, avg_temp, avg_humidity, cnt)
        VALUES %s
        ON CONFLICT (window_start, device_id)
        DO UPDATE SET
            window_end   = EXCLUDED.window_end,
            avg_temp     = EXCLUDED.avg_temp,
            avg_humidity = EXCLUDED.avg_humidity,
            cnt          = EXCLUDED.cnt;
    """

    execute_values(cur, sql, rows, page_size=1000)

    cur.close()
    conn.close()


def write_batch(batch_df, batch_id):
    # If a batch is empty, skip everything
    if batch_df.rdd.isEmpty():
        return

    # 1) RAW -> raw_readings (append is fine)
    raw_out = (
        batch_df
        .select(
            col("event_time").alias("ts"),
            col("sensor_id").alias("device_id"),
            col("temperature"),
            col("humidity")
        )
    )

    (raw_out.write
        .format("jdbc")
        .option("url", JDBC_URL)
        .option("dbtable", "raw_readings")
        .option("user", JDBC_PROPS["user"])
        .option("password", JDBC_PROPS["password"])
        .option("driver", JDBC_PROPS["driver"])
        .mode("append")
        .save()
    )

    # 2) AGG (per minute) -> agg_readings_minute (UPSERT to avoid duplicate PK crash)
    agg_out = (
        batch_df
        .groupBy(window(col("event_time"), "1 minute").alias("w"), col("sensor_id"))
        .agg(
            avg("temperature").alias("avg_temp"),
            avg("humidity").alias("avg_humidity"),
            count("*").alias("cnt"),
        )
        .select(
            col("w.start").alias("window_start"),
            col("w.end").alias("window_end"),
            col("sensor_id").alias("device_id"),
            col("avg_temp"),
            col("avg_humidity"),
            col("cnt")
        )
    )

    # Collect is OK for small-ish agg per batch (IoT demo scenario).
    # If you expect huge agg output, we can rewrite to partition-wise upsert.
    rows = [
        (
            r["window_start"],
            r["window_end"],
            r["device_id"],
            float(r["avg_temp"]) if r["avg_temp"] is not None else None,
            float(r["avg_humidity"]) if r["avg_humidity"] is not None else None,
            int(r["cnt"]) if r["cnt"] is not None else 0
        )
        for r in agg_out.collect()
    ]
    upsert_agg_rows(rows)

    # 3) ALERTS -> alerts (append is fine)
    THRESH = 32.0
    alerts_out = (
        batch_df
        .filter(col("temperature") > lit(THRESH))
        .select(
            col("event_time").alias("ts"),
            col("sensor_id").alias("device_id"),
            lit("temp_gt_threshold").alias("rule"),
            col("temperature").alias("value"),
            lit(THRESH).alias("threshold")
        )
    )

    (alerts_out.write
        .format("jdbc")
        .option("url", JDBC_URL)
        .option("dbtable", "alerts")
        .option("user", JDBC_PROPS["user"])
        .option("password", JDBC_PROPS["password"])
        .option("driver", JDBC_PROPS["driver"])
        .mode("append")
        .save()
    )


# -------------------------
# Stream start (add checkpoint!)
# -------------------------
checkpoint_dir = os.environ.get("SPARK_CHECKPOINT_DIR", "/tmp/checkpoints/iot_stream")

query = (
    parsed.writeStream
    .foreachBatch(write_batch)
    .outputMode("append")
    .option("checkpointLocation", checkpoint_dir)
    .start()
)

query.awaitTermination()

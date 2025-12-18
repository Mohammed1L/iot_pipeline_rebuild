from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, to_timestamp
from pyspark.sql.types import StructType, StringType, DoubleType

spark = (
    SparkSession.builder
    .appName("IoT Streaming")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

schema = StructType() \
    .add("sensor_id", StringType()) \
    .add("temperature", DoubleType()) \
    .add("humidity", DoubleType()) \
    .add("timestamp", StringType())

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
          col("data.sensor_id"),
          col("data.temperature"),
          col("data.humidity"),
          to_timestamp(col("data.timestamp")).alias("event_time")
      )
)

query = (
    parsed.writeStream
    .format("console")
    .outputMode("append")
    .start()
)

query.awaitTermination()

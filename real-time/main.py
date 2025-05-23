import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, current_timestamp
from pyspark.sql.types import StructType, StringType, DoubleType, TimestampType

import config

# 1. Spark session para streaming
spark = SparkSession.builder \
    .appName("Stream Realtime Delivery") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,org.postgresql:postgresql:42.7.5") \
    .config("spark.hadoop.hadoop.native.lib", "false") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# === STREAM 1: UBICACIONES DE REPARTIDORES ===

# Esquema del mensaje de ubicación
esquema_ubicacion = StructType() \
    .add("repartidor_id", StringType()) \
    .add("latitud", DoubleType()) \
    .add("longitud", DoubleType()) \
    .add("timestamp", TimestampType())

# Lectura desde Kafka
kafka_bootstrap_servers = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
kafka_topic_ubicaciones = os.environ.get("KAFKA_TOPIC_UBICACIONES", "ubicaciones_repartidores")

df_kafka_ubicaciones = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", kafka_bootstrap_servers) \
    .option("subscribe", kafka_topic_ubicaciones) \
    .option("startingOffsets", "latest") \
    .load()

# Parseo de ubicaciones
df_ubicaciones = df_kafka_ubicaciones.selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), esquema_ubicacion).alias("data")) \
    .select("data.*") \
    .withColumn("ingestion_time", current_timestamp())


# Escribir ubicaciones a PostgreSQL (modo upsert)
def escribir_ubicaciones(batch_df, batch_id):
    url = f"jdbc:postgresql://{config.DATABASE_HOST}:5432/{config.DATABASE_NAME}"
    batch_df.write \
        .format("jdbc") \
        .option("url", url) \
        .option("dbtable", "ubicaciones_realtime") \
        .option("user", config.DATABASE_USER) \
        .option("password", config.DATABASE_PASSWORD) \
        .mode("append") \
        .save()


query_ubicaciones = df_ubicaciones.writeStream \
    .foreachBatch(escribir_ubicaciones) \
    .outputMode("update") \
    .option("checkpointLocation", "checkpoints/ubicaciones") \
    .start()

# === STREAM 2: ESTADO DE PEDIDOS ===

# Esquema del mensaje de estado de pedido
esquema_estado = StructType() \
    .add("pedido_id", StringType()) \
    .add("estado", StringType()) \
    .add("timestamp", TimestampType())

kafka_topic_estados = os.environ.get("KAFKA_TOPIC_ESTADOS", "estados_pedidos")

df_kafka_estados = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", kafka_bootstrap_servers) \
    .option("subscribe", kafka_topic_estados) \
    .option("startingOffsets", "latest") \
    .load()

# Parseo de estados
df_estados = df_kafka_estados.selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), esquema_estado).alias("data")) \
    .select("data.*") \
    .withColumn("ingestion_time", current_timestamp())


# Escribir estados a PostgreSQL (modo upsert)
def escribir_estados(batch_df, batch_id):
    url = f"jdbc:postgresql://{config.DATABASE_HOST}:5432/{config.DATABASE_NAME}"
    batch_df.write \
        .format("jdbc") \
        .option("url", url) \
        .option("dbtable", "estado_pedidos_realtime") \
        .option("user", config.DATABASE_USER) \
        .option("password", config.DATABASE_PASSWORD) \
        .mode("append") \
        .save()


query_estados = df_estados.writeStream \
    .foreachBatch(escribir_estados) \
    .outputMode("update") \
    .option("checkpointLocation", "checkpoints/estados") \
    .start()

# Esperar ambos streams
query_ubicaciones.awaitTermination()
query_estados.awaitTermination()

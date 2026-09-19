#!/usr/bin/env python
# coding: utf-8

try:
    import notebookutils
except ImportError:
    notebookutils = None

from delta.tables import DeltaTable
from pyspark.sql import SparkSession, functions as F
from pyspark.sql.functions import col
from pyspark.sql.types import (
    BooleanType,
    DecimalType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)


VARIABLE_LIBRARY_NAME = "Variables_1"
TRIGGER_INTERVAL = "10 seconds"

WORKSPACE_ID = "198ac056-e709-4dd6-bcfe-9ad6550ea321"
BRONZE_LAKEHOUSE_ID = "5a3aca47-3afc-4d35-b7be-8a5f1c59ace4"
SILVER_LAKEHOUSE_ID = "e31b1836-a85b-41f5-9285-7a1b50c90046"
CHECKPOINT_LAKEHOUSE_ID = "6bb65a11-d438-4fd0-b737-18b12947cead"

ONELAKE_ROOT = (
    f"abfss://{WORKSPACE_ID}@onelake.dfs.fabric.microsoft.com"
)
BRONZE_TABLE_PATH = (
    f"{ONELAKE_ROOT}/{BRONZE_LAKEHOUSE_ID}/Tables/dbo/raw_transactions"
)
SILVER_TABLE_PATH = (
    f"{ONELAKE_ROOT}/{SILVER_LAKEHOUSE_ID}/Tables/dbo/silver_transactions"
)
BRONZE_CHECKPOINT_PATH = (
    f"{ONELAKE_ROOT}/{CHECKPOINT_LAKEHOUSE_ID}"
    "/Files/finguard/bronze/source/raw_transactions/checkpoint_v1"
)
SILVER_CHECKPOINT_PATH = (
    f"{ONELAKE_ROOT}/{CHECKPOINT_LAKEHOUSE_ID}"
    "/Files/finguard/silver/source/raw_transactions/checkpoint_v1"
)

# Applied only when the Bronze checkpoint doesn't already contain offsets.
KAFKA_STARTING_OFFSETS = "earliest"

spark = SparkSession.builder.getOrCreate()


TRANSACTION_SCHEMA = StructType([
    StructField("transaction_id", StringType(), True),
    StructField("customer_id", StringType(), True),
    StructField("card_number", StringType(), True),
    StructField("merchant_id", StringType(), True),
    StructField("merchant_name", StringType(), True),
    StructField("merchant_category", StringType(), True),
    StructField("amount", DecimalType(18, 2), True),
    StructField("currency", StringType(), True),
    StructField("transaction_type", StringType(), True),
    StructField("payment_channel", StringType(), True),
    StructField("device_id", StringType(), True),
    StructField("city", StringType(), True),
    StructField("country", StringType(), True),
    StructField("transaction_timestamp", TimestampType(), True),
    StructField("is_international", BooleanType(), True),
    StructField("status", StringType(), True),
])


def load_kafka_config():
    if notebookutils is None:
        raise RuntimeError(
            "NotebookUtils is unavailable in this Spark Job Definition runtime. "
            "Pass the Kafka settings as job arguments or retrieve them from "
            "Azure Key Vault instead of using Fabric Variable Library."
        )

    try:
        variable_library = notebookutils.variableLibrary.getLibrary(
            VARIABLE_LIBRARY_NAME
        )
    except Exception as exc:
        raise RuntimeError(
            "Cannot read Fabric Variable Library 'Variables_1'. "
            "Run this job in the Fabric workspace that contains the library "
            "and under an identity supported by NotebookUtils."
        ) from exc

    return {
        "api_key": str(variable_library.api_key),
        "api_secret": str(variable_library.api_secret),
        "bootstrap_servers": str(variable_library.bootstrap_servers),
        "topic": str(variable_library.topic),
    }


def escape_jaas(value: str) -> str:
    return value.replace("\\", "\\\\").replace('"', '\\"')


def build_bronze_stream(kafka_config):
    api_key = escape_jaas(kafka_config["api_key"])
    api_secret = escape_jaas(kafka_config["api_secret"])
    jaas_config = (
        "org.apache.kafka.common.security.plain.PlainLoginModule required "
        f'username="{api_key}" password="{api_secret}";'
    )

    kafka_stream = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", kafka_config["bootstrap_servers"])
        .option("kafka.sasl.jaas.config", jaas_config)
        .option("kafka.security.protocol", "SASL_SSL")
        .option("kafka.sasl.mechanism", "PLAIN")
        .option("subscribe", kafka_config["topic"])
        .option("startingOffsets", KAFKA_STARTING_OFFSETS)
        .option("groupIdPrefix", "fabric-finguard-prod")
        .load()
    )

    return kafka_stream.select(
        col("key").cast("string").alias("key"),
        col("value").cast("string").alias("value"),
        col("topic"),
        col("partition"),
        col("offset"),
        col("timestamp"),
        col("timestampType"),
        F.current_timestamp().alias("ingestion_timestamp"),
    )


def build_silver_stream():
    bronze_stream = (
        spark.readStream
        .format("delta")
        .load(BRONZE_TABLE_PATH)
    )

    kafka_metadata_columns = [
        col(column_name).alias(f"kafka_{column_name}")
        for column_name in ["topic", "partition", "offset", "timestamp"]
    ]

    transformed_stream = (
        bronze_stream
        .withColumn(
            "data",
            F.from_json(col("value"), TRANSACTION_SCHEMA),
        )
        .select(
            "data.*",
            *kafka_metadata_columns,
            "ingestion_timestamp",
        )
    )

    valid_transaction = (
        col("transaction_id").isNotNull()
        & col("customer_id").isNotNull()
        & col("card_number").isNotNull()
        & col("merchant_id").isNotNull()
        & (col("amount") > 0)
    )

    return transformed_stream.filter(valid_transaction)


def ensure_bronze_table_exists(bronze_stream):
    if not DeltaTable.isDeltaTable(spark, BRONZE_TABLE_PATH):
        (
            spark.createDataFrame([], bronze_stream.schema)
            .write
            .format("delta")
            .mode("errorifexists")
            .save(BRONZE_TABLE_PATH)
        )


def start_queries(bronze_stream, silver_stream):
    bronze_query = (
        bronze_stream.writeStream
        .queryName("finguard_kafka_to_bronze")
        .format("delta")
        .outputMode("append")
        .option("checkpointLocation", BRONZE_CHECKPOINT_PATH)
        .trigger(processingTime=TRIGGER_INTERVAL)
        .start(BRONZE_TABLE_PATH)
    )

    silver_query = (
        silver_stream.writeStream
        .queryName("finguard_bronze_to_silver")
        .format("delta")
        .outputMode("append")
        .option("checkpointLocation", SILVER_CHECKPOINT_PATH)
        .trigger(processingTime=TRIGGER_INTERVAL)
        .start(SILVER_TABLE_PATH)
    )

    return [bronze_query, silver_query]


def supervise_queries(queries):
    try:
        spark.streams.awaitAnyTermination()

        stopped_queries = [query for query in queries if not query.isActive]
        failures = [
            f"{query.name}: {query.exception()}"
            for query in stopped_queries
            if query.exception() is not None
        ]

        if failures:
            raise RuntimeError(
                "A streaming query failed: " + "; ".join(failures)
            )

        stopped_names = ", ".join(query.name for query in stopped_queries)
        raise RuntimeError(
            f"Streaming query stopped unexpectedly: {stopped_names}"
        )
    finally:
        for query in queries:
            if query.isActive:
                query.stop()


def main():
    spark.conf.set("spark.sql.session.timeZone", "UTC")
    spark.conf.set("spark.databricks.delta.optimizeWrite.enabled", "true")

    kafka_config = load_kafka_config()
    bronze_stream = build_bronze_stream(kafka_config)
    ensure_bronze_table_exists(bronze_stream)
    silver_stream = build_silver_stream()

    queries = start_queries(bronze_stream, silver_stream)
    supervise_queries(queries)


if __name__ == "__main__":
    main()

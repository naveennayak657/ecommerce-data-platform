# ──────────────────────────────────────────────────
# Author: Naveen Vishlavath
# File: spark_streaming/stream_processor.py
#
# PySpark Structured Streaming job that reads e-commerce
# events from Kafka and writes them to Delta Lake using
# Medallion Architecture (Bronze -> Silver -> Gold)
#
# Bronze : raw events as-is from Kafka
# Silver : cleaned, validated, deduplicated events
# Gold   : business aggregations ready for Snowflake
# ──────────────────────────────────────────────────

import logging
import os
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField,
    StringType, IntegerType,
    DoubleType, TimestampType
)
from dotenv import load_dotenv

# load environment variables
load_dotenv()

# ── logging setup ────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# ── config from environment variables ───────────────
KAFKA_BOOTSTRAP_SERVERS = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092')
KAFKA_TOPIC             = os.getenv('KAFKA_TOPIC', 'ecommerce-events')
BRONZE_PATH             = os.getenv('BRONZE_PATH', './delta_lake/bronze')
SILVER_PATH             = os.getenv('SILVER_PATH', './delta_lake/silver')
GOLD_PATH               = os.getenv('GOLD_PATH',   './delta_lake/gold')
CHECKPOINT_PATH         = os.getenv('CHECKPOINT_PATH', './delta_lake/checkpoints')

# ── event schema ─────────────────────────────────────
# defining schema explicitly is better than inferring it
# schema inference is slow and unreliable in streaming
EVENT_SCHEMA = StructType([
    StructField('event_id',       StringType(),    True),
    StructField('event_type',     StringType(),    True),
    StructField('user_id',        IntegerType(),   True),
    StructField('session_id',     StringType(),    True),
    StructField('timestamp',      StringType(),    True),
    StructField('product_id',     StringType(),    True),
    StructField('product_name',   StringType(),    True),
    StructField('category',       StringType(),    True),
    StructField('price',          DoubleType(),    True),
    StructField('quantity',       IntegerType(),   True),
    StructField('total_amount',   DoubleType(),    True),
    StructField('action',         StringType(),    True),
    StructField('payment_method', StringType(),    True),
    StructField('status',         StringType(),    True),
])


def create_spark_session() -> SparkSession:
    """
    Create SparkSession with Delta Lake and Kafka support.
    Using builder pattern with all required packages.
    Had to figure out the exact package versions that work
    together — delta 3.2.0 works with Spark 3.5.x
    """
    logger.info("🔧 Creating Spark session...")

    spark = (
        SparkSession.builder
        .appName("EcommerceStreamProcessor")
        .master("local[*]")
        .config(
            "spark.jars.packages",
            # delta lake package for medallion architecture
            "io.delta:delta-spark_2.12:3.2.0,"
            # kafka package for reading streams
            "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0"
        )
        .config("spark.sql.extensions",
                "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog",
                "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        # reduce shuffle partitions for local dev
        # default is 200 which is way too many for our volume
        .config("spark.sql.shuffle.partitions", "4")
        # enable auto schema evolution in delta lake
        .config("spark.databricks.delta.schema.autoMerge.enabled", "true")
        .getOrCreate()
    )

    # reduce spark logging noise in console
    spark.sparkContext.setLogLevel("WARN")
    logger.info("✅ Spark session created successfully")
    return spark


def read_from_kafka(spark: SparkSession):
    """
    Read streaming events from Kafka topic.
    Returns a streaming DataFrame — data flows in continuously.
    """
    logger.info(f"📡 Connecting to Kafka topic: {KAFKA_TOPIC}")

    return (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
        .option("subscribe", KAFKA_TOPIC)
        # read from beginning so we don't miss any events
        .option("startingOffsets", "earliest")
        # max records per micro-batch — prevents overwhelming spark
        .option("maxOffsetsPerTrigger", 1000)
        .load()
    )


def parse_events(raw_df):
    """
    Parse raw Kafka messages into structured events.
    Kafka delivers messages as raw bytes — we need to
    deserialize the JSON and apply our schema.
    """
    return (
        raw_df
        # kafka value is binary — cast to string first
        .select(F.col("value").cast("string").alias("json_value"),
                F.col("timestamp").alias("kafka_timestamp"))
        # parse JSON string into structured columns
        .select(
            F.from_json(F.col("json_value"), EVENT_SCHEMA).alias("data"),
            F.col("kafka_timestamp")
        )
        # flatten nested struct into individual columns
        .select("data.*", "kafka_timestamp")
    )


def write_bronze(parsed_df, checkpoint_path: str):
    """
    🥉 BRONZE LAYER
    Write raw parsed events to Delta Lake as-is.
    No filtering, no transformations — raw truth.
    Partition by event_type for efficient querying later.
    """
    logger.info(f"🥉 Writing to Bronze layer: {BRONZE_PATH}")

    return (
        parsed_df.writeStream
        .format("delta")
        .outputMode("append")
        .option("checkpointLocation", f"{checkpoint_path}/bronze")
        # partition by event_type — makes queries faster
        .partitionBy("event_type")
        .start(BRONZE_PATH)
    )


def transform_silver(parsed_df):
    """
    🥈 SILVER LAYER
    Clean and validate the bronze data:
    - Filter out null event_ids
    - Filter out invalid prices
    - Convert timestamp string to proper timestamp type
    - Add ingestion metadata columns
    - Remove obvious duplicates
    """
    return (
        parsed_df
        # filter out records with missing critical fields
        .filter(F.col("event_id").isNotNull())
        .filter(F.col("event_type").isNotNull())
        .filter(F.col("user_id").isNotNull())
        .filter(F.col("price") > 0)
        # filter only valid event types
        .filter(F.col("event_type").isin([
            'order_placed', 'item_viewed',
            'cart_updated', 'payment_processed'
        ]))
        # convert timestamp string to proper timestamp
        .withColumn("event_timestamp",
                    F.to_timestamp(F.col("timestamp")))
        # add ingestion metadata — useful for debugging
        .withColumn("ingested_at",
                    F.current_timestamp())
        .withColumn("ingestion_date",
                    F.current_date())
        # drop the original string timestamp
        .drop("timestamp")
        # drop kafka metadata column
        .drop("kafka_timestamp")
    )


def write_silver(silver_df, checkpoint_path: str):
    """
    Write cleaned silver data to Delta Lake.
    Partitioned by date for efficient time-range queries.
    """
    logger.info(f"🥈 Writing to Silver layer: {SILVER_PATH}")

    return (
        silver_df.writeStream
        .format("delta")
        .outputMode("append")
        .option("checkpointLocation", f"{checkpoint_path}/silver")
        .partitionBy("ingestion_date")
        .start(SILVER_PATH)
    )


def transform_gold(silver_df):
    """
    🥇 GOLD LAYER
    Business-ready aggregations.
    These are the metrics that go to Snowflake and
    get consumed by analysts and dashboards.

    Aggregations we compute:
    - Revenue per category per minute
    - Order count per user
    - Payment success rate
    - Top products by views
    """

    # ── revenue by category ──────────────────────────
    revenue_by_category = (
        silver_df
        .filter(F.col("event_type") == "order_placed")
        .withWatermark("event_timestamp", "10 minutes")
        .groupBy(
            F.window("event_timestamp", "1 minute").alias("time_window"),
            F.col("category")
        )
        .agg(
            F.sum("total_amount").alias("total_revenue"),
            F.count("event_id").alias("order_count"),
            F.avg("total_amount").alias("avg_order_value")
        )
        .select(
            F.col("time_window.start").alias("window_start"),
            F.col("time_window.end").alias("window_end"),
            "category",
            "total_revenue",
            "order_count",
            "avg_order_value"
        )
    )

    # ── payment success rate ─────────────────────────
    payment_stats = (
        silver_df
        .filter(F.col("event_type") == "payment_processed")
        .withWatermark("event_timestamp", "10 minutes")
        .groupBy(
            F.window("event_timestamp", "1 minute").alias("time_window"),
            F.col("payment_method")
        )
        .agg(
            F.count("event_id").alias("total_payments"),
            F.sum(F.when(F.col("status") == "success", 1).otherwise(0))
             .alias("successful_payments"),
            F.sum(F.when(F.col("status") == "failed", 1).otherwise(0))
             .alias("failed_payments")
        )
        .withColumn(
            "success_rate",
            F.round(
                F.col("successful_payments") / F.col("total_payments") * 100,
                2
            )
        )
        .select(
            F.col("time_window.start").alias("window_start"),
            F.col("time_window.end").alias("window_end"),
            "payment_method",
            "total_payments",
            "successful_payments",
            "failed_payments",
            "success_rate"
        )
    )

    return revenue_by_category, payment_stats


def write_gold(revenue_df, payment_df, checkpoint_path: str):
    """
    Write gold aggregations to Delta Lake.
    Using update mode since we're aggregating with windows.
    """
    logger.info(f"🥇 Writing to Gold layer: {GOLD_PATH}")

    revenue_query = (
        revenue_df.writeStream
        .format("delta")
        .outputMode("append")
        .option("checkpointLocation", f"{checkpoint_path}/gold_revenue")
        .start(f"{GOLD_PATH}/revenue_by_category")
    )

    payment_query = (
        payment_df.writeStream
        .format("delta")
        .outputMode("append")
        .option("checkpointLocation", f"{checkpoint_path}/gold_payments")
        .start(f"{GOLD_PATH}/payment_stats")
    )

    return revenue_query, payment_query


def main():
    logger.info("🚀 Starting PySpark Stream Processor")
    logger.info(f"📡 Kafka  : {KAFKA_BOOTSTRAP_SERVERS}")
    logger.info(f"📂 Bronze : {BRONZE_PATH}")
    logger.info(f"📂 Silver : {SILVER_PATH}")
    logger.info(f"📂 Gold   : {GOLD_PATH}")

    # create spark session
    spark = create_spark_session()

    # read from kafka
    raw_df = read_from_kafka(spark)

    # parse raw kafka messages
    parsed_df = parse_events(raw_df)

    # ── bronze layer ─────────────────────────────────
    bronze_query = write_bronze(parsed_df, CHECKPOINT_PATH)

    # ── silver layer ─────────────────────────────────
    silver_df    = transform_silver(parsed_df)
    silver_query = write_silver(silver_df, CHECKPOINT_PATH)

    # ── gold layer ───────────────────────────────────
    revenue_df, payment_df       = transform_gold(silver_df)
    revenue_query, payment_query = write_gold(
        revenue_df, payment_df, CHECKPOINT_PATH
    )

    logger.info("✅ All streaming queries started successfully")
    logger.info("📊 Pipeline is running — waiting for events...")

    # wait for all streams to finish
    # in production this runs forever until manually stopped
    spark.streams.awaitAnyTermination()


if __name__ == '__main__':
    main()
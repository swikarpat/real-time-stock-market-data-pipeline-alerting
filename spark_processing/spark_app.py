from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import Window
import findspark
import os

findspark.init()

# --- Configuration (Load from environment variables) ---
KAFKA_BROKERS = os.environ.get("KAFKA_BROKERS", "localhost:9092")
POSTGRES_HOST = os.environ.get("POSTGRES_HOST", "localhost")
POSTGRES_PORT = os.environ.get("POSTGRES_PORT", "5432")
POSTGRES_DB = os.environ.get("POSTGRES_DB", "stock_data_db")
POSTGRES_USER = os.environ.get("POSTGRES_USER", "postgres")
POSTGRES_PASSWORD = os.environ.get("POSTGRES_PASSWORD", "postgres")
S3_BUCKET = os.environ.get("S3_BUCKET", "real-time-stock-data") # Add S3 bucket configuration

# --- Schema Definition ---
stock_data_schema = StructType([
    StructField("symbol", StringType(), True),
    StructField("timestamp", TimestampType(), True),
    StructField("price", DoubleType(), True),
    StructField("volume", LongType(), True)
])

# --- Spark Session Initialization ---
spark = SparkSession.builder \
    .appName("StockMarketProcessing") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.0,org.postgresql:postgresql:42.2.10,com.amazonaws:aws-java-sdk-bundle:1.11.874") \
    .getOrCreate()

spark.sparkContext.setLogLevel("INFO")

# --- Data Validation ---
def validate_data(df):
    """
    Validates the incoming data against the defined schema.
    You can add more specific validation rules here.
    """
    # Check for null values in critical columns
    if df.filter(col("symbol").isNull() | col("timestamp").isNull() | col("price").isNull() | col("volume").isNull()).count() > 0:
        spark.sparkContext.parallelize([f"Validation Error at {time.strftime('%Y-%m-%d %H:%M:%S')}"]).saveAsTextFile(f"/tmp/validation_errors/{time.strftime('%Y%m%d%H%M%S')}")
        return None

    return df

# --- Read Data from Kafka ---
try:
    df = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BROKERS) \
        .option("subscribe", "stock-market-data") \
        .option("startingOffsets", "earliest") \
        .load()
except Exception as e:
    spark.sparkContext.parallelize([f"Kafka Connection Error at {time.strftime('%Y-%m-%d %H:%M:%S')}: {e}"]).saveAsTextFile(f"s3a://{S3_BUCKET}/spark_errors/kafka_errors/{time.strftime('%Y%m%d%H%M%S')}")
    raise

# --- Data Processing ---
df = df.selectExpr("CAST(value AS STRING)").select(from_json("value", stock_data_schema).alias("data")).select("data.*")

# Validate data
validated_df = validate_data(df)

if validated_df is not None:
    # Windowed aggregation (5-minute average price)
    windowed_df = validated_df \
        .withWatermark("timestamp", "10 minutes") \
        .groupBy(
        window("timestamp", "5 minutes"),
        "symbol"
    ) \
        .agg(
        avg("price").alias("avg_price"),
        sum("volume").alias("total_volume"),
        max("timestamp").alias("latest_timestamp")
    )

    # Calculate 5-period simple moving average (SMA)
    window_spec = Window.partitionBy("symbol").orderBy("latest_timestamp").rowsBetween(-4, 0)
    sma_df = windowed_df.withColumn("sma_5", avg("avg_price").over(window_spec))

    # --- Write to PostgreSQL with Error Handling and Retries---
    def write_to_postgresql(batch_df, batch_id):
        """Writes data to PostgreSQL with error handling and retries."""
        retries = 3
        delay = 5

        for attempt in range(retries):
            try:
                batch_df.write \
                    .format("jdbc") \
                    .option("url", f"jdbc:postgresql://{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}") \
                    .option("driver", "org.postgresql.Driver") \
                    .option("dbtable", "stock_aggs") \
                    .option("user", POSTGRES_USER) \
                    .option("password", POSTGRES_PASSWORD) \
                    .mode("append") \
                    .save()
                spark.sparkContext.parallelize([f"Batch {batch_id} written successfully at {time.strftime('%Y-%m-%d %H:%M:%S')}"]).saveAsTextFile(f"/tmp/success_logs/{time.strftime('%Y%m%d%H%M%S')}")
                return  # Success, exit the retry loop
            except Exception as e:
                spark.sparkContext.parallelize([f"Attempt {attempt + 1} failed at {time.strftime('%Y-%m-%d %H:%M:%S')}: {e}"]).saveAsTextFile(f"/tmp/retry_logs/{time.strftime('%Y%m%d%H%M%S')}")
                if attempt < retries - 1:
                    time.sleep(delay)
                else:
                    spark.sparkContext.parallelize([f"Failed to write batch {batch_id} to PostgreSQL after multiple retries at {time.strftime('%Y-%m-%d %H:%M:%S')}: {e}"]).saveAsTextFile(f"/tmp/failed_logs/{time.strftime('%Y%m%d%H%M%S')}")
                    raise  # Re-raise the exception after final failure

    write_query = sma_df \
        .writeStream \
        .outputMode("update") \
        .foreachBatch(write_to_postgresql) \
        .option("checkpointLocation", "/tmp/checkpoint") \
        .start()

    # --- Write to S3 (Data Lake) ---
    s3_query = sma_df \
        .writeStream \
        .outputMode("append") \
        .format("parquet") \
        .option("path", f"s3a://{S3_BUCKET}/stock_aggs/") \
        .option("checkpointLocation", "/tmp/s3_checkpoint/") \
        .start()

    write_query.awaitTermination()
    s3_query.awaitTermination()
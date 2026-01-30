import time  # <--- Bổ sung thư viện time
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, to_timestamp
from pyspark.sql.types import StructType, StringType, DoubleType, IntegerType, ArrayType

# Cấu hình Spark Session
spark = (
    SparkSession.builder
    .appName("KafkaEcommerceStreaming")
    .master("spark://spark-master:7077")
    .config("spark.sql.streaming.checkpointLocation", "/opt/spark/work-dir/checkpoint")
    .config("spark.executor.memory", "1g")
    .config("spark.driver.memory", "1g")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

# Định nghĩa Schema
schema = (
    StructType()
    .add("source", StringType())
    .add("product_id", StringType())
    .add("name", StringType())
    .add("price", DoubleType())
    .add("currency", StringType())
    .add("timestamp", DoubleType())
    .add("category", StringType())
    .add("brand", StringType())
    .add("rating", DoubleType())
    .add("stock", IntegerType())
    .add("thumbnail", StringType())
    .add("rating_count", IntegerType())
    .add("description", StringType())
    .add("image", StringType())
    .add("images", ArrayType(StringType()))
)

# Đọc từ Kafka
df_raw = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "kafka:9092")
    .option("subscribe", "ecommerce_products")
    .option("startingOffsets", "latest")
    .option("failOnDataLoss", "false")
    .load()
)

# Transform
df_parsed = (
    df_raw
    .selectExpr("CAST(value AS STRING)")
    .select(from_json(col("value"), schema).alias("data"))
    .select("data.*")
    .withColumn("event_time", to_timestamp(col("timestamp")))
    .drop("images")
)


# --- PHẦN CHỈNH SỬA QUAN TRỌNG Ở ĐÂY ---
def write_to_clickhouse(batch_df, batch_id):
    start_time = time.time()

    # Persist batch_df để tối ưu hiệu năng khi gọi count() và write()
    batch_df.persist()
    records_count = batch_df.count()

    if records_count > 0:
        try:
            print(f"--> Processing Batch {batch_id} with {records_count} records...")

            (
                batch_df.write
                .format("jdbc")
                .option("url", "jdbc:clickhouse://clickhouse:8123/ecommerce")
                .option("dbtable", "products")
                .option("driver", "com.clickhouse.jdbc.ClickHouseDriver")
                .option("batchsize", "5000")
                .option("isolationLevel", "NONE")
                .mode("append")
                .save()
            )

            end_time = time.time()
            duration = end_time - start_time
            speed = records_count / duration if duration > 0 else 0

            # Log benchmark đẹp tương tự extract_api
            print("=" * 60)
            print(f"BATCH {batch_id} COMPLETED SUCCESS")
            print(f"   └─ Records: {records_count} rows")
            print(f"   └─ Time:    {duration:.2f} s")
            print(f"   └─ Speed:   {speed:.1f} rows/s")
            print("=" * 60)

        except Exception as e:
            duration = time.time() - start_time
            print("=" * 60)
            print(f"BATCH {batch_id} FAILED")
            print(f"   └─ Error: {str(e)}")
            print(f"   └─ Time:  {duration:.2f} s")
            print("=" * 60)
    else:
        # Batch rỗng (không in quá nhiều để đỡ rối log, hoặc in 1 dòng nhỏ)
        pass
        print(f"Batch {batch_id}: No data (took {time.time()-start_time:.3f}s)")

    # Giải phóng bộ nhớ
    batch_df.unpersist()


# Start Query
print("Starting Spark Streaming Job with Benchmark Logs...")
query = (
    df_parsed.writeStream
    .foreachBatch(write_to_clickhouse)
    .outputMode("append")
    .trigger(processingTime='15 seconds')
    .start()
)

query.awaitTermination()
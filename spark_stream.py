# spark_stream.py
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import *

spark = SparkSession.builder \
    .appName("EcommerceStreaming") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

schema = StructType([
    StructField("user_id", IntegerType()),
    StructField("product", StringType()),
    StructField("price", DoubleType()),
    StructField("event_type", StringType()),
    StructField("timestamp", DoubleType())
])

# Read from Kafka
df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "ecommerce_topic") \
    .load()

# Convert Kafka value (binary → JSON)
json_df = df.selectExpr("CAST(value AS STRING)")

parsed_df = json_df.select(
    from_json(col("value"), schema).alias("data")
).select("data.*")

# Example transformation: filter purchases only
purchases = parsed_df.filter(col("event_type") == "purchase")

# Write to HDFS
query = purchases.writeStream \
    .format("parquet") \
    .option("path", "hdfs://localhost:9000/ecommerce/output") \
    .option("checkpointLocation", "hdfs://localhost:9000/ecommerce/checkpoint") \
    .outputMode("append") \
    .start()

query.awaitTermination()

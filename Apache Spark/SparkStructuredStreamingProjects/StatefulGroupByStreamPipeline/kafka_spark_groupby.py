from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, sum
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType

# Create a SparkSession
spark = SparkSession.builder \
	.appName("GroupByStreaming") \
    .master("local[3]") \
    .config("spark.streaming.stopGracefullyOnShutdown", "true") \
	.config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.0.0") \
	.getOrCreate()

# Read the stream from Kafka
df = spark.readStream.format("kafka")\
    .option("kafka.bootstrap.servers", "localhost:9092")\
    .option("subscribe", "trx_data")\
    .option("startingOffsets", "earliest")\
    .load()

# Define the schema of the JSON data
schema = StructType([
    StructField("user_id", StringType()),
    StructField("amount", IntegerType()),
    StructField("timestamp", TimestampType())
])

# Parse the JSON data and select the fields
df = df.select(from_json(col("value").cast("string"), schema).alias("data")).select("data.*")

# Perform the aggregation
df = df.groupBy("user_id").agg(sum("amount").alias("total_amount"))

# Write the output to the console in complete mode
query = df.writeStream \
	.outputMode("complete") \
	.format("console") \
	.start()

# Wait for the query to terminate
query.awaitTermination()

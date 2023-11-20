from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, IntegerType, StringType

spark = SparkSession.builder \
    .appName("KafkaReader") \
    .master("local[2]") \
    .config("spark.streaming.stopGracefullyOnShutdown", "true") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.0.0") \
    .getOrCreate()

# Define your schema
schema = StructType([
    StructField("id", IntegerType()),
    StructField("name", StringType()),
    StructField("age", IntegerType())
])

# Read from Kafka
df = spark \
  .readStream \
  .format("kafka") \
  .option("kafka.bootstrap.servers", "localhost:9092") \
  .option("subscribe", "user_data") \
  .option("startingOffsets", "earliest") \
  .load()

# Convert the data and filter
data = df.selectExpr("CAST(value AS STRING)") \
  .select(from_json(col("value").cast("string"), schema).alias("data")) \
  .select("data.*") \
  .filter(col("age") > 25)

# Start streaming and print to console
query = data \
    .writeStream \
    .outputMode("update") \
    .format("console") \
    .start()

query.awaitTermination()


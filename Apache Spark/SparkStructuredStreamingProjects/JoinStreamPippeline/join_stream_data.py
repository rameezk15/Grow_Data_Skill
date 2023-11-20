from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType, TimestampType

# Create a SparkSession
spark = SparkSession.builder \
    .appName("JoinStreamingAndStatic") \
    .master("local[3]") \
    .config("spark.streaming.stopGracefullyOnShutdown", "true") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.0.0") \
    .getOrCreate()

# Read the stream from Kafka
df = spark.readStream.format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "fruit_data") \
    .option("startingOffsets", "earliest") \
    .load()

# Define the schema of the JSON data
schema = StructType([
    StructField("id", StringType()),
    StructField("value", StringType()),
    StructField("timestamp", TimestampType())
])

# Parse the JSON data and select the fields
df = df.select(from_json(col("value").cast("string"), schema).alias("data")).select("data.*")

# Read the static dataset
df_static = spark.read.csv("fruit_dim.csv", inferSchema=True, header=True)

# Join the streaming data with the static dataset
df_joined = df.join(df_static, df.id == df_static.id,'inner').drop(df_static.id)

# Write the output to the console in append mode
query = df_joined.writeStream \
    .outputMode("append") \
    .format("console") \
    .option("truncate", "false") \
    .start()

# Wait for the query to terminate
query.awaitTermination()

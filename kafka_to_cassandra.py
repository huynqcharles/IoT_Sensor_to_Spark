from pyspark.sql import SparkSession
from pyspark.sql.functions import col, decode, from_json, expr
from pyspark.sql.types import StructType, StructField, StringType, FloatType

# Create Spark session
spark = SparkSession.builder \
    .appName("WeatherDataProcessing") \
    .config("spark.cassandra.connection.host", "127.0.0.1") \
    .getOrCreate()

# Set log level to only show errors
spark.sparkContext.setLogLevel("ERROR")

# Define schema for data from WeatherAPI
weather_schema = StructType([
    StructField("location", StructType([
        StructField("name", StringType(), True),
        StructField("country", StringType(), True),
        StructField("localtime", StringType(), True)
    ])),
    StructField("current", StructType([
        StructField("last_updated", StringType(), True),  # Use as ID
        StructField("temp_c", StringType(), True),
        StructField("wind_kph", StringType(), True),
        StructField("pressure_mb", StringType(), True),
        StructField("humidity", StringType(), True),
        StructField("cloud", StringType(), True),
        StructField("feelslike_c", StringType(), True)
    ]))
])

# Read data from Kafka
kafka_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "sensor-data") \
    .option("startingOffsets", "latest") \
    .option("failOnDataLoss", "false") \
    .load()

# Convert data from byte to UTF-8 string and parse JSON
kafka_value_df = kafka_df.withColumn("json_value", decode(col("value"), "UTF-8"))

# Parse JSON data into columns
weather_data_df = kafka_value_df.select(from_json(col("json_value"), weather_schema).alias("data"))

# Extract specific fields and cast to appropriate types
processed_df = weather_data_df.select(
    col("data.current.last_updated").alias("last_updated"),
    col("data.current.temp_c").cast(FloatType()).alias("temp_c"),
    col("data.current.wind_kph").cast(FloatType()).alias("wind_kph"),
    col("data.current.pressure_mb").cast(FloatType()).alias("pressure_mb"),
    col("data.current.humidity").cast(FloatType()).alias("humidity"),
    col("data.current.cloud").cast(FloatType()).alias("cloud")
)

# Write data to Cassandra
query = processed_df.writeStream \
    .format("org.apache.spark.sql.cassandra") \
    .options(table="weather_data_numeric", keyspace="sensor_data") \
    .option("checkpointLocation", "/tmp/checkpoints_weather") \
    .outputMode("append") \
    .start()

query.awaitTermination()

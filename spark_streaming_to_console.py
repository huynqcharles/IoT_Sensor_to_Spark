from pyspark.sql import SparkSession
from pyspark.sql.functions import col, decode, from_json, explode
from pyspark.sql.types import StructType, StructField, StringType, ArrayType

spark = SparkSession.builder \
    .appName("KafkaSensorDataProcessing") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

sensor_data_schema = ArrayType(StructType([
    StructField("timestamp", StringType(), True),
    StructField("sensor", StructType([
        StructField("pin", StringType(), True),
        StructField("sensor_type", StructType([
            StructField("name", StringType(), True),
            StructField("manufacturer", StringType(), True)
        ])),
        StructField("id", StringType(), True)
    ])),
    StructField("sensordatavalues", ArrayType(StructType([
        StructField("value", StringType(), True),
        StructField("value_type", StringType(), True)
    ]))),
    StructField("location", StructType([
        StructField("latitude", StringType(), True),
        StructField("longitude", StringType(), True),
        StructField("country", StringType(), True),
        StructField("indoor", StringType(), True),
        StructField("altitude", StringType(), True)
    ]))
]))

kafka_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "sensor-data") \
    .option("startingOffsets", "earliest") \
    .load()

kafka_value_df = kafka_df.withColumn("json_value", decode(col("value"), "UTF-8"))

sensor_data_df = kafka_value_df.select(from_json(col("json_value"), sensor_data_schema).alias("data"))

exploded_df = sensor_data_df.select(explode(col("data")).alias("sensor_data"))

result_df = exploded_df.select(
    col("sensor_data.timestamp").alias("timestamp"),
    col("sensor_data.sensor.pin").alias("pin"),
    col("sensor_data.sensor.sensor_type.name").alias("sensor_name"),
    col("sensor_data.sensor.sensor_type.manufacturer").alias("manufacturer"),
    col("sensor_data.sensor.id").alias("sensor_id"),
    col("sensor_data.sensordatavalues.value_type").alias("value_type"),
    col("sensor_data.sensordatavalues.value").alias("sensor_value"),
    col("sensor_data.location.latitude").alias("latitude"),
    col("sensor_data.location.longitude").alias("longitude"),
    col("sensor_data.location.country").alias("country"),
    col("sensor_data.location.indoor").alias("indoor"),
    col("sensor_data.location.altitude").alias("altitude")
)

query = result_df.writeStream \
    .outputMode("append") \
    .format("console") \
    .start()

query.awaitTermination()

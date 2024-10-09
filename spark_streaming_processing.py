from pyspark.sql import SparkSession
from pyspark.sql.functions import col, decode, collect_list, to_json
import subprocess

# Tạo Spark session
spark = SparkSession.builder \
    .appName("KafkaSensorDataProcessing") \
    .getOrCreate()

# Đọc dữ liệu từ Kafka
kafka_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "sensor-data") \
    .option("startingOffsets", "earliest") \
    .load()

# Chuyển đổi dữ liệu từ byte sang chuỗi UTF-8
kafka_value_df = kafka_df.withColumn("json_value", decode(col("value"), "UTF-8"))

# Gộp tất cả các dòng lại thành một danh sách JSON duy nhất
aggregated_json_df = kafka_value_df \
    .groupBy() \
    .agg(collect_list("json_value").alias("json_list")) \
    .withColumn("aggregated_json", to_json(col("json_list")))

# Biến toàn cục để đếm số file
file_count = 0

# Hàm để xóa tất cả các file trong thư mục HDFS
def clear_hdfs_directory(hdfs_directory):
    try:
        subprocess.run(["hdfs", "dfs", "-rm", "-r", hdfs_directory], check=True)
    except subprocess.CalledProcessError as e:
        print(f"Error deleting directory {hdfs_directory}: {e}")

# Hàm lưu dữ liệu vào HDFS với tên file tăng dần và xóa file cũ
def save_to_hdfs(batch_df, batch_id):
    global file_count
    # Tăng giá trị của file_count
    file_count += 1

    # Lấy dòng đầu tiên chứa JSON
    json_row = batch_df.select("aggregated_json").first()
    
    # Lưu vào HDFS nếu không trống
    if json_row and json_row["aggregated_json"]:
        # Xóa các file cũ trong HDFS
        hdfs_directory = "hdfs://localhost:9000/iot_sensor_data/*"
        clear_hdfs_directory(hdfs_directory)
        
        # Tạo RDD từ chuỗi JSON
        rdd = spark.sparkContext.parallelize([json_row["aggregated_json"]])
        
        # Tạo đường dẫn file với tên tăng dần
        hdfs_path = f"hdfs://localhost:9000/iot_sensor_data/sensor_data_{file_count}.json"
        
        # Lưu vào HDFS dưới dạng file text
        rdd.saveAsTextFile(hdfs_path)

# Viết stream, chỉ lấy batch đầu tiên và lưu vào HDFS
query = aggregated_json_df.writeStream \
    .outputMode("complete") \
    .foreachBatch(save_to_hdfs) \
    .start()

query.awaitTermination()

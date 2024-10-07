from kafka import KafkaProducer
import requests
import json
import time

# Khởi tạo Kafka Producer
producer = KafkaProducer(bootstrap_servers='localhost:9092',
                         value_serializer=lambda v: json.dumps(v).encode('utf-8'))

# Hàm lấy dữ liệu từ Sensor.Community API cho Việt Nam
def fetch_data():
    url = 'https://api.sensor.community/v1/filter/country=VN'
    response = requests.get(url)
    if response.status_code == 200:
        return response.json()
    else:
        print(f"Error fetching data: {response.status_code}")
        return None

# Mô phỏng việc lấy dữ liệu real-time (lặp liên tục)
try:
    while True:
        data = fetch_data()  # Gọi API để lấy dữ liệu
        if data:
            print("DU LIEU JSON:", json.dumps(data, indent=4))  # In dữ liệu JSON ra terminal
            producer.send('sensor-data', value=data)  # Gửi dữ liệu vào Kafka topic
            print("DU LIEU DA DUOC GUI DEN KAFKA")
        time.sleep(60)  # Chờ 60 giây trước khi gọi API lại
except KeyboardInterrupt:
    print("DUNG GUI DU LIEU")
finally:
    producer.close()


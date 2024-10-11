from kafka import KafkaProducer
import requests
import json
import time

producer = KafkaProducer(bootstrap_servers='localhost:9092',
                         value_serializer=lambda v: json.dumps(v).encode('utf-8'))

def fetch_data():
    url = 'https://api.sensor.community/v1/filter/country=VN'
    response = requests.get(url)
    if response.status_code == 200:
        try:
            data = response.json()
            print("DATA FETCH FROM API:", json.dumps(data, indent=4))
            return data
        except json.JSONDecodeError:
            print("ERROR: NOT VALID JSON")
            return None
    else:
        print(f"Error fetching data: {response.status_code}")
        return None

try:
    while True:
        data = fetch_data()
        if data and isinstance(data, (list, dict)):
            producer.send('sensor-data', value=data)
            print("SENT TO KAFKA")
        time.sleep(10)
except KeyboardInterrupt:
    print("STOPPED SENDING DATA")
finally:
    producer.close()

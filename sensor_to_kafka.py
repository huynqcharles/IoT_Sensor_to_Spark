from kafka import KafkaProducer
import requests
import json
import time
import random  # Add random to apply random changes to values

# Initialize Kafka Producer
producer = KafkaProducer(bootstrap_servers='localhost:9092',
                         value_serializer=lambda v: json.dumps(v).encode('utf-8'))

# Function to fetch data from WeatherAPI and randomly modify some values
def fetch_data_and_modify():
    url = 'http://api.weatherapi.com/v1/current.json?key=c4b9b4b2537a43e8ba1100937241310&q=Hanoi&aqi=no'
    response = requests.get(url)
    if response.status_code == 200:
        data = response.json()

        # Randomly modify values:
        data["current"]["temp_c"] += round(random.uniform(-0.5, 0.5), 2)  # Modify temperature by ±0.5°C
        data["current"]["feelslike_c"] += round(random.uniform(-0.5, 0.5), 2)  # Modify feels like temperature by ±0.5°C
        data["current"]["pressure_mb"] += round(random.uniform(-1, 1), 2)  # Modify pressure by ±1 mb
        data["current"]["humidity"] += round(random.uniform(-2, 2), 2)  # Modify humidity by ±2%
        data["current"]["wind_kph"] += round(random.uniform(-1, 1), 2)  # Modify wind speed by ±1 kph
        data["current"]["cloud"] += random.randint(-5, 5)  # Modify cloud percentage by ±5%

        # Ensure values are within reasonable limits
        data["current"]["cloud"] = max(0, min(100, data["current"]["cloud"]))  # Cloud percentage between 0 and 100%
        data["current"]["humidity"] = max(0, min(100, data["current"]["humidity"]))  # Humidity between 0 and 100%
        
        # Update the system's current time for data retrieval
        data["current"]["last_updated"] = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
        return data
    else:
        print(f"Error fetching data: {response.status_code}")
        return None

# Simulate real-time data fetching (continuous loop)
try:
    while True:
        data = fetch_data_and_modify()  # Call API to fetch and modify data
        if data:
            print("JSON DATA:", json.dumps(data, indent=4))  # Print JSON data to terminal
            producer.send('sensor-data', value=data)  # Send data to Kafka topic
            print("DATA SENT TO KAFKA")
#        time.sleep(5)  # Wait 5 seconds before calling the API again
except KeyboardInterrupt:
    print("STOP SENDING DATA")
finally:
    producer.close()

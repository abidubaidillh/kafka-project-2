import csv
import json
import time
import random
from kafka import KafkaProducer

# Inisialisasi Kafka Producer
producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

# Path ke file ratings
filename = 'ratings_small.csv'  # atau 'ratings.csv' untuk file besar

# Buka dan baca file CSV
with open(filename, newline='', encoding='utf-8') as csvfile:
    reader = csv.DictReader(csvfile)
    for row in reader:
        # Kirim data ke Kafka topic
        producer.send('movie-ratings', value=row)
        print(f"Sent: {row}")

        # Tambahkan jeda acak untuk simulasi streaming
        time.sleep(random.uniform(0.1, 0.5))  # 100-500 ms

producer.flush()
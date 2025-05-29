from kafka import KafkaConsumer
import json
import csv
import os

# Inisialisasi Kafka Consumer
consumer = KafkaConsumer(
    'movie-ratings',
    bootstrap_servers='localhost:9092',
    value_deserializer=lambda m: json.loads(m.decode('utf-8')),
    auto_offset_reset='earliest',
    enable_auto_commit=True,
    group_id='movie-consumer-group'
)

# Konfigurasi batch
batch_size = 100  # Ganti sesuai kebutuhan
batch = []
batch_number = 1

# Folder untuk menyimpan hasil batch
output_folder = 'batches'
os.makedirs(output_folder, exist_ok=True)

def save_batch_to_csv(batch, batch_number):
    if not batch:
        return
    filename = f"{output_folder}/batch_{batch_number}.csv"
    with open(filename, 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=batch[0].keys())
        writer.writeheader()
        writer.writerows(batch)
    print(f"[✔] Saved batch_{batch_number}.csv with {len(batch)} rows")

# Konsumsi data dari Kafka
for message in consumer:
    batch.append(message.value)

    if len(batch) >= batch_size:
        save_batch_to_csv(batch, batch_number)
        batch = []
        batch_number += 1
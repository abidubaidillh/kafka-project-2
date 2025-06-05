import pandas as pd
from kafka import KafkaProducer
import json
import time
import random
import os

KAFKA_TOPIC = 'movielens_ratings_stream'
KAFKA_BROKERS = ['127.0.0.1:9092'] 

# Inisialisasi Kafka Producer
try:
    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BROKERS,
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )
    print("Kafka Producer connected successfully!")
except Exception as e:
    print(f"Error connecting to Kafka Producer: {e}")
    exit()

# Dataset paths
RATINGS_PATH = 'dataset/ratings_small.csv'
MOVIES_PATH = 'dataset/movies_metadata.csv'

def send_data_from_ratings():
    try:
        ratings_df = pd.read_csv(RATINGS_PATH)
        movies_df = pd.read_csv(MOVIES_PATH, low_memory=False)

        # Gabungkan movieId dengan title
        movies_df = movies_df[['id', 'title']]
        movies_df['id'] = pd.to_numeric(movies_df['id'], errors='coerce')
        merged_df = ratings_df.merge(movies_df, left_on='movieId', right_on='id', how='left')

        print(f"Streaming {len(merged_df)} records from ratings...")
    except Exception as e:
        print(f"Error loading data: {e}")
        return

    for index, row in merged_df.iterrows():
        message = {
            'userId': int(row['userId']),
            'movieId': int(row['movieId']),
            'rating': float(row['rating']),
            'timestamp': int(row['timestamp']),
            'title': row['title'] if pd.notnull(row['title']) else "Unknown"
        }

        try:
            producer.send(KAFKA_TOPIC, value=message)
            print(f"Sent ({index+1}/{len(merged_df)}): User {message['userId']} rated '{message['title']}'")
        except Exception as e:
            print(f"Send error: {e}")
        
        time.sleep(random.uniform(0.01, 0.05))  # Jeda antar kiriman

    try:
        producer.flush()
        print("All data sent.")
    except Exception as e:
        print(f"Flush error: {e}")

    producer.close()
    print("Producer closed.")

if __name__ == "__main__":
    send_data_from_ratings()

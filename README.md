# Proyek Big Data: Kafka, Spark, dan API

|Nama|NRP|
|-|-|
|Benjamin|5027231078|
|Muhammad Syahmi Ash|5027231085|
|Abid Ubaidillah A|5027231089|
***

## Overview Proyek

Proyek ini bertujuan untuk mensimulasikan arsitektur pemrosesan Big Data secara real-time menggunakan Apache Kafka untuk streaming data, Apache Spark untuk pemrosesan batch dan pelatihan model Machine Learning, serta Flask API untuk menyajikan hasil prediksi model. Dataset yang digunakan adalah MovieLens (rating dan metadata film), di mana model-model yang dibangun bertujuan untuk memprediksi apakah pengguna akan menyukai suatu film, memperkirakan rating yang akan diberikan, serta mengelompokkan pengguna ke dalam segmen berdasarkan perilaku rating mereka.

**Arsitektur Sistem:**

1.  **Kafka Producer:** Membaca file `ratings_small.csv` dari MovieLens baris per baris, lalu mengirimkannya sebagai stream pesan ke Kafka Topic..
2.  **Kafka Server:** Bertindak sebagai message broker yang menampung data streaming dari producer dan meneruskannya ke consumer.
3.  **Kafka Consumer:** Menerima data dari Kafka Server dan menyimpannya dalam bentuk file batch CSV di folder `batched_data/`
4.  **Apache Spark:** Membaca file-file batch CSV tersebut untuk melakukan:
    *   Preprocessing data.
    *   Pelatihan model Machine Learning secara akumulatif setiap batch:
        1.  **Model Klasifikasi:** Memprediksi apakah user akan menyukai film berdasarkan rating ≥ 4.0..
        2.  **Model Regresi:** Memprediksi rating numerik yang akan diberikan oleh pengguna.
        3.  **Model Clustering:** Mengelompokkan pengguna berdasarkan pola rating dan perilaku mereka.
    *   Menyimpan model-model hasil pelatihan ke folder `trained_models/`.
5.  **Flask API:** Memuat model terbaru yang telah dilatih dan menyediakan endpoint RESTful untuk:
    *   Menerima input data .
    *   Mengembalikan prediksi klasifikasi, rating, dan cluster.


**Teknologi yang Digunakan:**
*   Apache Kafka: Streaming data & message queue antara komponen.
*   Apache Spark (PySpark): Preprocessing, training, evaluasi model Machine Learning.
*   Flask: Untuk membangun REST API.
*   Streamlit: Untuk membangun antarmuka pengguna (UI) interaktif.
*   Python: Bahasa pemrograman utama.
*   Docker & Docker Compose: Untuk manajemen environment Kafka dan Zookeeper.
*   Mamba/Conda: Manajemen environment dan dependensi Python.

## Struktur Direktori Proyek
```
Kafka-2.2
    │   api_server.py               # Untuk membangun REST API
    │   docker-compose.yml          # Untuk manajemen environment Kafka dan Zookeeper
    │   kafka_consumer.py           # Menangani data dari Kafka Server
    │   kafka_producer.py           # Membaca file dari dataset dan mengirimkannya ke server agar diteruskan ke consumer
    │   spark_training.py           # Preprocessing, melatih model dan menyimpan model
    │
    ├───batched_data                # hasil dari consumer
    │       movielens_batch_1748945638_1.csv
    │       movielens_batch_1748945638_2.csv
    │       movielens_batch_1748945638_3.csv
    │
    ├───dataset                     # sumber data producer
    │       movies_metadata.csv
    │       ratings_small.csv
    │
    └───models                       # Model yang dihasilkan
        ├───classification_model_set_1/
        │
        ├───classification_model_set_2/
        |
        ├───classification_model_set_3/      
        │
        ├───clustering_model_set_1/
        │
        ├───clustering_model_set_2/
        │
        ├───clustering_model_set_3/
        │
        ├───regression_model_set_1/
        │
        ├───regression_model_set_2/
        │
        └───regression_model_set_3/
```

======================================================================================================================================================================
# MENCARI DATASET DI KAGGLE

Memakai Data Movilens

(https://www.kaggle.com/datasets/rounakbanik/the-movies-dataset?select=links.csv).

# LANGKAH LANGKAH MENJALANKAN

File Docker Compose
```
version: '3'
services:
  zookeeper:
    image: confluentinc/cp-zookeeper:latest
    environment:
      ZOOKEEPER_CLIENT_PORT: 2181
      ZOOKEEPER_TICK_TIME: 2000
  kafka:
    image: confluentinc/cp-kafka:latest
    depends_on:
      - zookeeper
    ports:
      - "9092:9092"
    environment:
      KAFKA_BROKER_ID: 1
      KAFKA_ZOOKEEPER_CONNECT: 'zookeeper:2181'
      KAFKA_ADVERTISED_LISTENERS: PLAINTEXT://kafka:29092,PLAINTEXT_HOST://localhost:9092
      KAFKA_LISTENER_SECURITY_PROTOCOL_MAP: PLAINTEXT:PLAINTEXT,PLAINTEXT_HOST:PLAINTEXT
      KAFKA_INTER_BROKER_LISTENER_NAME: PLAINTEXT
      KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: 1
```

1. jalankan docker-compose dengan
   
   `docker-compose up -d`

   pastikan sudah ter-compose dengan:

   `docker ps`

   [gambar]

   jika hasilnya seperti ini:
   ![image](https://github.com/user-attachments/assets/b29a0007-cfa0-4375-a13b-b7d22fbbe40a)

   docker-compose berhasil dijalankan dilanjutkan dengan membuat topik dalam dataset kai ini membuat topik "movie ratings"

2. Jalankan Consumer dan Producer
   
   `python kafka_producer.py`

   ![Image](https://github.com/user-attachments/assets/49b6cdc7-0895-4afe-9a2d-fd31549ddb6d)

   `python kafka_producer.py`

   ![Image](https://github.com/user-attachments/assets/f917c9ed-357a-48bf-84b6-a918f8229c67)

   Dari consumer mengahasilkan 3 file dalam Batces data untuk proses train nantinya
   
   Dsni kita membuat membuat 3 model (Regression, Clustering, Classification)

3. Jalankan spark untuk membuat dan melatih model
   
   `spark-submit spark_training.py`
  

   Dari spark ini menghasilkan 3 file untuk masing-masing model

4. Jalankan api server untuk membuat endpoint
   
   `spark-submit api_server.py`

   ![Image](https://github.com/user-attachments/assets/808a63e9-a4d3-4ddb-8c23-5cdf995ebade)

   3 Endpoint yang dibuat:
   - http://localhost:5000/predict_classification
   
   ![WhatsApp Image 2025-06-05 at 05 37 40_fb88cdf3](https://github.com/user-attachments/assets/857d04a1-516d-4696-8ca2-f12c12fb11a6)

   - http://localhost:5000/predict_regression
   
   ![WhatsApp Image 2025-06-05 at 05 37 40_f8d65016](https://github.com/user-attachments/assets/710340d0-6c00-446f-b1ae-9949ff5c764d)

   - http://localhost:5000/predict_clustering
   
   ![WhatsApp Image 2025-06-05 at 05 37 21_bbd53257](https://github.com/user-attachments/assets/1eb86e57-c54c-43d2-b4f8-b7045849c528)
   

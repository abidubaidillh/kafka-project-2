# Proyek Big Data: Simulasi Streaming Data Smart Home dengan Kafka, Spark, dan API

## Overview Proyek

Proyek ini bertujuan untuk mensimulasikan arsitektur pemrosesan Big Data secara real-time menggunakan Apache Kafka untuk streaming data, Apache Spark untuk pemrosesan batch dan pelatihan model Machine Learning, serta Flask API untuk menyajikan hasil prediksi model. Data yang digunakan adalah dataset penggunaan perangkat smart home, di mana model-model yang dibangun bertujuan untuk memprediksi efisiensi perangkat, potensi kerusakan, dan melakukan segmentasi perangkat berdasarkan karakteristiknya.

**Arsitektur Sistem:**

1.  **Kafka Producer:** Membaca dataset rating user dari file (`rating_small.csv`) baris per baris dan mengirimkannya sebagai stream pesan ke Kafka Server.
2.  **Kafka Server:** Bertindak sebagai message broker yang menampung stream data.
3.  **Kafka Consumer:** Mengkonsumsi data dari Kafka Server dan menyimpan data tersebut dalam bentuk file-file batch CSV.
4.  **Apache Spark:** Membaca file-file batch CSV tersebut untuk melakukan:
    *   Preprocessing data.
    *   Melatih tiga jenis model Machine Learning secara akumulatif (setiap model dilatih ulang dengan data yang lebih banyak seiring masuknya batch baru):
        1.  **Model Klasifikasi Efisiensi Perangkat:** Memprediksi apakah perangkat efisien atau tidak.
        2.  **Model Regresi Potensi Kerusakan:** Memprediksi jumlah potensi insiden kerusakan.
        3.  **Model Clustering Perangkat:** Mengelompokkan perangkat ke dalam segmen-segmen berdasarkan karakteristiknya.
    *   Menyimpan model-model yang telah dilatih.
5.  **Flask API:** Memuat model-model Machine Learning terbaru yang telah dilatih oleh Spark dan menyediakan endpoint RESTful untuk:
    *   Menerima input data perangkat dari pengguna.
    *   Mengembalikan hasil prediksi dari ketiga model.
6.  **Streamlit UI:** Antarmuka pengguna berbasis web sederhana untuk berinteraksi dengan Flask API secara visual.

**Teknologi yang Digunakan:**
*   Apache Kafka: Untuk message queuing dan data streaming.
*   Apache Spark (PySpark): Untuk pemrosesan data batch dan pelatihan model Machine Learning.
*   Flask: Untuk membangun REST API.
*   Streamlit: Untuk membangun antarmuka pengguna (UI) interaktif.
*   Python: Bahasa pemrograman utama.
*   Docker & Docker Compose: Untuk manajemen environment Kafka dan Zookeeper.
*   Mamba/Conda: Untuk manajemen environment Python dan dependensinya.

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
  
   [gambar]

   Dari spark ini menghasilkan 3 file untuk masing-masing model

4. Jalankan api server untuk membuat endpoint
   
   `spark-submit api_server.py`

   ![Image](https://github.com/user-attachments/assets/808a63e9-a4d3-4ddb-8c23-5cdf995ebade)

   3 Endpoint yang dibuat:
   - http://localhost:5000/predict_classification
   - http://localhost:5000/predict_regression
   - http://localhost:5000/predict_clustering
   

from flask import Flask, request, jsonify
from pyspark.sql import SparkSession
from pyspark.ml import PipelineModel
from pyspark.sql.types import StructType, StructField, IntegerType, FloatType
import os

app = Flask(__name__)

# --- Konfigurasi Jalur Model ---
# Sesuaikan ini dengan BASE_PROJECT_DIR yang Anda gunakan di spark_training.py
BASE_PROJECT_DIR = '/mnt/c/Users/syahmi/OneDrive/Documents/gass.html/Insis/Kafka-2.2/Kafka-2.2'
MODEL_OUTPUT_DIR = os.path.join(BASE_PROJECT_DIR, 'models')

# Inisialisasi SparkSession di luar endpoint agar hanya dibuat sekali
# Saat aplikasi Flask dimulai
spark = SparkSession.builder \
    .appName("MovieLensModelAPI") \
    .config("spark.hadoop.io.nativeio", "false") \
    .getOrCreate()

# --- Muat Model Spark (akan dimuat saat aplikasi Flask dimulai) ---
# Penting: Pastikan nama model sesuai dengan yang Anda simpan di spark_training.py
# Contoh: kita akan memuat model dari batch terakhir, misalnya batch 1
# Anda mungkin perlu menyesuaikan 'set_1' jika Anda hanya ingin melayani batch tertentu,
# atau membuat logika untuk memilih model dari batch tertentu.

try:
    # Muat model klasifikasi
    MODEL_CLASSIFICATION_PATH = os.path.join(MODEL_OUTPUT_DIR, "classification_model_set_1")
    model_classification = PipelineModel.load(MODEL_CLASSIFICATION_PATH)
    print(f"Model Klasifikasi berhasil dimuat dari: {MODEL_CLASSIFICATION_PATH}")
except Exception as e:
    print(f"Gagal memuat Model Klasifikasi: {e}")
    model_classification = None

try:
    # Muat model regresi
    MODEL_REGRESSION_PATH = os.path.join(MODEL_OUTPUT_DIR, "regression_model_set_1")
    model_regression = PipelineModel.load(MODEL_REGRESSION_PATH)
    print(f"Model Regresi berhasil dimuat dari: {MODEL_REGRESSION_PATH}")
except Exception as e:
    print(f"Gagal memuat Model Regresi: {e}")
    model_regression = None

try:
    # Muat model clustering
    MODEL_CLUSTERING_PATH = os.path.join(MODEL_OUTPUT_DIR, "clustering_model_set_1")
    model_clustering = PipelineModel.load(MODEL_CLUSTERING_PATH)
    print(f"Model Clustering berhasil dimuat dari: {MODEL_CLUSTERING_PATH}")
except Exception as e:
    print(f"Gagal memuat Model Clustering: {e}")
    model_clustering = None

# --- Definisi Skema Data Input ---
# Ini penting untuk membuat Spark DataFrame dari data JSON yang masuk
input_schema_classification_regression = StructType([
    StructField("userId", IntegerType(), True),
    StructField("movieId", IntegerType(), True)
])

input_schema_clustering = StructType([
    StructField("userId", IntegerType(), True),
    StructField("movieId", IntegerType(), True),
    StructField("rating", FloatType(), True) # Clustering menggunakan rating juga
])

# --- Endpoint untuk Klasifikasi (Predict Like/Dislike) ---
@app.route('/predict_classification', methods=['POST'])
def predict_classification():
    if model_classification is None:
        return jsonify({"error": "Model Klasifikasi tidak tersedia."}), 500

    data = request.get_json(force=True)
    if not isinstance(data, list):
        data = [data] # Pastikan data adalah list of dictionaries

    try:
        # Konversi data JSON ke Spark DataFrame
        input_df = spark.createDataFrame(data, schema=input_schema_classification_regression)
        
        # Buat prediksi
        predictions = model_classification.transform(input_df)
        
        # Ambil hasil dan ubah ke format JSON
        # Pilih kolom yang relevan: userId, movieId, dan prediction (0 atau 1)
        results = predictions.select("userId", "movieId", "prediction").collect()
        
        output = []
        for row in results:
            output.append({
                "userId": row.userId,
                "movieId": row.movieId,
                "prediction_like": int(row.prediction) # 0 for dislike, 1 for like
            })
        
        return jsonify(output)
    except Exception as e:
        return jsonify({"error": str(e), "message": "Terjadi kesalahan saat memproses prediksi klasifikasi."}), 500

# --- Endpoint untuk Regresi (Predict Rating) ---
@app.route('/predict_regression', methods=['POST'])
def predict_regression():
    if model_regression is None:
        return jsonify({"error": "Model Regresi tidak tersedia."}), 500

    data = request.get_json(force=True)
    if not isinstance(data, list):
        data = [data] # Pastikan data adalah list of dictionaries

    try:
        # Konversi data JSON ke Spark DataFrame
        input_df = spark.createDataFrame(data, schema=input_schema_classification_regression)
        
        # Buat prediksi
        predictions = model_regression.transform(input_df)
        
        # Ambil hasil dan ubah ke format JSON
        # Pilih kolom yang relevan: userId, movieId, dan prediction (rating)
        results = predictions.select("userId", "movieId", "prediction").collect()
        
        output = []
        for row in results:
            output.append({
                "userId": row.userId,
                "movieId": row.movieId,
                "predicted_rating": round(row.prediction, 2) # Bulatkan rating ke 2 desimal
            })
        
        return jsonify(output)
    except Exception as e:
        return jsonify({"error": str(e), "message": "Terjadi kesalahan saat memproses prediksi regresi."}), 500

# --- Endpoint untuk Clustering (Predict User Cluster) ---
@app.route('/predict_clustering', methods=['POST'])
def predict_clustering():
    if model_clustering is None:
        return jsonify({"error": "Model Clustering tidak tersedia."}), 500

    data = request.get_json(force=True)
    if not isinstance(data, list):
        data = [data] # Pastikan data adalah list of dictionaries

    try:
        # Konversi data JSON ke Spark DataFrame
        input_df = spark.createDataFrame(data, schema=input_schema_clustering)
        
        # Buat prediksi
        predictions = model_clustering.transform(input_df)
        
        # Ambil hasil dan ubah ke format JSON
        # Pilih kolom yang relevan: userId, movieId, rating, dan prediction (cluster)
        results = predictions.select("userId", "movieId", "rating", "prediction").collect()
        
        output = []
        for row in results:
            output.append({
                "userId": row.userId,
                "movieId": row.movieId,
                "rating": row.rating,
                "cluster_id": int(row.prediction)
            })
        
        return jsonify(output)
    except Exception as e:
        return jsonify({"error": str(e), "message": "Terjadi kesalahan saat memproses prediksi clustering."}), 500

if __name__ == '__main__':
    # For running in a local development environment
    # Using host '0.0.0.0' so it's accessible from outside WSL (e.g., from Windows browser)
    app.run(host='0.0.0.0', port=5000, debug=False) # Change to False
    
    # Penting: Pastikan SparkSession dihentikan saat aplikasi Flask berhenti
    # Ini bisa ditangani oleh shutdown hook Spark secara otomatis,
    # tetapi secara eksplisit bisa lebih baik jika ada masalah.
    # spark.stop() # Tidak perlu dipanggil di sini karena Flask akan menangani shutdown

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when
from pyspark.ml.feature import VectorAssembler, StandardScaler
from pyspark.ml.classification import LogisticRegression
from pyspark.ml.regression import LinearRegression
from pyspark.ml.clustering import KMeans
from pyspark.ml import Pipeline
from pyspark.ml.evaluation import MulticlassClassificationEvaluator, RegressionEvaluator, ClusteringEvaluator
import os
import glob

# --- Konfigurasi Jalur ---
# PERBAIKAN PENTING: Gunakan jalur gaya Linux untuk akses WSL ke drive Windows
# Mengubah 'C:\...' menjadi '/mnt/c/...'
# Tambahkan sub-direktori 'models' untuk menyimpan output model agar rapi
BASE_PROJECT_DIR = '/mnt/c/Users/syahmi/OneDrive/Documents/gass.html/Insis/Kafka-2.2/Kafka-2.2'
BATCH_DATA_DIR = os.path.join(BASE_PROJECT_DIR, 'batched_data')
MODEL_OUTPUT_DIR = os.path.join(BASE_PROJECT_DIR, 'models') # Direktori baru untuk menyimpan model
BATCH_FILE_PATTERN = os.path.join(BATCH_DATA_DIR, 'movielens_batch_*.csv')

# Pastikan direktori model ada
if not os.path.exists(MODEL_OUTPUT_DIR):
    os.makedirs(MODEL_OUTPUT_DIR)
    print(f"Created directory: {MODEL_OUTPUT_DIR}")

def train_models():
    spark = SparkSession.builder \
        .appName("MovieLensModelTraining") \
        .config("spark.hadoop.io.nativeio", "false") \
        .getOrCreate()
    
    batch_files = sorted(glob.glob(BATCH_FILE_PATTERN))
    if not batch_files:
        print(f"No batch files found in {BATCH_DATA_DIR}. Exiting.")
        spark.stop()
        return

    accumulated_df = None

    for i, batch_file_path in enumerate(batch_files):
        print(f"\n--- Processing Batch {i+1}: {batch_file_path} ---")
        df_batch = spark.read.csv(batch_file_path, header=True, inferSchema=True)

        if 'timestamp' in df_batch.columns:
            df_batch = df_batch.drop('timestamp')  # optional

        # Tambahkan label untuk klasifikasi: like = rating >= 4
        df_batch = df_batch.withColumn("label_like", when(col("rating") >= 4.0, 1).otherwise(0).cast("integer"))

        if accumulated_df is None:
            accumulated_df = df_batch
        else:
            accumulated_df = accumulated_df.unionByName(df_batch)

        accumulated_df.cache()
        total_records = accumulated_df.count()
        print(f"Total accumulated records: {total_records}")

        if total_records < 100:
            print(f"Skipping model training for batch {i+1}, not enough data.")
            accumulated_df.unpersist()
            continue

        # --- Fitur: userId & movieId ---
        feature_cols = ["userId", "movieId"]

        # --- 1. Klasifikasi (Disukai atau tidak) ---
        print(f"\nTraining Classification Model (Set {i+1})...")
        assembler_cls = VectorAssembler(inputCols=feature_cols, outputCol="features_unscaled_cls")
        scaler_cls = StandardScaler(inputCol="features_unscaled_cls", outputCol="features_cls")
        classifier = LogisticRegression(featuresCol="features_cls", labelCol="label_like")

        pipeline_cls = Pipeline(stages=[assembler_cls, scaler_cls, classifier])
        train_cls, test_cls = accumulated_df.randomSplit([0.8, 0.2], seed=100+i)

        model_cls = pipeline_cls.fit(train_cls)
        preds_cls = model_cls.transform(test_cls)

        evaluator_cls = MulticlassClassificationEvaluator(labelCol="label_like", metricName="accuracy")
        acc = evaluator_cls.evaluate(preds_cls)
        print(f"  Classification Accuracy: {acc:.4f}")
        
        # Simpan model klasifikasi
        model_cls_path = os.path.join(MODEL_OUTPUT_DIR, f"classification_model_set_{i+1}")
        model_cls.write().overwrite().save(model_cls_path)
        print(f"  Classification model saved to: {model_cls_path}")

        # --- 2. Regresi Rating ---
        print(f"\nTraining Regression Model (Set {i+1})...")
        assembler_reg = VectorAssembler(inputCols=feature_cols, outputCol="features_unscaled_reg")
        scaler_reg = StandardScaler(inputCol="features_unscaled_reg", outputCol="features_reg")
        regressor = LinearRegression(featuresCol="features_reg", labelCol="rating")

        pipeline_reg = Pipeline(stages=[assembler_reg, scaler_reg, regressor])
        train_reg, test_reg = accumulated_df.randomSplit([0.8, 0.2], seed=200+i)

        model_reg = pipeline_reg.fit(train_reg)
        preds_reg = model_reg.transform(test_reg)

        evaluator_reg = RegressionEvaluator(labelCol="rating", metricName="rmse")
        rmse = evaluator_reg.evaluate(preds_reg)
        print(f"  Regression RMSE: {rmse:.4f}")
        
        # Simpan model regresi
        model_reg_path = os.path.join(MODEL_OUTPUT_DIR, f"regression_model_set_{i+1}")
        model_reg.write().overwrite().save(model_reg_path)
        print(f"  Regression model saved to: {model_reg_path}")

        # --- 3. Clustering pengguna ---
        print(f"\nTraining Clustering Model (Set {i+1})...")
        assembler_cluster = VectorAssembler(inputCols=feature_cols + ["rating"], outputCol="features_unscaled_cluster")
        scaler_cluster = StandardScaler(inputCol="features_unscaled_cluster", outputCol="features_cluster")
        kmeans = KMeans(featuresCol="features_cluster", k=5, seed=300+i)

        pipeline_cluster = Pipeline(stages=[assembler_cluster, scaler_cluster, kmeans])
        model_cluster = pipeline_cluster.fit(accumulated_df)

        preds_cluster = model_cluster.transform(accumulated_df)
        evaluator_cluster = ClusteringEvaluator(featuresCol="features_cluster", predictionCol="prediction")
        silhouette = evaluator_cluster.evaluate(preds_cluster)
        print(f"  Clustering Silhouette Score: {silhouette:.4f}")
        
        # Simpan model clustering
        model_cluster_path = os.path.join(MODEL_OUTPUT_DIR, f"clustering_model_set_{i+1}")
        model_cluster.write().overwrite().save(model_cluster_path)
        print(f"  Clustering model saved to: {model_cluster_path}")

        accumulated_df.unpersist()

    spark.stop()
    print("Model training complete. Spark session stopped.")

if __name__ == "__main__":
    train_models()

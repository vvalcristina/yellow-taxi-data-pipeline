from pyspark.sql import SparkSession

# Cria uma sessão Spark
spark = SparkSession.builder \
    .appName("MinIO S3A Connection Test") \
    .getOrCreate()

# Testa a leitura de um arquivo Parquet do MinIO
try:
    df = spark.read.format("parquet").load("s3a://yellow-taxi-file/yellow_tripdata_2023-05.parquet")
    df.show(5)  # Mostra as primeiras 5 linhas do DataFrame
    print("Conexão e leitura do arquivo Parquet no MinIO foram bem-sucedidas.")
except Exception as e:
    print(f"Erro ao ler o arquivo do MinIO: {e}")

# Encerra a sessão Spark
spark.stop()

import yaml
import os
import requests
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col
import logging
from .storage import Storage  # Certifique-se de que o arquivo storage.py está no mesmo diretório ou no PYTHONPATH

class IngestionPipeline:
    """Pipeline principal para ingestão de dados."""

    def __init__(self, config_path: str):
        # Carregar configurações
        self.config = self.load_config(config_path)
        self.spark_config = self.config.get('spark', {})
        self.ingestion_config = self.config.get('ingestion', {})
        self.parquet_config = self.config.get('parquet', {})
        
        # Configurar Spark
        self.spark = self.create_spark_session(self.spark_config)

        # Configurar buckets
        self.bucket_bronze = self.parquet_config.get('bucket_bronze', '')
        self.bucket_silver = self.parquet_config.get('bucket_silver', '')
        self.bucket_gold = self.parquet_config.get('bucket_gold', '')

        self.path_template = self.ingestion_config.get('path', '')

        # Configurar Storage
        self.storage = Storage(
            endpoint_url=self.spark_config['s3a']['endpoint'],
            access_key=self.spark_config['s3a']['access_key'],
            secret_key=self.spark_config['s3a']['secret_key'],
            bucket=''  # Inicialmente vazio, será definido conforme necessário
        )

        self.logger = logging.getLogger(__name__)
        
    @staticmethod
    def load_config(config_path: str):
        """Carrega as configurações do arquivo YAML."""
        with open(config_path, 'r') as file:
            return yaml.safe_load(file)
    
    @staticmethod
    def create_spark_session(config: dict) -> SparkSession:
        """Cria uma sessão Spark configurada."""
        return SparkSession.builder \
            .appName(config.get('app_name', 'DefaultApp')) \
            .config("spark.hadoop.fs.s3a.access.key", config.get('s3a', {}).get('access_key', '')) \
            .config("spark.hadoop.fs.s3a.secret.key", config.get('s3a', {}).get('secret_key', '')) \
            .config("spark.hadoop.fs.s3a.endpoint", config.get('s3a', {}).get('endpoint', '')) \
            .config("spark.hadoop.fs.s3a.path.style.access", config.get('s3a', {}).get('path_style_access', '')) \
            .config("spark.hadoop.fs.s3a.path.style.access", "true") \
            .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.2.2,com.amazonaws:aws-java-sdk-bundle:1.11.1000") \
            .getOrCreate()
    
    def download_file(self, url: str, local_path: str):
        """Baixa o arquivo de uma URL e salva localmente."""
        self.logger.info(f"Realiza request na URL {url}")
        response = requests.get(url)
        response.raise_for_status()  # Levanta um erro se o download falhar
        with open(local_path, 'wb') as file:
            file.write(response.content)
            self.logger.info(f"Arquivo gravado em {local_path}")


    def download_file_bucket(self, s3_path: str, local_path: str, bucket_name: str):
        """Baixa um arquivo do bucket S3 especificado para o caminho local."""
        self.logger.info(f"Baixa um arquivo do bucket S3 para o caminho local especificado")
        self.storage.set_bucket(bucket_name)
        self.storage.download_file(s3_path, local_path)
    
    def upload_file_bucket(self, local_path:str, bucket_name: str, s3_path:str, year: int, month: int ):
        """Baixa o arquivo de uma URL e salva no bucket S3 especificado."""
        month_str = f'{month:02d}'
        s3_path = f'yellow_tripdata_{year}-{month_str}.parquet'
        parquet_path = f"s3a://{self.bucket_silver}/year={year}/month={month_str}/yellow_tripdata.parquet"

        self.storage.set_bucket(bucket_name)
        self.storage.upload_file_bucket(local_path,bucket_name,s3_path, year,month)

        
    def process_file(self, year: int, month: int):
        self.logger.info("Processa o arquivo de um mês específico e salva no MinIO.")
        month_str = f"{month:02d}"
        
        # URL do arquivo Parquet 
        file_url = f'https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_{year}-{month_str}.parquet'
        local_path = f'/opt/airflow/tmp/yellow_tripdata_{year}-{month_str}.parquet'
        local_file_path  = f'/opt/airflow/tmp/yellow_tripdata_{year}-{month_str}.parquet'

        # Baixa o arquivo
        #self.logger.info(f"****** Baixando arquivo: {file_url} *******")
        #s3_path = f'yellow_tripdata_{year}-{month_str}.parquet'
        #self.upload_file_bucket(file_url, year, month, self.bucket_bronze)

        self.download_file(file_url, local_file_path)

        # Lê o arquivo no Spark
        try:
            df: DataFrame = self.spark.read.parquet(local_file_path)
            self.logger.info(f"DataFrame lido com sucesso")
        except Exception as e:
            self.logger.error(f"Erro ao ler o arquivo Parquet: {e}")
            return

        # Processa o DataFrame
        df_filtered = df.select(
            col("VendorID"),
            col("passenger_count"),
            col("total_amount"),
            col("tpep_pickup_datetime"),
            col("tpep_dropoff_datetime")
        )

        self.logger.info(f"Arquivo filtrado {df_filtered.columns}")
        # Define o caminho dinâmico para salvar no MinIO
        parquet_path = f"s3a://{self.bucket_silver}/year={year}/month={month_str}/yellow_tripdata.parquet"

        # Salva o DataFrame como Parquet no MinIO

        self.upload_file_bucket(local_path=local_path,bucket_name=self.bucket_silver,s3_path=parquet_path, year=year,month =month)
        #df_filtered.write.parquet(parquet_path, mode="overwrite")
        self.logger.info(f"******** Arquivo Parquet {year}-{month_str} salvo com sucesso no MinIO {parquet_path}! *******")

    def create_hive_table(self, sql_file_path: str):
        """Cria a tabela no Hive usando um arquivo SQL."""
        with open(sql_file_path, 'r') as sql_file:
            create_table_sql = sql_file.read()
        
        self.spark.sql(create_table_sql)
        print("****** Tabela Hive criada ou atualizada ******")

    def run(self):
        """Executa a ingestão e atualização da tabela Hive."""
        for year, months in self.ingestion_config.get('years_months', {}).items():
            for month in months:
                self.process_file(year, month)
        #self.create_hive_table('sql/create_table.sql')

# Execução do pipeline
#if __name__ == "__main__":
#    pipeline = IngestionPipeline('config/settings.yaml')
#    pipeline.run()

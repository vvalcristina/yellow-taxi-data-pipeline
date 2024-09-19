import yaml
from pyspark.sql import SparkSession,DataFrame
from pyspark.sql.functions import col
import logging
import requests
import os

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

        # Configurar bucket e caminho para DataProcessor
        self.bucket = self.ingestion_config.get('bucket', '')
        self.path_template = self.ingestion_config.get('path', '')

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
            .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.2.2,com.amazonaws:aws-java-sdk-bundle:1.11.1000") \
            .getOrCreate()
    
    @staticmethod
    def download_file(url: str, local_path: str):
        """Baixa o arquivo de uma URL e salva localmente."""
        response = requests.get(url)
        response.raise_for_status()  # Levanta um erro se o download falhar
        with open(local_path, 'wb') as file:
            file.write(response.content)
    
    def process_file(self, year: int, month: int):
        self.logger.info("Processa o arquivo de um mês específico e salva no MinIO.")
        month_str = f"{month:02d}"
        
        # URL do arquivo Parquet e caminho local para salvar o arquivo
        file_url = f'https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_{year}-{month_str}.parquet'
        local_file_path = f'/tmp/yellow_tripdata_{year}-{month_str}.parquet'
        #local_file_path = f's3a://bronze/yellow_tripdata_{year}-{month_str}.parquet'

        
        # Baixa o arquivo
        self.logger.info(f"****** Baixando arquivo: {file_url} *******")
        self.download_file(file_url, local_file_path)


        if not os.path.isfile(local_file_path):
            self.logger.error(f"Erro: Arquivo {local_file_path} não foi encontrado.")
            return

        self.logger.info(f"****** Finalizou download em {local_file_path}*******")

        try:
            df: DataFrame = self.spark.read.parquet(local_file_path)
            self.logger.info(f"df: {df.show(2)} *******")
        except Exception as e:
            self.logger.error(f"Erro ao ler o arquivo Parquet: {e}")
            return

        df_filtered = df.select(
            col("VendorID"),
            col("passenger_count"),
            col("total_amount"),
            col("tpep_pickup_datetime"),
            col("tpep_dropoff_datetime")
        )
        # Define o caminho dinâmico para salvar no MinIO
        parquet_path = f"s3a://{self.bucket}/year={year}/month={month_str}/yellow_tripdata.parquet"

        # Salva o DataFrame como Parquet no MinIO
        df_filtered.write.parquet(parquet_path, mode="overwrite")
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
  #  pipeline = IngestionPipeline('config/settings.yaml')
 #   pipeline.run()

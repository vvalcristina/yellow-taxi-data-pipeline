import boto3
from botocore.exceptions import NoCredentialsError, PartialCredentialsError, ClientError
import requests
import logging

class Storage:
    def __init__(self, endpoint_url: str, access_key: str, secret_key: str, bucket: str):
        self.logger = logging.getLogger(__name__)
        self.s3 = boto3.client(
            's3',
            endpoint_url=endpoint_url,
            aws_access_key_id=access_key,
            aws_secret_access_key=secret_key
        )
        self.bucket = bucket

    def set_bucket(self, bucket: str):
        """Define o bucket a ser usado nas operações."""
        self.bucket = bucket
        self.logger.info(f"bucket name: {self.bucket}")

    def download_file(self, s3_path: str, local_path: str):
        """Baixa um arquivo do bucket S3 para o caminho local especificado."""
        try:
            self.s3.download_file(Bucket=self.bucket, Key=s3_path, Filename=local_path)
            self.logger.info(f"Arquivo salvo em {local_path}")
        except NoCredentialsError:
            self.logger.error("Credenciais não encontradas para acessar o S3")
        except ClientError as e:
            self.logger.error(f"Erro ao baixar o arquivo do S3: {e}")
        except Exception as e:
            self.logger.error(f"Erro inesperado: {e}")

    def upload_file(self, url: str, s3_path: str):
        """Faz o upload de um arquivo baixado de uma URL para o bucket S3."""
        try:
            response = requests.get(url)
            response.raise_for_status()  # Verifica se o download foi bem-sucedido
        except requests.exceptions.HTTPError as err:
            self.logger.error(f"Erro ao fazer download do arquivo: {err}")
            return

        try:
            self.s3.put_object(Bucket=self.bucket, Key=s3_path, Body=response.content)
            self.logger.info(f"Arquivo salvo em s3://{self.bucket}/{s3_path}")
        except NoCredentialsError:
            self.logger.error("Credenciais não encontradas para acessar o S3")
        except ClientError as e:
            self.logger.error(f"Erro ao salvar o arquivo no S3: {e}")
        except Exception as e:
            self.logger.error(f"Erro inesperado: {e}")

    def upload_file_bucket(self, local_path: str, bucket_name: str, s3_path: str, year: int, month: int):
        """Baixa o arquivo de uma URL e faz upload para o bucket S3 especificado."""
        month_str = f'{month:02d}'  # Formata o mês com dois dígitos
        # Define o caminho correto no S3
        s3_path = f's3a://{bucket_name}/year={year}/month={month_str}/yellow_tripdata.parquet'
        
        try:
            self.set_bucket(bucket_name)  # Define o bucket para o upload
            self.s3.upload_file(Filename=local_path, Bucket=bucket_name, Key=s3_path.replace(f's3a://{bucket_name}/', ''))
            self.logger.info(f"Arquivo carregado com sucesso para {s3_path}")
        except NoCredentialsError:
            self.logger.error("Credenciais não encontradas para acessar o S3")
        except ClientError as e:
            self.logger.error(f"Erro ao fazer upload do arquivo para o S3: {e}")
        except Exception as e:
            self.logger.error(f"Erro inesperado: {e}")

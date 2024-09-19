import json
import logging
import os
from urllib.parse import urlparse

import boto3


class Storage:
    def __init__(self):
        self.s3_client = boto3.client(
            's3',
            region_name='us-east-1',
            endpoint_url=os.environ.get('MINIO_ENDPOINT'),
            aws_access_key_id=os.environ.get('MINIO_ACCESS_KEY'),
            aws_secret_access_key=os.environ.get('MINIO_SECRET_KEY')
        )

    def __decompose_s3_path(self, s3_path: str) -> tuple:
        parsed_url = urlparse(s3_path)
        bucket = parsed_url.netloc
        key = parsed_url.path.lstrip('/')
        return bucket, key

    def save_file_to_bucket(self, data: dict, path: str, file_type: str = 'json'):
            """Salva arquivos no bucket, podendo ser JSON ou outros formatos."""
            logging.info('Saving file to bucket...')
            
            # Definir o nome do arquivo com base no tipo
            filename = f'file.{file_type}'
            path_with_filename = path + filename
            
            # Extrair bucket e key
            bucket, key = self.__decompose_s3_path(s3_path=path_with_filename)
            
            # Verificar tipo do arquivo e salvar apropriadamente
            if file_type == 'json':
                self.s3_client.put_object(Bucket=bucket, Key=key, Body=json.dumps(data))
            else:
                self.s3_client.put_object(Bucket=bucket, Key=key, Body=data)
            
            logging.info('File successfully saved!')
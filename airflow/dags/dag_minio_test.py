from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from datetime import datetime

# Definindo argumentos padrão da DAG
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 9, 20),
    'depends_on_past': False,
    'retries': 1,
}

# Criando a DAG
with DAG(
    dag_id='spark_minio_test',
    default_args=default_args,
    schedule_interval=None,  # Executar manualmente
    catchup=False,
) as dag:

    # Operador Spark para executar o script PySpark
    spark_task = SparkSubmitOperator(
        application='/opt/airflow/dags/minio_test.py',  # Caminho do seu script PySpark
        task_id='spark_minio_test_task',
        conn_id='spark_default',  # Isso deve estar configurado no Airflow
        verbose=True,
        conf={
            'spark.hadoop.fs.s3a.access.key': 'minioadmin',
            'spark.hadoop.fs.s3a.secret.key': 'minioadmin123',
            'spark.hadoop.fs.s3a.endpoint': 'http://minio:9000',
            'spark.hadoop.fs.s3a.connection.ssl.enabled': 'false'
        }
    )

    spark_task

from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
from util.ingestion import IngestionPipeline
import os

# Definir argumentos padrão da DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 9, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Função para inicializar o pipeline
def run_pipeline():
    config_path = os.path.join(os.path.dirname(__file__), '../config/settings.yaml')
    pipeline = IngestionPipeline(config_path)
    pipeline.run()

# Definir a DAG
with DAG('dag_ingestion_nyc',
         default_args=default_args,
         description='Atualiza a tabela no Data Lake diariamente',
         schedule_interval=timedelta(days=1),
         catchup=False) as dag:
    
    # Tarefa para executar o pipeline
    run_pipeline_task = PythonOperator(
        task_id='run_pipeline',
        python_callable=run_pipeline
    )

    run_pipeline_task
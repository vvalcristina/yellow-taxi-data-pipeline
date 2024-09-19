FROM apache/airflow:2.9.2-python3.11

USER root

# Instalar JDK 17
RUN apt-get update && \
    apt-get install -y openjdk-17-jdk && \
    apt-get clean;

# Configurar variáveis de ambiente para Java
ENV JAVA_HOME /usr/lib/jvm/java-17-openjdk-amd64

COPY requirements.txt .
COPY airflow/dags/config/settings.yaml /opt/airflow/config/settings.yaml


USER airflow
RUN pip install -r requirements.txt

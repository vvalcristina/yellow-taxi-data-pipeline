FROM apache/airflow:2.9.2-python3.11

USER root

# Criar o diretório e definir permissões
RUN mkdir -p /opt/airflow/temp && \
    groupadd -r airflow || true && \
    useradd -r -g airflow airflow || true && \
    chown -R airflow:airflow /opt/airflow/temp


# Instalar JDK 17
RUN apt-get update && \
    apt-get install -y openjdk-17-jdk && \
    apt-get clean;

# Configurar variáveis de ambiente para Java
ENV JAVA_HOME /usr/lib/jvm/java-17-openjdk-amd64

COPY requirements.txt .
COPY airflow/dags/config/settings.yaml /opt/airflow/config/settings.yaml
# Ajustar permissões para o usuário airflow


USER airflow
RUN pip install -r requirements.txt

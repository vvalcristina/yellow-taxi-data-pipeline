# Dockerfile para Spark
FROM bitnami/spark:latest

USER root

# Instala curl
RUN apt-get update && apt-get install -y curl

# Adiciona o JAR do Hadoop
RUN curl -o /opt/bitnami/spark/jars/hadoop-aws.jar https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar

# Voltar para o usuário padrão
USER 1001

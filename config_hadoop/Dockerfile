# Use uma imagem base do Ubuntu
FROM ubuntu:20.04

# Instale dependências
RUN apt-get update && apt-get install -y \
    wget \
    openjdk-11-jdk \
    tar \
    bash

# Baixe e instale o Hadoop
ENV HADOOP_VERSION 3.3.1
RUN wget https://archive.apache.org/dist/hadoop/common/hadoop-$HADOOP_VERSION/hadoop-$HADOOP_VERSION.tar.gz && \
    tar -xzvf hadoop-$HADOOP_VERSION.tar.gz && \
    mv hadoop-$HADOOP_VERSION /usr/local/hadoop && \
    rm hadoop-$HADOOP_VERSION.tar.gz

# Configure o Hadoop
ENV HADOOP_HOME /usr/local/hadoop
ENV PATH $PATH:$HADOOP_HOME/bin

# Defina o diretório de trabalho
WORKDIR /usr/local/hadoop

# Comando padrão para o contêiner
CMD [ "bash" ]

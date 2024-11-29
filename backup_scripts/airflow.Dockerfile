FROM apache/airflow:2.10.3-python3.12

USER root
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
      sudo \
      curl \
      vim \
      unzip \
      rsync \
      openjdk-17-jdk \
      build-essential \
      software-properties-common \
      ssh && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

FROM bitnami/spark as spark-base
FROM spark-base as pyspark
FROM pyspark

# Set JAVA_HOME environment variable
ENV JAVA_HOME /usr/lib/jvm/java-17-openjdk-amd64/
ENV SPARK_HOME=${SPARK_HOME:-"/opt/bitnami/spark"}
ENV HADOOP_HOME=${HADOOP_HOME:-"/opt/bitnami/spark/hadoop"}
ENV SPARK_VERSION=3.5.3
#ENV PYTHONPATH=$SPARK_HOME/python/:$PYTHONPATH
ENV PYTHONPATH=$SPARK_HOME/python/:$SPARK_HOME/python/lib/py4j-*-src.zip:$PYTHONPATH

ENV PATH="/opt/bitnami/spark/sbin:/opt/bitnami/spark/bin:${PATH}"

RUN mkdir -p ${HADOOP_HOME} && mkdir -p ${SPARK_HOME}
WORKDIR ${SPARK_HOME}

# Download spark
RUN mkdir -p ${SPARK_HOME} \
    && curl https://dlcdn.apache.org/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop3.tgz -o spark-${SPARK_VERSION}-bin-hadoop3.tgz \
    && tar xvzf spark-${SPARK_VERSION}-bin-hadoop3.tgz --directory /opt/bitnami/spark --strip-components 1 \
    && rm -rf spark-${SPARK_VERSION}-bin-hadoop3.tgz

USER airflow

# RUN pip install apache-airflow apache-airflow-providers-apache-spark pyspark
COPY ../requirements.txt .
RUN pip3 install -r requirements.txt

# Download delta jars
RUN curl https://repo1.maven.org/maven2/io/delta/delta-core_2.12/2.4.0/delta-core_2.12-2.4.0.jar -Lo /opt/bitnami/spark/jars/delta-core_2.12-2.4.0.jar
RUN curl https://repo1.maven.org/maven2/io/delta/delta-spark_2.12/3.2.1/delta-spark_2.12-3.2.1.jar -Lo /opt/bitnami/spark/jars/delta-spark_2.12-3.2.1.jar
RUN curl https://repo1.maven.org/maven2/io/delta/delta-storage/3.2.0/delta-storage-3.2.0.jar -Lo /opt/bitnami/spark/jars/delta-storage-3.2.0.jar

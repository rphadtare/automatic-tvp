FROM bitnami/spark as spark_base

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

## Download spark and hadoop dependencies and install

# ENV variables
ENV JAVA_HOME /usr/lib/jvm/java-17-openjdk-amd64/
ENV SPARK_HOME=${SPARK_HOME:-"/opt/bitnami/spark"}
ENV HADOOP_HOME=${HADOOP_HOME:-"/opt/bitnami/spark/hadoop"}
ENV SPARK_VERSION=3.5.3
ENV PYTHONPATH=$SPARK_HOME/python/:$PYTHONPATH

ENV PATH="/opt/bitnami/spark/sbin:/opt/bitnami/spark/bin:${PATH}"
ENV SPARK_HOME="/opt/bitnami/spark"
ENV SPARK_MASTER="spark://spark-master:7077"
ENV SPARK_MASTER_HOST spark-master
ENV SPARK_MASTER_PORT 7077
ENV PYSPARK_PYTHON python3

# Add iceberg spark runtime jar to IJava classpath
ENV IJAVA_CLASSPATH=/opt/bitnami/spark/jars/*

RUN mkdir -p ${HADOOP_HOME} && mkdir -p ${SPARK_HOME}
WORKDIR ${SPARK_HOME}

# Download spark
RUN mkdir -p ${SPARK_HOME} \
    && curl https://dlcdn.apache.org/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop3.tgz -o spark-${SPARK_VERSION}-bin-hadoop3.tgz \
    && tar xvzf spark-${SPARK_VERSION}-bin-hadoop3.tgz --directory /opt/bitnami/spark --strip-components 1 \
    && rm -rf spark-${SPARK_VERSION}-bin-hadoop3.tgz


FROM spark-base as pyspark

# Install python deps
COPY requirements.txt .
RUN pip3 install -r requirements.txt

COPY conf/spark-defaults.conf "$SPARK_HOME/conf"
COPY spark_scripts/* /opt/bitnami/spark/spark-delta-scripts/

RUN chmod u+x /opt/bitnami/spark/sbin/* && \
    chmod u+x /opt/bitnami/spark/bin/*


FROM pyspark

# Download delta jars
RUN curl https://repo1.maven.org/maven2/io/delta/delta-core_2.12/2.4.0/delta-core_2.12-2.4.0.jar -Lo /opt/bitnami/spark/jars/delta-core_2.12-2.4.0.jar
RUN curl https://repo1.maven.org/maven2/io/delta/delta-spark_2.12/3.2.1/delta-spark_2.12-3.2.1.jar -Lo /opt/bitnami/spark/jars/delta-spark_2.12-3.2.1.jar
RUN curl https://repo1.maven.org/maven2/io/delta/delta-storage/3.2.0/delta-storage-3.2.0.jar -Lo /opt/bitnami/spark/jars/delta-storage-3.2.0.jar


#COPY backup_scripts/entrypoint.sh .
#RUN chmod u+x /opt/bitnami/spark/entrypoint.sh
#
#ENTRYPOINT ["./entrypoint.sh"]
#CMD [ "bash" ]
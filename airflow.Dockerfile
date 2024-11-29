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

## Download spark and hadoop dependencies and install

# ENV variables
ENV JAVA_HOME /usr/lib/jvm/java-17-openjdk-arm64
RUN export JAVA_HOME

# Install python deps
COPY requirements.txt .
RUN pip3 install -r requirements.txt

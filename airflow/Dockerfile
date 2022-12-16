FROM apache/airflow:2.4.3

USER root

# Install OpenJDK-11
RUN apt update && \
    apt-get install -y openjdk-17-jdk && \
    apt-get install -y ant && \
    apt-get clean;

# Set JAVA_HOME
ENV JAVA_HOME /usr/lib/jvm/java-17-openjdk-amd64/
RUN export JAVA_HOME

USER airflow

RUN pip install pyspark==3.3.1 apache-airflow-providers-apache-spark



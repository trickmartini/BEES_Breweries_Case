FROM apache/airflow:latest

USER root
# Install Java
RUN apt-get update && \
    apt-get install -y openjdk-17-jdk && \
    apt-get clean

# Switch back to the airflow user for further installations
USER airflow

# Install the required Python packages
RUN pip install apache-airflow-providers-apache-spark pyspark

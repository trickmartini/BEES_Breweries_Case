FROM apache/airflow:latest

USER root
# Install Java
RUN apt-get update && \
    apt-get install -y openjdk-17-jdk && \
    apt-get clean

# Set JAVA_HOME environment variable
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-arm64
ENV PATH="${JAVA_HOME}/bin:${PATH}"

# Switch back to the airflow user for further installations
USER airflow

# Install the required Python packages
RUN pip install apache-airflow-providers-apache-spark pyspark

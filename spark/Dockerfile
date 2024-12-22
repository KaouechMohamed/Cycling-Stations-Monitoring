# Use the Bitnami Spark image as the base
FROM bitnami/spark:3.2.4

# Switch to root to install packages
USER root

# Install required system utilities, Python, and Scala
RUN apt-get update && \
    apt-get install -y python3-pip curl telnet && \
    rm -rf /var/lib/apt/lists/* && \
    curl -LO https://downloads.lightbend.com/scala/2.12.15/scala-2.12.15.tgz && \
    tar -xzvf scala-2.12.15.tgz -C /opt/ && \
    rm scala-2.12.15.tgz && \
    mv /opt/scala-2.12.15 /opt/scala

# Set environment variables for Scala
ENV SCALA_HOME=/opt/scala \
    PATH=$PATH:/opt/scala/bin

# Install Python libraries for Cassandra and Elasticsearch
RUN pip install --no-cache-dir cassandra-driver==3.28.0 elasticsearch==8.15.1

# Switch back to the default user
USER 1001

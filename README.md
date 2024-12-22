# Realtime Data Streaming Of Cycling Stations in Europe | End-to-End Data Engineering Project

## Table of Contents
- [Introduction](#introduction)
- [System Architecture](#system-architecture)
- [Technologies](#technologies)
- [Getting Started](#getting-started)


## Introduction

This project serves as a comprehensive guide to building an end-to-end data engineering pipeline. It covers each stage from data ingestion to processing and finally to storage, utilizing a robust tech stack that includes Python, Apache Kafka, Apache Zookeeper, Apache Spark, and Cassandra. Everything is containerized using Docker for ease of deployment and scalability.

## System Architecture

![System Architecture](https://github.com/airscholar/e2e-data-engineering/blob/main/Data%20engineering%20architecture.png)

The project is designed with the following components:

- **Data Source**: Using 'jcdecaux.com' API to collect cycling stations data from different countries of Europe for our pipeline.
- **Apache Kafka and Zookeeper**: Used for streaming data from the data source to the processing engine.
- **Apache Spark**: For data processing with its master and worker nodes.
- **Cassandra**: Where the processed data will be stored.

## Technologies

- Python
- Apache Kafka
- Apache Zookeeper
- Apache Spark
- Cassandra
- Docker

## Getting Started

1. Clone the repository:
    ```bash
    git clone https://github.com/KaouechMohamed/Cycling-Stations-Monitoring.git
    ```

2. Navigate to the project directory:
    ```bash
    cd Cycling-Stations-Monitoring
    ```

3. Run Docker Compose to spin up the services:
    ```bash
    docker-compose up -d
    ```
4. Run requirements file to install kafka for python (ensure to create a virtualenv ):
    ```bash
    pip install -r requirements.txt
    ```
5. Run the kafka producer that fetch the api (make sure to have an ENV file that contains your api key API_KEY=XXXXXXXXXXXX):
    ```bash
    python3 producer.py
    ```
6. copy the pyspark consumer code into the spark master container :
    ```bash
    docker cp ./stream_consumer.py spark_master:/opt/bitnami/spark/ 
    ```
7. submit the spark job into the spark master and include the corresponding JARS :
    ```bash
     docker-compose exec spark-master spark-submit --class consumer --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.4,com.datastax.spark:spark-cassandra-connector_2.12:3.2.0,commons-httpclient:commons-httpclient:3.1 stream_consumer.py
    ```
8. insure that the data is successfully inserted into cassandra :
    ```bash
    docker exec -it cassandra cqlsh -u cassandra -p cassandra localhost 9042
    ```
   ```bash
    SELECT * FROM spark_streams.stations;
    ```




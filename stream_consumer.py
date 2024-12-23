import logging
from cassandra.cluster import Cluster
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType,IntegerType, FloatType

logging.basicConfig(level=logging.INFO)


def create_keyspace(session):
    try:
        session.execute("""
            CREATE KEYSPACE IF NOT EXISTS spark_streams
            WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1'};
        """)
        logging.info("Keyspace created successfully!")
    except Exception as e:
        logging.error(f"Error creating keyspace: {e}")


def create_table(session):
    try:
        session.execute("""
        CREATE TABLE IF NOT EXISTS spark_streams.stations (
            number INT PRIMARY KEY,
            contract_name TEXT,
            station_name TEXT,
            address TEXT,
            lattitude float,
            longitude float,
            status TEXT,
            total_bike_stands INT,
            available_bike_stands INT,
            available_bikes INT,
            last_update TEXT);
        """)
        logging.info("Table created successfully!")
    except Exception as e:
        logging.error(f"Error creating table: {e}")

def create_spark_connection():
    try:
        s_conn = SparkSession.builder \
            .appName('SparkDataStreaming') \
            .master("spark://spark-master:7077") \
            .config('spark.cassandra.connection.host', 'cassandra') \
            .getOrCreate()

        s_conn.sparkContext.setLogLevel("ERROR")
        logging.info("Spark connection created successfully!")
        return s_conn
    except Exception as e:
        logging.error(f"Couldn't create the spark session due to exception {e}")
        return None


def connect_to_kafka(spark_conn):
    try:
        spark_df = spark_conn.readStream \
            .format('kafka') \
            .option('kafka.bootstrap.servers', 'kafka:29092') \
            .option('subscribe', 'stations') \
            .option('startingOffsets', 'earliest') \
            .load()
        logging.info("Kafka dataframe created successfully")
        return spark_df
    except Exception as e:
        logging.error(f"Kafka dataframe could not be created because: {e}")
        return None


def create_cassandra_connection():
    try:
        cluster = Cluster(['cassandra'])
        cas_session = cluster.connect()
        return cas_session
    except Exception as e:
        logging.error(f"Could not create Cassandra connection due to {e}")
        return None


def create_selection_df_from_kafka(spark_df):
    schema = StructType([
    StructField("number", IntegerType(), False),
    StructField("contract_name", StringType(), False),
    StructField("station_name", StringType(), False),
    StructField("address", StringType(), False),
    StructField("lattitude", FloatType(), False),
    StructField("longitude", FloatType(), False),
    StructField("status", StringType(), False),
    StructField("total_bike_stands", IntegerType(), False),
    StructField("available_bike_stands", IntegerType(), False),
    StructField("available_bikes", IntegerType(), False),
    StructField("last_update", StringType(), False)
    ])
    try:
        sel = spark_df.selectExpr("CAST(value AS STRING)") \
            .select(from_json(col('value'), schema).alias('data')).select("data.*")
        return sel
    except Exception as e:
        logging.error(f"Error creating selection DataFrame: {e}")
        return None


def main():
    # Create spark connection
    spark_conn = create_spark_connection()

    if spark_conn is not None:
        # Connect to Kafka
        spark_df = connect_to_kafka(spark_conn)
        if spark_df is not None:
            # Create selection DataFrame
            selection_df = create_selection_df_from_kafka(spark_df)
            if selection_df is not None:
                # Create Cassandra connection
                session = create_cassandra_connection()
                if session is not None:
                    create_keyspace(session)
                    create_table(session)
                    logging.info("Streaming is being started...")
                    streaming_query = (selection_df.writeStream
                                       .format("org.apache.spark.sql.cassandra")
                                       .option('checkpointLocation', '/tmp/checkpoint')
                                       .option('keyspace', 'spark_streams')
                                       .option('table', 'stations')
                                       .start())
                    streaming_query.awaitTermination()
                else:
                    logging.error("Failed to create Cassandra session")
            else:
                logging.error("Failed to create selection DataFrame")
        else:
            logging.error("Failed to connect to Kafka")
    else:
        logging.error("Failed to create Spark connection")


if __name__ == "__main__":
    main()
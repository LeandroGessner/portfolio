import logging as log
import pyspark.sql.functions as f
from pyspark.sql import SparkSession
from pyspark.sql import DataFrame


# Kafka Configuration
KAFKA_BROKERS = 'kafka0:29092'
KAFKA_CHECKPOINT = 'checkpoint'

# App Configuration
ACME_PYSPARK_APP_NAME = 'AcmeSparkStreaming'
CLICKSTREAM_RAW_EVENTS_TOPIC = 'acme.clickstream.raw.events'
CLICKSTREAM_LATEST_EVENTS_TOPIC = 'acme.clickstream.latest.events'
CLICKSTREAM_RAW_EVENTS_SUBJECT = f'{CLICKSTREAM_RAW_EVENTS_TOPIC}-value'

packages = [
    'org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0',
    'org.apache.spark:spark-avro_2.12:3.3.0'
]

# Initialize logging
log.basicConfig(
    level=log.INFO,
    format='%(asctime)s [%(levelname)s] [%(name)8s] %(message)s'
)
logger = log.getLogger('acme_pyspark')


def initialize_spark_session(app_name: str):
    """
    Initialize the Spark Session with provided configurations.

    :param app_name: Name of the spark application.
    :return: Spark session object or None if there's an error.
    """
    try:
        spark = SparkSession.builder \
            .appName(app_name) \
            .master('spark://spark-master:7077') \
            .config('spark.jars.packages', ','.join(packages)) \
            .getOrCreate()

        spark.sparkContext.setLogLevel("WARN")
        logger.info('Spark session initialized successfully')
        return spark

    except Exception as e:
        logger.error(f"Spark session initialization failed. Error: {e}")
        return None


def get_streaming_dataframe(
        spark: SparkSession, 
        brokers, 
        topic
) -> DataFrame:
    """
    Get a streaming dataframe from Kafka.

    :param spark: Initialized Spark session.
    :param brokers: Comma-separated list of Kafka brokers.
    :param topic: Kafka topic to subscribe to.
    :return: Dataframe object or None if there's an error.
    """
    streaming_df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "localhost:9092") \
        .load()
    
    return streaming_df


def transform_streaming_data(df: DataFrame):
    """
    Transform the initial dataframe to get the final structure.

    :param df: Initial dataframe with raw data.
    :return: Transformed dataframe.
    """

    return df


def initiate_streaming_to_topic(
        df: DataFrame, 
        brokers, 
        topic, 
        checkpoint
):
    """
    Start streaming the transformed data to the specified Kafka topic.

    :param df: Transformed dataframe.
    :param path: S3 bucket path.
    :param checkpoint_location: Checkpoint location for streaming.
    :return: None
    """
    logger.info("Initiating streaming process...")


def main():
    spark = initialize_spark_session(ACME_PYSPARK_APP_NAME)
    
    if spark:
        df = get_streaming_dataframe(spark, KAFKA_BROKERS, CLICKSTREAM_RAW_EVENTS_TOPIC)
        
        if df:
            transformed_df = transform_streaming_data(df)
            initiate_streaming_to_topic(
                transformed_df, 
                KAFKA_BROKERS, 
                CLICKSTREAM_LATEST_EVENTS_TOPIC, 
                KAFKA_CHECKPOINT
            )


# Execute the main function if this script is run as the main module
if __name__ == '__main__':
    main()

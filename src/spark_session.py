from pyspark.sql import SparkSession

def get_spark_session():
    """
    Creates or retrieves the existing Spark session.

    :return: SparkSession
    """
    return SparkSession.builder \
        .appName("Flights-In-Sea") \
        .getOrCreate()

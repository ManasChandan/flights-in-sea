from pyspark.sql import SparkSession
from sedona.register import SedonaRegistrator
from sedona.utils import SedonaKryoRegistrator, KryoSerializer


def get_spark_session():
    """
    Creates or retrieves the existing Spark session.

    :return: SparkSession
    """

    sedona_jar_path = "C:\sedona\sedona-spark-shaded-3.4_2.12-1.5.0.jar"

    spark =  SparkSession.builder \
        .appName("Flights-In-Sea") \
        .config("spark.jars", sedona_jar_path) \
        .config("spark.serializer", KryoSerializer.getName) \
        .config("spark.kryo.registrator", SedonaKryoRegistrator.getName) \
        .getOrCreate()

    SedonaRegistrator.registerAll(spark)

    return spark
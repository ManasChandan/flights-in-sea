from src.spark_session import get_spark_session

def test_spark_session():
    """
    Tests the spark session
    :return: None
    """
    spark_session = get_spark_session()
    spark_session.stop()
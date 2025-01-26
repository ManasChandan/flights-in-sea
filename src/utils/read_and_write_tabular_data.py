from src.spark_session import get_spark_session
from pyspark.sql import DataFrame

def read_csv_file(file_path, columns=None):
    """
    Reads a CSV file using the Spark session and selects specified columns if provided.

    :param file_path: Path to the CSV file
    :param columns: List of columns to select (optional)
    :return: Spark DataFrame
    """

    # Getting the spark session
    spark = get_spark_session()

    # reading the data
    df = spark.read.csv(file_path, header=True, inferSchema=True)

    # Only acquiring the required columns
    if columns:
        df = df.select(*columns)

    return df

def save_df_to_csv(df : DataFrame, output_path : str):
    """
    Saves a Spark DataFrame to a CSV file.

    I didn't want to do a hadoop setup now, so converting the same first to pandas then doing the operations.

    :param df: Spark DataFrame to save
    :param output_path: Path to save the CSV file
    """

    # Saves the csv file
    df.toPandas().to_csv(output_path, index=False)

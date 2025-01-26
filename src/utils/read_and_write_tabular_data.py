import os
import tarfile
import tempfile
from src.spark_session import get_spark_session
from pyspark.sql import DataFrame

def read_csv_file(file_path : str, columns=None) -> DataFrame:
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

def read_csv_from_tar(tar_file_path : str, columns : list | None = None) -> DataFrame:
    """
    Reads a csv file from the tar file. The csv is compressed using tar file.

    :param tar_file_path: The tar file path.
    :param columns: Only specific columns to read
    :return: The read Dataframe
    """
    # Extracting the tar file
    with tempfile.TemporaryDirectory() as temp_dir:
        with tarfile.open(tar_file_path, 'r') as tar_ref:
            tar_ref.extractall(path=temp_dir)

        # Creating the path for the csv file
        csv_file_path = os.path.join(temp_dir, f"{os.path.basename(tar_file_path).split('.')[0]}.csv.gz")

        # reading the csv file
        df = read_csv_file(csv_file_path, columns)

    return df

def save_df_to_csv(df : DataFrame, output_path : str) -> None:
    """
    Saves a Spark DataFrame to a CSV file.

    I didn't want to do a hadoop setup now, so converting the same first to pandas then doing the operations.

    :param df: Spark DataFrame to save
    :param output_path: Path to save the CSV file
    """

    # Saves the csv file
    df.toPandas().to_csv(output_path)

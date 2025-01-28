import os
import tarfile
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

def extract_tar_file(tar_file_path : str):
    """
    Extracts a tar file and saves the content in some location.

    :param tar_file_path: The tar file path.
    :return: The folder path where the  tar file is extracted.
    """

    # Creating the folder path
    folder_path_for_extraction = os.path.join(
        os.path.dirname(tar_file_path), os.path.basename(tar_file_path).split('.')[0]
    )

    # Folder creation
    if not os.path.exists(folder_path_for_extraction):
        os.makedirs(folder_path_for_extraction, exist_ok=True)

    print(folder_path_for_extraction)

    # Extraction of the tar file
    with tarfile.open(tar_file_path, 'r') as tar_ref:
      tar_ref.extractall(folder_path_for_extraction)

    return folder_path_for_extraction

def read_csv_from_tar(tar_file_path : str, columns : list | None = None) -> DataFrame:
    """
    Reads a csv file from the tar file. The csv is compressed using tar file.

    :param tar_file_path: The tar file path.
    :param columns: Only specific columns to read
    :return: The read Dataframe
    """

    # Extract the .tar file
    extracted_folder_path = extract_tar_file(tar_file_path)

    # csv_file_path
    csv_file_path = os.path.join(
        extracted_folder_path, f"{os.path.basename(tar_file_path).split('.')[0]}.csv.gz"
    )

    # reading the csv file
    df = read_csv_file(csv_file_path, columns)

    return df

def save_df_to_csv(df : DataFrame, output_path : str) -> None:
    """
    Saves a Spark DataFrame to a CSV file.

    :param df: Spark DataFrame to save
    :param output_path: Path to save the CSV file
    """

    # Saves the csv file
    df.toPandas().to_csv(output_path)

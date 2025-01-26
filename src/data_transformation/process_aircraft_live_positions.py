from src.utils.read_and_write_tabular_data import *
from constants.model_run_params import *
from pyspark.sql import functions as F

def read_aircraft_positions_data(positions_tar_file_path : str) -> DataFrame:
    """
    Read the aircraft positions data

    :param positions_tar_file_path: The file path for the tar file
    :return: The read dataframe.
    """

    # Reading the positions data
    pos_data = read_csv_file(positions_tar_file_path, columns=POSITIONS_COLUMNS)

    # return the pos_data
    return pos_data

def position_data_cleaning(pos_data : DataFrame) -> DataFrame:
    """
    Cleans the positions data.

    :param pos_data: The position dataframe.
    :return: Cleaned positions data.
    """
    # Drop nulls for lat, lon and icoa24
    pos_data = pos_data.dropna(subset=["lat", "lon", "icao24"])

    # Considering only On Air Datapoints
    pos_data = pos_data.filter(F.col("onground") == False)

    # Fill lastposupdate with time and drop both columns
    pos_data = pos_data.withColumn(
        "record_time", F.when(
            (F.col("lastposupdate").isNull()) | (F.col("lastposupdate") == ""), F.col("time")
        ).otherwise(F.col("lastposupdate"))
    ).drop("time", "lastposupdate")

    return pos_data

def process_aircraft_live_positions(tar_file_path : str) -> DataFrame:
    """
    Reads and process the airplane positions data.

    :param tar_file_path: The compressed file path of the csv.tar file.
    :return:
    """

    # Reads the data
    pos_data = read_aircraft_positions_data(tar_file_path)

    # Cleans the positions data
    pos_data = position_data_cleaning(pos_data)

    # Return the positions data
    return pos_data

file_path = r"D:\Work\Projects\flights-in-sea\Datasets\states_2021-05-10-00.csv"
pos_data = process_aircraft_live_positions(file_path)
pos_data = pos_data.limit(100)
pos_data.toPandas().to_csv(r"here.csv")
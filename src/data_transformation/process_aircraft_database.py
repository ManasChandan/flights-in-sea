from pyspark.sql import DataFrame, functions as F
from src.utils.read_and_write_tabular_data import *
from src.constants.model_run_params import *

def read_aircraft_database_file():
    """
    Reads the aircraft database file from the local system.

    :return:
    """

    # Reads the file
    aircraft_db = read_csv_file(AIRCRAFT_DATABASE_PATH,
                                columns=AIRCRAFT_COLUMNS)
    # Returns the read csv
    return aircraft_db

def process_aircraft_data(aircraft_db : DataFrame) -> DataFrame:
    """
    Processed the aircraft db, merges the manufacture details into a single column.

    :param aircraft_db: read aircraft db from the local system
    :return: processed aircraft table
    """

    # Manufacturer details column
    cols_to_merge = ["manufacturericao", "manufacturername"]

    # Merge the columns using the SQL concat ws functions
    merged_col = F.concat_ws(" | ", *[F.col(col) for col in cols_to_merge])

    # Drop the other columns after being merged
    aircraft_db = aircraft_db.withColumn("Manufacture Details", merged_col).drop(*cols_to_merge)

    # Drop any empty string values
    aircraft_db = aircraft_db.filter(F.col("Manufacture Details") != "")

    # Drop any Null values
    return aircraft_db.dropna()

def process_aircraft_database() -> DataFrame:
    """
    Function to read and process the aircraft database.

    Processing is merging the manufacturer details

    :return: The read and processed aircraft database in a Pyspark SQL Dataframe
    """

    # Read the csv for aircraft db
    aircraft_db = read_aircraft_database_file()

    # Process the aircraft_db
    aircraft_db = process_aircraft_data(aircraft_db)

    return aircraft_db

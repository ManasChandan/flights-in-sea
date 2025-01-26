from src.data_transformation import process_aircraft_database
from src.utils.read_and_write_tabular_data import save_df_to_csv

def test_process_aircraft_database():
    """
    Test to see the working of processing aircraft data.

    Saves the processed database locally to the test Dataset folders

    :return: None
    """
    aircraft_db = process_aircraft_database.process_aircraft_database()
    save_df_to_csv(aircraft_db, r"D:\Work\Projects\flights-in-sea\tests\Datasets\aircraft_clean_database.csv")
from src.data_transformation import process_aircraft_live_positions
from src.utils.read_and_write_tabular_data import save_df_to_csv

def test_process_aircraft_live_positions():
    """
    Tests the functionality for the aircraft live positions.

    :return: None
    """
    file_path = r"D:\Work\Projects\flights-in-sea\Datasets\Zenodo OpenSky Data\states_2021-05-10-01.csv.tar"
    pos_data = process_aircraft_live_positions.process_aircraft_live_positions(file_path)
    save_df_to_csv(pos_data.limit(100), r"D:\Work\Projects\flights-in-sea\tests\Datasets\aircraft_live.csv")

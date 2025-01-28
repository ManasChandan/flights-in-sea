import pandas as pd
import geopandas as gpd

def read_shp_file(file_path : str,
                  save_file_path : str) -> None:
    """
    The function reads a shp file and writes the same to a csv file.

    :param file_path: The path of the shp file
    :param save_file_path: The path for the csv file (include the name of the csv file)
    :return: None
    """
    # Reading the shp file
    geo_data = gpd.read_file(file_path)

    # Converting the geometry of the geo data to well known text
    geo_data["geometry"] = geo_data["geometry"].apply(lambda x : x.wkt)

    # Saving the geo data as a csv file
    geo_data.to_csv(save_file_path, index=False)

def read_shp_file_and_save_kml(file_path : str,
                  save_file_path : str) -> None:
    """
    The function reads a shp file and writes the same to a csv file.

    :param file_path: The path of the shp file
    :param save_file_path: The path for the kml file (include the name of the kml file)
    :return: None
    """
    # Reading the shp file
    geo_data = gpd.read_file(file_path)

    # Converting the geometry of the geo data to well known text
    geo_data["geometry"] = geo_data["geometry"].apply(lambda x : x.wkt)

    # Saving the geo data as a csv file
    geo_data.to_file(save_file_path, driver="KML")

def read_csv_with_wkt(file_path : str) -> gpd.GeoDataFrame:
    """
    Reads a csv file that has a wkt in the geometry named column

    :param file_path: File path of the csv file
    :return: The read geo pandas dataframe
    """
    # Reading the csv file
    geo_data = pd.read_csv(file_path)

    # Converting the pandas dataframe to geo pandas dataframe
    geo_data = gpd.GeoDataFrame(geo_data,
                                geometry=gpd.GeoSeries.from_wkt(geo_data['geometry']))

    # Setting the crs
    geo_data.set_crs("EPSG:4326")

    return geo_data
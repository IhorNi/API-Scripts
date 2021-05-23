import googlemaps
import time
import pandas as pd
import numpy as np
from pandas import DataFrame

from GetGoogleAPIKey import get_my_key


def get_place_location(file_adrress):

    """Function to read place gps coordinates from excel file and transform it to dict: name - location"""

    place_gps = pd.read_excel(file_adrress, names=['Place', 'LAT', 'LONG'])
    place_gps = {place_gps.Place[i]: str(place_gps.LAT[i]) + ',' + str(place_gps.LONG[i]) for i in range(len(place_gps))}

    return place_gps


def get_places_nearby(gmaps_client, location, radius, type_, page_token, result_dataframe=pd.DataFrame()):
    """Function to communicate with Google Places API to search nearby places (20+ responses ready)"""

    places_result = gmaps_client.places_nearby(location=location,
                                               radius=radius,
                                               open_now=False,
                                               type=type_,
                                               page_token=page_token)

    if places_result['status'] == 'OK':

        result_df: DataFrame = pd.json_normalize(places_result['results'])

        if result_dataframe.empty:
            result_dataframe = result_df
        else:
            result_dataframe = result_dataframe.append(result_df, ignore_index=True)

        try:
            next_page_token = places_result["next_page_token"]
        except KeyError:
            return result_dataframe

        time.sleep(2)
        result_dataframe = get_places_nearby(gmaps_client,
                                             location,
                                             radius,
                                             type_,
                                             next_page_token,
                                             result_dataframe)

    return result_dataframe


def fetch_competitors(places_collection, types, types_radius_dict):
    """Function to search for certain type of competitors in radius for given places"""

    API_KEY = get_my_key()
    gmaps = googlemaps.Client(key=API_KEY)

    # target columns are not always present so have not to choose but clean
    columns_to_drop = ['icon', 'photos', 'reference', 'geometry.viewport.northeast.lat',
                       'geometry.viewport.northeast.lng', 'geometry.viewport.southwest.lat',
                       'plus_code.compound_code', 'plus_code.global_code', 'geometry.viewport.southwest.lng',
                       'scope']
    result = pd.DataFrame()
    for name, location in places_collection.items():
        for type_ in types:
            radius = types_radius_dict[type_]
            places_result = get_places_nearby(gmaps, location, radius, type_, '')

            try:
                places_result['Place'] = name
                places_result['type_1'] = [places_result['types'][i][0] for i in range(len(places_result['types']))]
                places_result['type_2'] = [places_result['types'][i][1] for i in range(len(places_result['types']))]
                places_result['search_radius'] = radius

                print(f'{name} - {type_} is downloaded')
            except KeyError:
                print(f'No data for {name} - {type_}')

            # There might be cases with extremely small lat, long diffs so that address (vicinity) is the same
            # Such cases are to be filtered - only unique combo Place - competitor vicinity
            result = result[~result.duplicated(['Place', 'vicinity'])]
            result = result.append(places_result,
                                   ignore_index=True)

    # save to excel result with names
    result.drop(columns_to_drop, axis=1, inplace=True)
    result.to_excel(str(len(places_collection)) + ".xlsx", sheet_name=str(len(places_collection)))


if __name__ == '__main__':

    places_collection = get_place_location('GPS.xlsx')

    radius = [1000, 1000, 3000]
    types = ['cafe', 'bakery']
    types_radius_dict = dict(zip(types, radius))

    fetch_competitors(places_collection, types, types_radius_dict)

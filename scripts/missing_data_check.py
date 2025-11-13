# for each csv file in the folder
folder_path = "/path/to/estuary-full-data"
output_file = "missingdatacheck.csv"

import os
import pandas as pd
import numpy as np

station_types = ['met', 'nut', 'wq']


def parse_station_info(file_name):
    """
    Gets the station name and type from the file name
    """
    base_name = file_name.replace('.csv', '').rstrip('0123456789')

    for st_type in station_types:
        if base_name.endswith(st_type):
            station_type = st_type
            station_name = base_name[:-len(st_type)]
            return station_name, station_type

    return None, None


# establish a dataframe for the output info
output_df = pd.DataFrame(columns=[
    'station_location',
    'earliest_date',
    'latest_date',
    '%missing_water_temp',
    '%missing_air_temp'
])

# for accumulating water and air temperature data
water_temp_data = {}
air_temp_data = {}

# process each CSV file in the folder
for file_name in os.listdir(folder_path):
    if not file_name.endswith('.csv'):
        continue # Skip non-csv files

    file_path = os.path.join(folder_path, file_name)
    station_name, station_type = parse_station_info(file_name)

    if station_type != 'nut':
        try:
            df = pd.read_csv(file_path, low_memory=False)
            df.columns = df.columns.str.strip()
            df.columns = df.columns.str.lower()

            if 'datetimestamp' in df.columns:
                df['datetimestamp'] = pd.to_datetime(
                    df['datetimestamp'], errors='coerce')
                earliest_date = df['datetimestamp'].min()
                latest_date = df['datetimestamp'].max()
            else:
                print(
                    f"Warning: 'datetimestamp' column not found in {file_name}")
                continue

            # Initialize missing data counts if the station is new
            if station_name not in water_temp_data:
                water_temp_data[station_name] = (0, 0)  # (total, missing)
            if station_name not in air_temp_data:
                air_temp_data[station_name] = (0, 0)  # (total, missing)

            # update temperature data counts
            if station_type == 'wq':  # water quality station
                total_water_temp = len(df['temp'])
                missing_water_temp = df['temp'].isna().sum()
                water_temp_data[station_name] = (
                    water_temp_data[station_name][0] + total_water_temp,
                    water_temp_data[station_name][1] + missing_water_temp
                )

            if station_type == 'met':  # meteorological station
                total_air_temp = len(df['atemp'])
                missing_air_temp = df['atemp'].isna().sum()
                air_temp_data[station_name] = (
                    air_temp_data[station_name][0] + total_air_temp,
                    air_temp_data[station_name][1] + missing_air_temp
                )

            # check if the station exists in the output dataframe
            if station_name in output_df['station_location'].values:
                # update earliest and latest dates if necessary
                current_earliest = output_df.loc[output_df['station_location']
                                                    == station_name, 'earliest_date'].values[0]
                current_latest = output_df.loc[output_df['station_location']
                                                == station_name, 'latest_date'].values[0]
                output_df.loc[output_df['station_location'] == station_name, 'earliest_date'] = min(
                    current_earliest, earliest_date)
                output_df.loc[output_df['station_location'] == station_name, 'latest_date'] = max(
                    current_latest, latest_date)
            else:
                # add new entry to the dataframe
                output_df = output_df.append({
                    'station_location': station_name,
                    'earliest_date': earliest_date,
                    'latest_date': latest_date,
                    '%missing_water_temp':  np.nan,  # placeholder
                    '%missing_air_temp':  np.nan    # placeholder
                }, ignore_index=True)
        except UnicodeDecodeError as e:  # added this because initial run got stuck on a unicode reading issue
            print(f"Encoding error in file {file_name}: {e}")
            continue  # skip this file if there is an encoding issue

# after processing all files, calculate missing percentages for water and air temperature
for station_name in output_df['station_location']:
    # water temperature
    total_water, missing_water = water_temp_data.get(station_name, (0, 0))
    if total_water > 0:
        output_df.loc[output_df['station_location'] == station_name,
                      '%missing_water_temp'] = (missing_water / total_water) * 100

    # air temperature
    total_air, missing_air = air_temp_data.get(station_name, (0, 0))
    if total_air > 0:
        output_df.loc[output_df['station_location'] == station_name,
                      '%missing_air_temp'] = (missing_air / total_air) * 100

print(output_df)

output_df.to_csv(output_file, index=False)

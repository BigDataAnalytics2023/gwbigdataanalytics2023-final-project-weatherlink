#Script to combine the weather_master and census_master tables

import os
import pandas as pd

# Read in all accident data
file_path = 'US_Accidents_March23.csv'
try:
    data = pd.read_csv(file_path, encoding='utf-8')
except UnicodeDecodeError:
    data = pd.read_csv(file_path, encoding='ISO-8859-1') 

# Remove all columns not relevant to weather join operation
columns_to_drop = [
    'End_Lat', 'End_Lng', 'Distance(mi)', 'Description', 'Street', 'City',
    'County', 'State', 'Zipcode', 'Country', 'Timezone', 'Airport_Code',
    'Weather_Timestamp', 'Temperature(F)', 'Wind_Chill(F)', 'Humidity(%)',
    'Pressure(in)', 'Visibility(mi)', 'Wind_Direction', 'Wind_Speed(mph)',
    'Precipitation(in)', 'Weather_Condition', 'Amenity', 'Bump', 'Crossing',
    'Give_Way', 'Junction', 'No_Exit', 'Railway', 'Roundabout', 'Station',
    'Stop', 'Traffic_Calming', 'Traffic_Signal', 'Turning_Loop',
    'Sunrise_Sunset', 'Civil_Twilight', 'Nautical_Twilight',
    'Astronomical_Twilight', 'End_Time', 'Severity', 'Source']
data = data.drop(columns=columns_to_drop)

# Read in all weather data
weather_file_path = 'C:/Users/Tharun/GWU/Fall_2023/Big_Data/finalproject/CountyDailyWeatherData/weather_master.csv'
try:
    weather_data = pd.read_csv(weather_file_path, encoding='utf-8')  
except UnicodeDecodeError:
    weather_data = pd.read_csv(weather_file_path, encoding='ISO-8859-1') 

import numpy as np

# Method to find the distance between two locations using the Haversine formula
def findDistance(lat1, lon1, lat2, lon2):
    lat1, lon1, lat2, lon2 = map(np.radians, [lat1, lon1, lat2, lon2])

    dlat = lat2 - lat1
    dlon = lon2 - lon1

    a = np.sin(dlat/2.0)**2 + np.cos(lat1) * np.cos(lat2) * np.sin(dlon/2.0)**2
    c = 2 * np.arcsin(np.sqrt(a))
    
    km = 6371 * c
    return km


# Given the latitude and longitude of a row, retrieve the weather data of the closest station
def findClosestWeather(year, month, day, lat, lon, weather_data):
    # Convert date into a single int (Format in weather data)
    time = int(year + month + day)

    # Scan all matching dates
    matches = weather_data[weather_data['LST_DATE'] == time]

    # If no matches are found, return None or a default value
    if matches.empty:
        return None

    # Use findDistance formula to find all of the distances
    distances = matches.apply(lambda row: findDistance(lat, lon, row['LATITUDE'], row['LONGITUDE']), axis=1)

    # Find the index of the minimum distance
    min_distance_index = distances.idxmin()

    # Retrieve the row with the smallest distance
    closest_row = matches.loc[min_distance_index]

    # Invalid/no recorded precipitation data gets set to 0
    if closest_row['P_DAILY_CALC'] < -9990:
        return 0
    
    # Return the precipitation data of the closest station
    return closest_row['P_DAILY_CALC']


# Scans through each row of accident data and appends the related precipitation data using above methods
def addPrecipitationData(data, weather_data):
    # Create a new column for precipitation in data
    data['Precipitation'] = np.nan

    for index, row in data.iterrows():
        # Extract year month and day
        t = row['Start_Time'].split('-')
        year, month, day = t[0], t[1], t[2][:2]

        lat, lon = row['Start_Lat'], row['Start_Lng']

        # Get the precipitation of closest weather station
        precipitation = findClosestWeather(year, month, day, lat, lon, weather_data)

        # append precipitation
        data.at[index, 'Precipitation'] = precipitation

        # Print progress
        if index % 100 == 0:
            print(f"Processed {index} rows")


# Append the precipitation data through method
addPrecipitationData(data, weather_data)

# Using the intermediate table created, join back to the original complete accident data table

# Read in the data fist
file_path = 'US_Accidents_March23.csv'
try:
    complete_data = pd.read_csv(file_path, encoding='utf-8') 
except UnicodeDecodeError:
    complete_data = pd.read_csv(file_path, encoding='ISO-8859-1')
# Merge
complete_data = pd.merge(complete_data, data[['ID', 'Precipitation']], on='ID', how='left')

# Finally, Upload to GBQ

# Clear parentheses from the dataset (GBQ cannot handle parentheses)
def replace_parentheses_in_column_names(df):
    # Convert measures such as distance(mi) into distance_mi
    df.columns = [col.replace('(', '_').replace(')', '') for col in df.columns]
    return df

# Apply parenthesis replacement
complete_data = replace_parentheses_in_column_names(complete_data)


from pandas_gbq import to_gbq
from google.cloud import bigquery

client = bigquery.Client()

os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = 'weatherlink-404323-9261c60d889c.json'

project_id = 'weatherlink-404323'
dataset_id = 'weatherlink_master'
table_id = 'accidents_weather_merged'

to_gbq(complete_data, f"{dataset_id}.{table_id}", project_id=project_id, if_exists='replace')
import os
import pandas as pd
import geopandas as gpd
import numpy as np
from google.cloud import bigquery
from pandas_gbq import to_gbq

csv_file_path = 'census_master.csv'
census_df = pd.read_csv(csv_file_path)

csv_file_path = 'merged_accidentweatherdata.csv'
# Read the CSV file into a DataFrame
df = pd.read_csv(csv_file_path)

# Load geospatial data from shapefile
geo_data = gpd.read_file('tl_2023_us_county.shp')

geo_data = geo_data.rename(columns={"GEOID": "geoid"})

# Drop unneccesary columns for join operation
columns_to_drop = [
    'STATEFP', 'COUNTYFP', 'COUNTYNS', 'GEOIDFQ', 'NAME',
    'NAMELSAD', 'LSAD', 'CLASSFP', 'MTFCC', 'CSAFP', 'CBSAFP', 'METDIVFP',
    'FUNCSTAT', 'ALAND', 'AWATER', 'geometry']

geo_data.drop(columns=columns_to_drop, inplace=True)

census_df = census_df.rename(columns={"geo_id": "geoid"})
census_df['geoid'] = census_df['geoid'].astype(str)
geo_data['geoid'] = geo_data['geoid'].astype(str)

# Join the census data on the geoid
merged_df = pd.merge(census_df, geo_data, on='geoid', how='left')
merged_df = merged_df.rename(columns={"INTPTLAT": "Start_Lat"})
merged_df = merged_df.rename(columns={"INTPTLON": "Start_Lng"})

# Add a separate year col
df['year'] = pd.to_datetime(df['Start_Time']).dt.year

# Remove all extraneous cols from accident data for join operation
columns_to_keep = ['ID', 'Start_Time', 'Start_Lat', 'Start_Lng', 'year']
df_copy = df[columns_to_keep].copy()

# Remove all extraneous cols from weather data for join operation
columns_needed = ['Start_Lat', 'Start_Lng', 'geoid', 'year']
merged_df_copy = merged_df[columns_needed].copy()

# Convert coordinates from string into float values
def convert_coord(coord):
    if isinstance(coord, str):
        return float(coord.replace('+', ''))
    elif isinstance(coord, float):
        return coord
    else:
        return None  

merged_df_copy['Start_Lat'] = merged_df_copy['Start_Lat'].apply(convert_coord)
merged_df_copy['Start_Lng'] = merged_df_copy['Start_Lng'].apply(convert_coord)


# Method to find the distance between two locations using the Haversine formula
def findDistance(lat1, lon1, lat2, lon2):
    lat1, lon1, lat2, lon2 = map(np.radians, [lat1, lon1, lat2, lon2])

    dlat = lat2 - lat1
    dlon = lon2 - lon1

    a = np.sin(dlat/2.0)**2 + np.cos(lat1) * np.cos(lat2) * np.sin(dlon/2.0)**2
    c = 2 * np.arcsin(np.sqrt(a))
    
    km = 6371 * c
    return km

# Method to find the closest matching census data to a given accident row
def findClosestMatch(row, comparison_data_yearly):
    year = row['year']
    lat = row['Start_Lat']
    lon = row['Start_Lng']

    # Get the data for the same year
    same_year_data = comparison_data_yearly.get(year, pd.DataFrame())

    if same_year_data.empty:
        return None

    # Calculate distances
    distances = same_year_data.apply(
        lambda x: findDistance(lat, lon, x['Start_Lat'], x['Start_Lng']), axis=1
    )

    # Find the index of the closest match
    closest_idx = distances.idxmin()

    return same_year_data.loc[closest_idx]

# Preparing your data for the join
def prepareForJoin(data, comparison_data):
    comparison_data_yearly = {year: df for year, df in comparison_data.groupby('year')}
    closest_matches = []

    for index, row in data.iterrows():
        closest_match = findClosestMatch(row, comparison_data_yearly)
        if closest_match is not None:
            closest_matches.append(closest_match)
        else:
            # Append NaNs or zeros in case of no match
            closest_matches.append(pd.Series([np.nan] * len(comparison_data.columns), index=comparison_data.columns))

        if index % 1000 == 0:
            print(f"Processed {index} rows")

    # Concatenate the results with the original data
    closest_matches_df = pd.DataFrame(closest_matches).reset_index(drop=True)
    return pd.concat([data.reset_index(drop=True), closest_matches_df], axis=1)

joined_data = prepareForJoin(df_copy, merged_df_copy)
joined_data.to_csv('masterjoinedto.csv', encoding='utf-8', index=False)

# Join the data back into the original accident table using the intermediate table created
result_df = pd.merge(df, joined_data[['ID', 'year', 'geoid']], on='ID', how='left')

final_df = pd.merge(result_df, census_df, on=['geoid', 'year'], how='left')

# Clear parentheses from the dataset (GBQ cannot handle parentheses)
def replace_parentheses_in_column_names(df):
    # Convert measures such as distance(mi) into distance_mi
    df.columns = [col.replace('(', '_').replace(')', '') for col in df.columns]
    return df

# Apply the function to dataframe
final_df = replace_parentheses_in_column_names(final_df)

# Upload to GBQ finally

client = bigquery.Client()

os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = 'weatherlink-404323-9261c60d889c.json'
project_id = 'weatherlink-404323'
dataset_id = 'weatherlink_master'
table_id = 'accidents_census_weather_merged'

# Upload the DataFrame to GBQ
to_gbq(final_df, f"{dataset_id}.{table_id}", project_id=project_id, if_exists='replace')
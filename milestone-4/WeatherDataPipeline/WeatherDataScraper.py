#This script scrapes all of the data from NOAA's web repository of weather data into individual folders
#Folders are created for each year, and within each year folder is a csv with the weather data of a city

import requests
import csv
import re
import os
import urllib.parse
import json

#Scrape the fcc page for county information given the latitude and longitude
def fetch_county_info(latitude, longitude):
    #Fetch latitude and longitude
    params = urllib.parse.urlencode({'latitude': latitude, 'longitude': longitude, 'format': 'json'})
    url = f'https://geo.fcc.gov/api/census/block/find?{params}'
    response = requests.get(url)
    response.raise_for_status()
    data = response.json()
    return data['County']['FIPS'], data['County']['name']

#All headers of the daily weather data (Specified in separate document in weather repo)
headers = [
    'WBANNO', 'LST_DATE', 'CRX_VN', 'LONGITUDE', 'LATITUDE', 'T_DAILY_MAX', 'T_DAILY_MIN',
    'T_DAILY_MEAN', 'T_DAILY_AVG', 'P_DAILY_CALC', 'SOLARAD_DAILY', 'SUR_TEMP_DAILY_TYPE',
    'SUR_TEMP_DAILY_MAX', 'SUR_TEMP_DAILY_MIN', 'SUR_TEMP_DAILY_AVG', 'RH_DAILY_MAX',
    'RH_DAILY_MIN', 'RH_DAILY_AVG', 'SOIL_MOISTURE_5_DAILY', 'SOIL_MOISTURE_10_DAILY',
    'SOIL_MOISTURE_20_DAILY', 'SOIL_MOISTURE_50_DAILY', 'SOIL_MOISTURE_100_DAILY',
    'SOIL_TEMP_5_DAILY', 'SOIL_TEMP_10_DAILY', 'SOIL_TEMP_20_DAILY', 'SOIL_TEMP_50_DAILY',
    'SOIL_TEMP_100_DAILY', 'COUNTY_FIPS', 'COUNTY_NAME'
]

for year in range(2006, 2024):
    base_url = f'https://www.ncei.noaa.gov/pub/data/uscrn/products/daily01/{year}/'
    output_directory = f"{year}_daily_data"
    os.makedirs(output_directory, exist_ok=True)
    
    response = requests.get(base_url)
    response.raise_for_status()
    
    # Find all historical weather datasets in the year folder
    links = re.findall(r'(CRND0103-\d{4}-[A-Z]{2}_[\w_]+\.txt)', response.text)

    for link in links:
        file_url = base_url + link
        print(f"Processing {file_url}")
        
        file_response = requests.get(file_url)
        file_response.raise_for_status()
        
        # Extract the year state and city from the filename
        match = re.search(r'CRND0103-(\d{4})-([A-Z]{2})_([\w_]+)\.txt', link)
        if not match:
            print(f"Unable to extract location information from the filename: {link}")
            continue

        year = match.group(1)
        state = match.group(2)
        city = match.group(3).replace("_", " ")
        csv_filename = os.path.join(output_directory, f"{city}_{state}_{year}_daily.csv")

        # Initialize county information
        county_fips, county_name = None, None

        with open(csv_filename, 'w', newline='') as csvfile:
            writer = csv.writer(csvfile)
            writer.writerow(headers)

            lines = file_response.text.strip().split('\n')
            for line in lines:
                if line.startswith("#"):
                    continue
                data = re.split(r'\s+', line.strip())

                # Fetch new county information if needed
                if county_fips is None:
                    latitude, longitude = float(data[headers.index('LATITUDE')]), float(data[headers.index('LONGITUDE')])
                    county_fips, county_name = fetch_county_info(latitude, longitude)

                # Append county information to the row
                data.extend([county_fips, county_name])
                writer.writerow(data)

        #Print statement to track progress
        print(f"Completed writing {city}, {state} data to {csv_filename}")
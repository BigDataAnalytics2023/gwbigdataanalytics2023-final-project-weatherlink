#This script combines all of the data that was scraped to a local machine into one master CSV file

import os
import pandas as pd

def combine_csv_with_year(folders):
    combined_data = pd.DataFrame()

    for folder in folders:
        year = folder  # The year to append is the folder name

        for filename in os.listdir(folder):
            if filename.endswith('.csv'):
                file_path = os.path.join(folder, filename)

                try:
                    data = pd.read_csv(file_path, encoding='utf-8') 
                except UnicodeDecodeError:
                    data = pd.read_csv(file_path, encoding='ISO-8859-1') 

                # Append the year to the start of the data
                data.insert(0, 'Year', year)                    
                
                combined_data = pd.concat([combined_data, data], ignore_index=True)

    return combined_data

#Process all folders from 2007 to 2024
folders = [str(year) for year in range(2007, 2024)]

# Combine and save the data
combined_csv = combine_csv_with_year(folders)
combined_csv.to_csv('weather_master.csv', index=False)



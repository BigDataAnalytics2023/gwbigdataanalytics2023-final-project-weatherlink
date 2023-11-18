import re
import threading
from google.cloud import bigquery

# Helper methods specific for census data file format
def extract_year_from_table_name(table_name):
    # Assuming the table name follows a pattern like 'county_YYYY_1yr'
    match = re.search(r'_(\d{4})_', table_name)
    if match:
        return int(match.group(1))
    else:
        return None

# Function to process data from a Google BigQuery table
def process_data(table_name, columns_of_interest, destination_table_id):
    # Initialize BigQuery client
    client = bigquery.Client()

    # Construct the SQL query
    query = f"SELECT {', '.join(columns_of_interest)} FROM `{table_name}`"

    print(f"Going to run this query: {query}")

    # Run the query
    query_job = client.query(query)

    # Fetch the results
    results = query_job.result()

    # Get the year from the table name
    year = extract_year_from_table_name(table_name)

    print(f"I found the year {year} in table name {table_name}")

    # Process and clean the data (append the year as a column)
    cleaned_data = []
    count = 0
    for row in results:
        #print(f"This is what a row looks like {row}")
        # The data from the client comes back as a tuple, with the left element being the data, and the right being a map of keys to indicies.
        # Like this: Row(('45083', 30127.0, 60472.0), {'geo_id': 0, 'income_per_capita': 1, 'median_income': 2})
        # So, we need to get the index for the data we want for each column, then put that in our new row data

        # Extract the data tuple from the row
            # Extract the data tuple from the row
        fips = row['geo_id']
        median_income = row['median_income']
        per_capita_income = row['income_per_capita']
        #print(f"Here is the geo_id tuple {fips}income_per_capita {per_capita_income}, and the median_income dict {median_income}")

         
        # Use the column mapping to extract values in the correct order
        row_data = []
        row_data.append(fips)
        row_data.append(median_income)
        row_data.append(per_capita_income)

        # Append the 'Year' column
        row_data.append(year)

        # Append the row to the cleaned data
        cleaned_data.append(row_data)
        #TODO clean the data row here! replace nulls, adjust, etc

        #print(f"Here is a nice lil row of data: {row_data}")
        count+=1
 

    # Initialize a new BigQuery client for uploading cleaned data
    upload_client = bigquery.Client()

    # Specify the destination dataset and table
    dataset_id = destination_table_id.split('.')[1]
    table_id = destination_table_id.split('.')[2]

    # Create a reference to the destination table
    destination_table_ref = upload_client.dataset(dataset_id).table(table_id)

    # Our boy Google REALLY wants to have a schema for the data, so we need to try and get it from the cloud or we need to create it manually
    try:
        destination_table = client.get_table(destination_table_ref)
    except Exception:
        # If the table doesn't exist, define the schema manually
        schema = [bigquery.SchemaField(column, 'STRING') for column in columns_of_interest]

        # We also have a year, this is specific to census. Other data will probably be per day!
        schema.append(bigquery.SchemaField("Year", 'String'))
    else:
        # If the table exists, use the existing schema
        schema = destination_table.schema

    print(f"Going to upload {count} rows to {destination_table_id} which I interperet as dataset {dataset_id} and table {table_id}")

    # Upload or try to create the destination table if it doesn't exist
    try:
        upload_client.get_table(destination_table_ref)
    except Exception:
        upload_client.create_table(bigquery.Table(destination_table_ref, schema=schema))
        print("Failed to find table, created with manual schema")


    # Insert data into the destination table
    print(f"Uploading data for the year {year}")
    errors = upload_client.insert_rows(destination_table_ref, cleaned_data, selected_fields = schema)

    if not errors:
        print(f"Data from {table_name} processed and uploaded successfully.")
    else:
        print(f"Error uploading data from {table_name}: {errors}")

# Function to create and start worker threads
def create_and_start_threads(table_names, columns_of_interest, destination_table_id):
    threads = []

    for table_name in table_names:
        print(f"Starting thread for table {table_name}")
        thread = threading.Thread(target=process_data, args=(table_name, columns_of_interest, destination_table_id))
        threads.append(thread)
        thread.start()

    # Wait for all threads to finish
    for thread in threads:
        thread.join()

# Example usage
if __name__ == "__main__":
    # Replace these values with your actual table names, columns of interest, and destination table ID
    #table_names = ["your_table_1", "your_table_2", "your_table_3"]  # Add more tables as needed
    #columns_of_interest = ["column1", "column2", "column3"]  # Specify columns you are interested in
    #destination_table_id = "your_project.your_dataset.your_destination_table"

    # For the census data case, we are testing just one table and two columns for now
    start_year = 2007
    stop_year = 2023
    table_names = [
        f"bigquery-public-data.census_bureau_acs.county_{year}_1yr"
        for year in range(start_year, stop_year + 1)
    ]

    print(f"Here are the table names: {table_names}")

    columns_of_interest = ["geo_id", "income_per_capita", "median_income"]  
    destination_table_id = "weatherlink-404323.weatherlink_master.census"

    # Create and start threads
    create_and_start_threads(table_names, columns_of_interest, destination_table_id)


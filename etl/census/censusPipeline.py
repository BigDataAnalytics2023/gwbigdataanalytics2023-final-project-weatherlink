import threading
from google.cloud import bigquery

# Function to process data from a Google BigQuery table
def process_data(table_name, columns_of_interest, destination_table_id):
    # Initialize BigQuery client
    client = bigquery.Client()

    # Construct the SQL query
    query = f"SELECT {', '.join(columns_of_interest)} FROM `{table_name}`"

    # Run the query
    query_job = client.query(query)

    # Fetch the results
    results = query_job.result()

    # Process and clean the data (replace this with your actual data processing logic)
    cleaned_data = [(row[column] if column in row else None) for row in results for column in columns_of_interest]

    # Initialize a new BigQuery client for uploading cleaned data
    upload_client = bigquery.Client()

    # Specify the destination dataset and table
    dataset_id = destination_table_id.split('.')[0]
    table_id = destination_table_id.split('.')[1]

    # Create a reference to the destination table
    destination_table_ref = upload_client.dataset(dataset_id).table(table_id)

    # Define the schema of the destination table (replace this with your actual schema)
    schema = [bigquery.SchemaField(column, 'STRING') for column in columns_of_interest]

    # Create the destination table if it doesn't exist
    try:
        upload_client.get_table(destination_table_ref)
    except Exception:
        upload_client.create_table(bigquery.Table(destination_table_ref, schema=schema))

    # Insert data into the destination table
    errors = upload_client.insert_rows(destination_table_ref, [cleaned_data])

    if not errors:
        print(f"Data from {table_name} processed and uploaded successfully.")
    else:
        print(f"Error uploading data from {table_name}: {errors}")

# Function to create and start worker threads
def create_and_start_threads(table_names, columns_of_interest, destination_table_id):
    threads = []

    for table_name in table_names:
        thread = threading.Thread(target=process_data, args=(table_name, columns_of_interest, destination_table_id))
        threads.append(thread)
        thread.start()

    # Wait for all threads to finish
    for thread in threads:
        thread.join()

# Example usage
if __name__ == "__main__":
    # Replace these values with your actual table names, columns of interest, and destination table ID
    table_names = ["your_table_1", "your_table_2", "your_table_3"]  # Add more tables as needed
    columns_of_interest = ["column1", "column2", "column3"]  # Specify columns you are interested in
    destination_table_id = "your_project.your_dataset.your_destination_table"

    # Create and start threads
    create_and_start_threads(table_names, columns_of_interest, destination_table_id)

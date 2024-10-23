import pandas as pd
import requests
import os
import zipfile
from sqlalchemy import create_engine
from datetime import datetime
import pyarrow as pa
import pyarrow.parquet as pq
from prefect import task, flow
# Ensure you have this import if using GCS
from prefect_gcp.cloud_storage import GcsBucket
from prefect_gcp import GcpCredentials


# Function to create a database connection
@task()
def create_db_connection(db_name, user, password, host, port):
    engine = create_engine(
        f'postgresql://{user}:{password}@{host}:{port}/{db_name}')
    return engine


# Function to clean up downloaded data
@task()
def clean_df(df):
    required_columns = {
        'duration_sec': 'duration_sec',
        'start_time': pd.NaT,  # Placeholder for datetime
        'end_time': pd.NaT,    # Placeholder for datetime
        'start_station_id': 'start_station_id',
        'start_station_name': '',
        'start_station_latitude': 0.0,
        'start_station_longitude': 0.0,
        'end_station_id': 'end_station_id',
        'end_station_name': '',
        'end_station_latitude': 0.0,
        'end_station_longitude': 0.0,
        'bike_id': 'bike_id',
        'user_type': '',
        'bike_share_for_all_trip': ''
    }

    # Check and add missing columns
    for col, default in required_columns.items():
        if col not in df.columns:
            df[col] = default

    # Convert start_time and end_time to datetime
    df['start_time'] = pd.to_datetime(df['start_time'], errors='coerce')
    df['end_time'] = pd.to_datetime(df['end_time'], errors='coerce')

    # Convert duration_sec to numeric (it should be in seconds)
    df['duration_sec'] = pd.to_numeric(df['duration_sec'], errors='coerce')

    # Convert station latitude and longitude to float
    df['start_station_latitude'] = pd.to_numeric(
        df['start_station_latitude'], errors='coerce')
    df['start_station_longitude'] = pd.to_numeric(
        df['start_station_longitude'], errors='coerce')
    df['end_station_latitude'] = pd.to_numeric(
        df['end_station_latitude'], errors='coerce')
    df['end_station_longitude'] = pd.to_numeric(
        df['end_station_longitude'], errors='coerce')

    # Optionally handle missing values (this step can be customized)
    df = df.dropna()  # Drop rows with any NaN values
    # Alternatively, you might want to fill missing values with specific values:
    # df.fillna(value={'column_name': 'default_value'}, inplace=True)

    return df


# Function to generate the URLs for Bay Wheels data
@task()
def generate_urls(start_date, end_date):
    urls = []
    current_date = start_date
    while current_date <= end_date:
        url = f"https://s3.amazonaws.com/baywheels-data/{
            current_date.strftime('%Y%m')}-fordgobike-tripdata.csv.zip"
        urls.append(url)
        # Move to the next month
        if current_date.month == 12:
            current_date = datetime(current_date.year + 1, 1, 1)
        else:
            current_date = datetime(
                current_date.year, current_date.month + 1, 1)
    return urls


# Function to download and extract CSV from a zip file
@task(retries=3)
def download_and_extract_csv(url):
    try:
        response = requests.get(url)
        zip_file_name = url.split("/")[-1]

        # Save the zip file
        with open(zip_file_name, 'wb') as f:
            f.write(response.content)

        # Extract CSV files from the zip
        with zipfile.ZipFile(zip_file_name, 'r') as zip_ref:
            csv_files = [f for f in zip_ref.namelist() if f.endswith('.csv')]
            if not csv_files:
                raise ValueError(f"No valid CSV file found in {zip_file_name}")
            return zip_file_name, csv_files

    except Exception as e:
        raise RuntimeError(f"Error downloading or extracting {url}: {e}")


@task(log_prints=True)
def process_and_load_data(zip_file_name, csv_file, engine):
    try:
        with zipfile.ZipFile(zip_file_name, 'r') as zip_ref:
            with zip_ref.open(csv_file) as csv_file_obj:
                chunk_size = 50000
                os.makedirs('data', exist_ok=True)
                all_chunks = []

                for chunk in pd.read_csv(csv_file_obj, chunksize=chunk_size, encoding='latin-1'):
                    print(f"Chunk shape after reading: {chunk.shape}")
                    cleaned_chunk = clean_df(chunk)
                    print(f"Chunk shape after cleaning: {cleaned_chunk.shape}")

                    # Replace empty strings with None
                    cleaned_chunk.replace('', None, inplace=True)

                    # Append the cleaned chunk to the list
                    all_chunks.append(cleaned_chunk)

                    # Load the chunk into PostgreSQL
                    try:
                        cleaned_chunk.to_sql(
                            'baywheels_trips', engine, if_exists='append', index=False)
                    except Exception as e:
                        print(f"Error loading chunk into PostgreSQL: {e}")

                # Check the number of chunks processed
                print(f"Total chunks processed: {len(all_chunks)}")

                # If there's only one chunk, skip further processing
                if len(all_chunks) == 1:
                    print("Only one chunk processed. Skipping Parquet creation.")
                    return

                # Concatenate chunks if more than one
                final_df = pd.concat(all_chunks, ignore_index=True)
                print(f"Final DataFrame shape before writing to Parquet: {
                      final_df.shape}")

                # Write the combined DataFrame to Parquet
                parquet_file_name = os.path.join(
                    'data', zip_file_name.replace('.csv.zip', '.parquet'))
                final_df.to_parquet(parquet_file_name, compression="gzip")

                print(f"Converted all chunks of {
                      zip_file_name} to {parquet_file_name}.")

    except Exception as e:
        raise RuntimeError(f"Error processing {zip_file_name}: {e}")


# Function to clean up downloaded zip files


@task(log_prints=True)
def cleanup(zip_file_name):
    if os.path.exists(zip_file_name):
        os.remove(zip_file_name)
        print(f"Removed {zip_file_name}")


# Function to write data to Google Cloud Storage
@task()
def write_to_gcs(path):
    gcp_credentials_block = GcpCredentials.load("gcp-creds")
    gcs_block = GcsBucket.load('gcs-block')

    # Extract the year and month from the path
    # Get the year and month part from the filename (e.g., "201801")
    year_month = path.split('-')[0]
    year = year_month[:4]  # Extract the year (first 4 characters)
    month = year_month[4:6]  # Extract the month (next 2 characters)

    # Construct the new path
    # Format: baywheels/2018/01/201801-fordgobike-tripdata.parquet
    new_path = f"baywheels/{year}/{month}/{path}"

    # Upload to Google Cloud Storage
    gcs_block.upload_from_path(
        from_path='data/' + path, to_path=new_path
    )

# Define the flow to orchestrate the tasks


@flow()
def ingest_to_postgres_flow(db_name, user, password, host, port, start_date, end_date):
    engine = create_db_connection(db_name, user, password, host, port)
    data_urls = generate_urls(start_date, end_date)

    for url in data_urls:
        zip_file_name, csv_files = download_and_extract_csv(url)
        for csv_file in csv_files:
            process_and_load_data(zip_file_name, csv_file, engine)
        cleanup(zip_file_name)
        write_to_gcs(zip_file_name.replace('.csv.zip', '.parquet'))

    print("All data loaded successfully!")


# Main function to run the flow
def main():
    # PostgreSQL configuration
    POSTGRES_USER = 'admin'
    POSTGRES_PASSWORD = 'admin'
    POSTGRES_HOST = 'localhost'  # Container name for PostgreSQL in Docker Compose
    POSTGRES_PORT = '5432'
    POSTGRES_DB = 'baywheels'

    # Date range for downloading data
    start_date = datetime(2018, 1, 1)
    end_date = datetime(2019, 1, 1)

    # Call the ingest function
    ingest_to_postgres_flow(POSTGRES_DB, POSTGRES_USER, POSTGRES_PASSWORD,
                            POSTGRES_HOST, POSTGRES_PORT, start_date, end_date)


if __name__ == "__main__":
    main()

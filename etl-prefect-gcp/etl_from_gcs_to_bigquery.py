from pathlib import Path
import pandas as pd
from prefect import flow, task
from prefect_gcp.cloud_storage import GcsBucket
from prefect_gcp import GcpCredentials
from datetime import datetime
import os


@task(retries=3, log_prints=True)
def extract_from_gcs(year: int, month: int) -> Path:
    """Download trip data from GCS"""
    gcs_path = f"baywheels/{year}/{month:02d}/{
        year}{month:02d}-fordgobike-tripdata.parquet"
    print(f"GCS Path: {gcs_path}")
    os.makedirs(f"data/baywheels/{year}/{month:02d}/", exist_ok=True)

    gcp_credentials_block = GcpCredentials.load("gcp-creds")
    gcs_block = GcsBucket.load('gcs-block')
    gcs_block.get_directory(from_path=gcs_path, local_path=f"data/")

    return Path(f"data/{gcs_path}")


@task()
def transform(path: Path) -> pd.DataFrame:
    """Data cleaning example"""
    df = pd.read_parquet(path)

    # Example: Check and fill missing values in `start_station_name` and `end_station_name`
    print(f"pre: missing start_station_name: {
          df['start_station_name'].isna().sum()}")
    print(f"pre: missing end_station_name: {
          df['end_station_name'].isna().sum()}")

    # Fill missing values with placeholders
    df['start_station_name'] = df['start_station_name'].fillna(
        'Unknown Start Station')
    df['end_station_name'] = df['end_station_name'].fillna(
        'Unknown End Station')

    print(f"post: missing start_station_name: {
          df['start_station_name'].isna().sum()}")
    print(f"post: missing end_station_name: {
          df['end_station_name'].isna().sum()}")

    # Handle any other columns you might need to fill missing data for
    print(f"pre: missing user_type: {df['user_type'].isna().sum()}")
    df['user_type'] = df['user_type'].fillna('Unknown')
    print(f"post: missing user_type: {df['user_type'].isna().sum()}")

    return df


@task()
def write_bq(df: pd.DataFrame) -> None:
    """Write DataFrame to BigQuery"""
    gcp_credentials_block = GcpCredentials.load("gcp-creds")

    df.to_gbq(
        destination_table="baywheels.baywheels_trips",
        project_id="baywheels-trips-project",
        credentials=gcp_credentials_block.get_credentials_from_service_account(),
        chunksize=10000,
        if_exists="append",
    )


def generate_dates(start_date: datetime, end_date: datetime):
    """Generate list of (year, month) tuples from start_date to end_date"""
    dates = []
    current_date = start_date

    while current_date <= end_date:
        dates.append((current_date.year, current_date.month))

        # Move to the next month
        if current_date.month == 12:
            current_date = datetime(current_date.year + 1, 1, 1)
        else:
            current_date = datetime(
                current_date.year, current_date.month + 1, 1)

    return dates


@flow()
def etl_gcs_to_bq(start_date: datetime, end_date: datetime):
    """Main ETL flow to load data into BigQuery"""
    date_ranges = generate_dates(start_date, end_date)

    for year, month in date_ranges:
        print(f"Processing data for {year}-{month:02d}")
        path = extract_from_gcs(year, month)
        df = transform(path)
        write_bq(df)


if __name__ == "__main__":
    # Example: Process data from January 2018 to January 2019
    start_date = datetime(2018, 1, 1)
    end_date = datetime(2019, 1, 1)

    etl_gcs_to_bq(start_date, end_date)

"""@bruin
name: ingestion.trips
type: python
image: python:3.11
connection: duckdb-default

materialization:
  type: table
  strategy: append

@bruin"""

import os
import json
import pandas as pd
import requests
from datetime import datetime
from dateutil.relativedelta import relativedelta
from dateutil.parser import parse


def materialize():
    """
    Fetch NYC Taxi trip data from TLC public endpoint.
    
    Fetches parquet files for each taxi type and month in the date range.
    Returns concatenated DataFrame with all trips and extraction metadata.
    """
    # Get Bruin environment variables
    start_date = parse(os.getenv('BRUIN_START_DATE')).date()
    end_date = parse(os.getenv('BRUIN_END_DATE')).date()
    
    # Get pipeline variables
    bruin_vars = json.loads(os.getenv('BRUIN_VARS', '{}'))
    taxi_types = bruin_vars.get('taxi_types', ['yellow'])
    
    # TLC data endpoint base URL
    base_url = "https://d37ci6vzurychx.cloudfront.net/trip-data/"
    
    dataframes = []
    extracted_at = datetime.now()
    
    # Generate date range in monthly intervals
    current_date = start_date.replace(day=1)  # Start from first day of month
    while current_date <= end_date:
        year_month = current_date.strftime("%Y-%m")
        
        for taxi_type in taxi_types:
            # Construct the parquet file URL
            filename = f"{taxi_type}_tripdata_{year_month}.parquet"
            file_url = f"{base_url}{filename}"
            
            print(f"Fetching: {file_url}")
            
            try:
                # Download and read the parquet file directly
                response = requests.get(file_url, timeout=300)
                response.raise_for_status()
                
                # Read parquet data from the response content
                df = pd.read_parquet(pd.io.common.BytesIO(response.content))
                
                if not df.empty:
                    # Add metadata columns for lineage and debugging
                    df['taxi_type'] = taxi_type
                    df['extracted_at'] = extracted_at
                    df['source_file'] = filename
                    
                    dataframes.append(df)
                    print(f"Successfully loaded {len(df)} rows from {filename}")
                else:
                    print(f"No data found in {filename}")
                    
            except requests.exceptions.RequestException as e:
                print(f"Failed to download {filename}: {e}")
                # Continue with next file instead of failing the entire job
                continue
            except Exception as e:
                print(f"Error processing {filename}: {e}")
                continue
        
        # Move to next month
        current_date = current_date + relativedelta(months=1)
    
    if not dataframes:
        print("No data was successfully loaded")
        # Return empty DataFrame with expected schema
        return pd.DataFrame(columns=[
            'pickup_datetime', 'dropoff_datetime', 'taxi_type', 
            'extracted_at', 'source_file'
        ])
    
    # Concatenate all DataFrames
    final_df = pd.concat(dataframes, ignore_index=True)

    # Strip timezone info from datetime columns to avoid Windows tzdata issues
    # (PyArrow requires a timezone database on Windows to handle tz-aware columns)
    for col in final_df.select_dtypes(include=['datetimetz']).columns:
        final_df[col] = final_df[col].dt.tz_convert('UTC').dt.tz_localize(None)

    print(f"Total rows loaded: {len(final_df)}")
    print(f"Date range: {start_date} to {end_date}")
    print(f"Taxi types: {taxi_types}")
    
    return final_df
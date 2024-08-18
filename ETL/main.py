import time
import subprocess
import logging
from time import perf_counter

import pandas as pd
import numpy as np
from sqlalchemy import create_engine


logging.basicConfig(level=logging.INFO, format='%(levelname)s: %(message)s')
logger = logging.getLogger(__name__)



# def wait_for_postgres(host, max_retries=5, delay_seconds=5):
#     """Wait for PostgreSQL to become available."""
#     retries = 0
#     while retries < max_retries:
#         try:
#             result = subprocess.run(
#                 ["pg_isready", "-h", host], check=True, capture_output=True, text=True)
#             if "accepting connections" in result.stdout:
#                 print("Successfully connected to PostgreSQL!")
#                 return True
#         except subprocess.CalledProcessError as e:
#             print(f"Error connecting to PostgreSQL: {e}")
#             retries += 1
#             print(
#                 f"Retrying in {delay_seconds} seconds... (Attempt {retries}/{max_retries})")
#             time.sleep(delay_seconds)
#     print("Max retries reached. Exiting.")
#     return False


# # Use the function before running the ELT process
# if not wait_for_postgres(host="destination_database"):
#     exit(1)





def read_csv(csv_name):
    logger.info("Reading CSV")
    try:
        csv = pd.read_csv(csv_name, on_bad_lines='skip')
        logger.info("CSV read successfully")
        return csv
    except Exception as e:
        logger.error(f"Error reading CSV: {e}")
        exit(1)

def calculate_watch_time_minutes(x):
    comma_index = x.index('/')
    hours = x[:comma_index]
    minutes = x[comma_index+1:]
    if minutes and hours: 
        return int(hours) * 60 + int(minutes)
    if hours:
        return int(hours) * 60
    if minutes:
        return int(minutes)
    return np.nan


def process_csv(csv):
    logger.info("Processing CSV")
    t1=perf_counter()

    csv.drop(columns=['User reviews','Awards', 'Description', 'Rating', 'Ratedby', 'Film Industry', 'Streaming platform'], inplace=True)


    csv['Box office collection'] = csv['Box office collection'].replace({
            r'[$,]': '', 
            ' NIL': np.nan
        }, regex=True).astype(float)


    csv['Watch  hour '] = csv['Watch  hour '].replace({
            r'\s': '',
            'minutes?': '',
            'hours?': '/'
        }, regex=True)


    csv['Watch  hour '] = csv['Watch  hour '].apply(calculate_watch_time_minutes)
    csv.rename(columns={'Watch  hour ': 'Minutes'}, inplace=True)

    t2=perf_counter()
    logger.info(f"CSV processed successfully in {round(t2-t1, 5)} seconds")

    return csv


def main():
    try:
        csv = read_csv(r'ETL/datasets/my.csv')
        processed_csv = process_csv(csv)
        logger.info(processed_csv)
    except Exception as e:
        logger.error(f"An error occurred: {e}")

if __name__ == "__main__":
    main()
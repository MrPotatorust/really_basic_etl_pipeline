import time
import subprocess
import logging
from time import perf_counter

import pandas as pd
import numpy as np
from sqlalchemy import create_engine, text


logging.basicConfig(level=logging.INFO, format='%(levelname)s: %(message)s')
logger = logging.getLogger(__name__)



def wait_for_postgres(host, max_retries=5, delay_seconds=5):
    retries = 0
    while retries < max_retries:
        try:
            result = subprocess.run(
                ["pg_isready", "-h", host], check=True, capture_output=True, text=True)
            if "accepting connections" in result.stdout:
                logger.info("Successfully connected to PostgreSQL!")
                return True
        except subprocess.CalledProcessError as e:
            logger.error(f"Error connecting to PostgreSQL: {e}")
            retries += 1
            logger.warning(f"Retrying in {delay_seconds} seconds... (Attempt {retries}/{max_retries})")
            time.sleep(delay_seconds)
    logger.error(f"Failed to connect to PostgreSQL after {max_retries}")
    return False






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
    csv.rename(columns={'Watch  hour ': 'minutes',
                        'Movie name': 'movie_name',
                        'Year of release': 'year_of_release',
                        'Box office collection': 'box_office_collection',
                        'Director': 'director',
                        'Genre': 'genre'}, inplace=True)

    t2=perf_counter()
    logger.info(f"CSV processed successfully in {round(t2-t1, 5)} seconds")

    return csv

def load(csv):
    try:
        engine = create_engine('postgresql://admin:password@staging_database:5432/staging_db')
        logger.info("Engine created")
        csv.to_sql('stg_my_table', engine, if_exists='replace', index=False)

        # Only for debugging purposes
        with engine.connect() as conn:
            result = conn.execute(text("SELECT * FROM stg_my_table"))
            logger.info(result.all())

    except Exception as e:
        logger.error(f"Error creating engine to PostgreSQL: {e}")

def main():
    if not wait_for_postgres(host="destination_database"):
        exit(1)
    try:
        csv = read_csv(r'datasets/my.csv')
        processed_csv = process_csv(csv)

        logger.info(processed_csv)
        load(processed_csv)
        logger.info("Data loaded successfully")
        
        print()

    except Exception as e:
        logger.error(f"An error occurred: {e}")

if __name__ == "__main__":
    main()
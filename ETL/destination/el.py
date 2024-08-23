import time
from time import perf_counter
import logging

from sqlalchemy import create_engine, text, MetaData, Table, Column, Integer, String



logging.basicConfig(level=logging.INFO, format='%(levelname)s: %(message)s')
logger = logging.getLogger(__name__)

staging_engine = create_engine('postgresql://admin:password@staging_database:5432/staging_db')
destination_engine = create_engine('postgresql://admin:password@destination_database:5432/destination_db')


# Define the movie_name table
metadata = MetaData()
movie_name = Table('movie_name', metadata,
    Column('id', Integer, primary_key=True),
    Column('name', String)
)

with staging_engine.connect() as staging_conn, destination_engine.connect() as destination_conn:
    # Fetch data from staging
    result = staging_conn.execute(text('SELECT * FROM movie_names'))
    movies = result.fetchall()
    
    # Create the table in the destination database
    metadata.create_all(destination_engine)
    
    # Insert data into the destination database
    for movie in movies:
        destination_conn.execute(
            movie_name.insert().values(name=movie[0])
        )
    
    destination_conn.commit()

    logger.info(f"Inserted {len(movies)} movies into destination database")
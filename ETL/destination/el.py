import time
from time import perf_counter
import logging

from sqlalchemy import create_engine, text



logging.basicConfig(level=logging.INFO, format='%(levelname)s: %(message)s')
logger = logging.getLogger(__name__)

staging_engine = create_engine('postgresql://admin:password@staging_database:5432/staging_db')
destination_engine = create_engine('postgresql://admin:password@destination_database:5432/destination_db')


with staging_engine.connect() as staging_conn, destination_engine.connect() as destination_conn:
    movie_name = staging_conn.execute(text('SELECT * FROM movie_names'))
    logger.info(movie_name.all())
    logger.info(movie_name)



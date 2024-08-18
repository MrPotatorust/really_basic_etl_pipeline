# This is a REALLY basic ETL pipeline

## How to run?

Just go into the root directory and run "docker composer up" in a terminal

## How to access the database?

1. After succesfully running the docker containers run "docker exec -it basic_data_pipeline-destination_database-1 psql -U admin destination_db"

2. You can run normal querys on the table "my_table"

## Basic table schema:
#### Movie name, Year of release, Minutes, Genre, Director, Box office collection

Movie name - name of the movie__
Year of release - year of release__
Minutes - the length of the movie in minutes__
Genre - list of movies genres__
Director - the movie director__
Box office collection - how much money the movie grossed in dollars
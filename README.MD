# This is a REALLY basic ETL pipeline

## How to run?

Just go into the root directory and run "docker composer up" in a terminal

## How to access the database?

1. After succesfully running the docker containers run "docker exec -it basic_data_pipeline-destination_database-1 psql -U admin destination_db"

2. You can run normal querys on the table "my_table"

## Basic table schema:
#### Movie name, Year of release, Minutes, Genre, Director, Box office collection

Movie name - name of the movie  
Year of release - year of release  
Minutes - the length of the movie in minutes  
Genre - list of movies genres  
Director - the movie director  
Box office collection - how much money the movie grossed in dollars

## In the destination database:
#### ID, Movie name:

ID - auto increment id
Movie name - the name of the movie
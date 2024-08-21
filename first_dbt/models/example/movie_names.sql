SELECT 
  movie_name
FROM 
  {{ source('staging_db', 'stg_my_table') }}
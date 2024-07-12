{{ config(materialized='view') }}

with movies as (
    select * from {{ ref('stg_movies') }}
),
ratings as (
    select * from {{ ref('stg_ratings') }}
)

select 
    movies.id as movie_id,
    movies.production_countries as country,
    movies.release_year as release_year,
    ratings.userId as user_id,
    ratings.rating as rating
from movies
join ratings on ratings.movieId = movies.id




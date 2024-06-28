{{ config(materialized='table') }}

with movie_release_year as (
    select release_year, movie_id from {{ ref('_rating_per_country') }}
)

select release_year, count(movie_id) as number_of_movies from movie_release_year group by release_year
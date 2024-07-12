{{ config(materialized='table') }}

with movie_rating as (
    select country, rating from {{ ref('_rating_per_country') }}
)

select country, count(rating) as numer_of_ratings from movie_rating group by country
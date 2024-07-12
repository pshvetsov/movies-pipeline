{{ config(materialized='table') }}

with movie_rating as (
    select country, rating from {{ ref('_rating_per_country') }}
)

select country, avg(rating) as average_rating from movie_rating group by country
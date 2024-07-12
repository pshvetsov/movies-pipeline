{{config(materialized='view')}}

with ratingsdata as (
    SELECT * FROM
    {{ source ('staging', 'ratings_small') }}
)

select userId, movieId, rating from ratingsdata
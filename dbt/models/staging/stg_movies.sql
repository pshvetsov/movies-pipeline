{{config(aterialized='view')}}

with moviesdata  as (
    SELECT * FROM
    {{ source ('staging', 'movies_metadata') }}
)

select id, original_language, release_year, production_countries from moviesdata
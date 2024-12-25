{{ config(materialized='table', tags=['silver']) }}

select name
    , year_start
    , year_end
    , episodes
    , rating
    , RANK() OVER (partition by year_start order by rating desc) as rank
from {{ source('dags', 'kaggle_tv_shows') }}

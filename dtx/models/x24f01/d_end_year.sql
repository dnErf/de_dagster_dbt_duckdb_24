{{ config(materialized='table', tags=['silver']) }}

select distinct year_end
from {{ source('dags', 'kaggle_tv_shows') }}

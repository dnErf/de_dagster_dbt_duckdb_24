{{ config(materialized='table', tags=['silver']) }}

select distinct year_start
from {{ source('dags', 'kaggle_tv_shows') }}

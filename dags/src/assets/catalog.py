import duckdb

from dagster import asset

from ..resources.iceberg import IcebergResource
from ..constants import dbt_project_dir, EnvironmentVariables

@asset(compute_kind="python")
def iceberg_tvshow_catalog(ev: EnvironmentVariables, iceberg: IcebergResource):
    catalog = iceberg.init_catalog()
    
    db = iceberg.load_raw_kaggle_tvshows(catalog)
    db.execute(f"attach '{dbt_project_dir}/target/{ev.deployment}.duckdb'")
    
    dt = iceberg.kaggle_tvshows_refinement_and_save(db)
    c = int(dt.fetchone()[0])
    
    return c > 0

@asset(deps=["f_tv_shows","d_start_year","d_end_year"], compute_kind="python")
def mduck_asset():
    db = duckdb.connect(f"{dbt_project_dir}/target/dev.duckdb")
    db.execute("attach 'md:x24f01'")
    
    # save to motherduck
    db.execute("""
        create or replace table x24f01.main.f_tv_shows 
            as select * from dev.dags.kaggle_tv_shows 
    """)
    db.execute("""
        create or replace table x24f01.main.d_start_year
            as select * from dev.dags.kaggle_tv_shows
    """)
    db.execute("""
        create or replace table x24f01.main.d_end_year 
            as select * from dev.dags.kaggle_tv_shows
    """)
    
    c = db.sql("select count(*) from x24f01.main.f_tv_shows limit 1").fetchone()[0]
    print(c)

    # refactor this
    return c > 0

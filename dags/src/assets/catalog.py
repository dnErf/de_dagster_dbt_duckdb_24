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

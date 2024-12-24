from dagster import asset

from ..resources.iceberg import IcebergResource
# from ..constants import dbt_project_dir

@asset(compute_kind="python")
def iceberg_tvshow_catalog(iceberg: IcebergResource):
    catalog = iceberg.init_catalog()
    print(catalog.list_namespaces())
    tbl = catalog.load_table("raw.kaggle_tv_shows")
    dt = tbl.scan(limit=100).to_duckdb("kaggle_tv_shows")
    print(dt.sql("select * from kaggle_tv_shows"))
    # dt.execute(f"attach '{dbt_project_dir}/target/dev.duckdb'")

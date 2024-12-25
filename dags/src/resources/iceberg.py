import os

from duckdb import DuckDBPyConnection
from dagster import ConfigurableResource, ResourceDependency
from pyiceberg.catalog import Catalog, load_catalog

from ..constants import EnvironmentVariables

class IcebergResource(ConfigurableResource):
    ev: ResourceDependency[EnvironmentVariables]

    def init_catalog(self):
        print(self.ev)
        catalog = load_catalog('rest', **{
            "auth.type": "NONE", 
            "uri": os.getenv("S3_URI"),
            "s3.endpoint": os.getenv("S3_ENDPOINT"),
            "s3.access-key-id": os.getenv("S3_ACCESS_KEY"),
            "s3.secret-access-key": os.getenv("S3_SECRET_KEY"),
            "path-style-access": True
        })
        return catalog
    
    def load_raw_kaggle_tvshows(self, catalog: Catalog) -> DuckDBPyConnection | None:
        tbl = catalog.load_table("raw.kaggle_tv_shows")

        return tbl.scan(
            selected_fields=("Shows Name", "Release Year", "Episodes", "Rating")
        ).to_duckdb("kaggle_tv_shows")
    
    def kaggle_tvshows_refinement_and_save(self, db: DuckDBPyConnection):
        db.execute("use dev; create schema if not exists dags;")
        db.execute(f"""
            create or replace table {self.ev.deployment}.dags.kaggle_tv_shows as
            select "Shows Name" as name
                , substring(trim("Release Year"), 0, 5) as year_start
                , case when len("Release Year") > 5 then substring("Release Year", 6, 10) else substring("Release Year", 0, 5) end as "year_end"
                , substring("Episodes", 0, (len("Episodes") - 3)) as episodes
                , "Rating" as rating
            from temp.main.kaggle_tv_shows
            order by rating desc
        """)

        return db.sql(f"select count(*) from {self.ev.deployment}.dags.kaggle_tv_shows limit 1")

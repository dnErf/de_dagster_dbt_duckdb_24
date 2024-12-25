import os

from dagster import Definitions, EnvVar
from dagster_dbt import DbtCliResource

from .assets.dbt import dbt_manifest_assets
from .assets.catalog import iceberg_tvshow_catalog
from .constants import dbt_project_dir, EnvironmentVariables
from .resources.iceberg import IcebergResource

# from dagster import load_assets_from_modules
# all_assets = load_assets_from_modules([assets])

environment_variables = EnvironmentVariables(deployment=EnvVar("DAGSTER_DEPLOYMENT") or os.getenv("DAGSTER_DEPLOYMENT", "dev"))

defs = Definitions(
    # assets=all_assets,
    assets=[dbt_manifest_assets, iceberg_tvshow_catalog],
    resources={
        "ev": environment_variables,
        "dbt": DbtCliResource(project_dir=os.fspath(dbt_project_dir)),
        "iceberg": IcebergResource(ev=environment_variables)
    }
)

import os

from dagster import Definitions
from dagster_dbt import DbtCliResource

from .assets.dbt import dbt_manifest_assets
from .constants import dbt_project_dir

# from dagster import load_assets_from_modules
# all_assets = load_assets_from_modules([assets])

defs = Definitions(
    # assets=all_assets,
    assets=[dbt_manifest_assets],
    resources={
        "dbt": DbtCliResource(project_dir=os.fspath(dbt_project_dir))
    }
)

from dagster import AssetExecutionContext
from dagster_dbt import DbtCliResource, dbt_assets
from ..constants import dbt_manifest_path

@dbt_assets(manifest=dbt_manifest_path)
def dbt_manifest_assets(context: AssetExecutionContext, dbt: DbtCliResource):
    yield from dbt.cli(["build","--select","tag:silver"], context=context).stream()

from pathlib import Path

import duckdb
from dagster import (
    AssetExecutionContext, Definitions, ScheduleDefinition, asset, define_asset_job,
)
from dagster_dbt import DbtCliResource, DbtProject, dbt_assets

from extract.ingest import DB_PATH, LABEL_ID, load_label_releases

DBT_DIR = Path(__file__).parent / "discogs_dbt"
dbt_project = DbtProject(project_dir=DBT_DIR, profiles_dir=DBT_DIR)
dbt_project.prepare_if_dev()          # builds the dbt manifest when running locally


# ingest step, named like the dbt source so Dagster links them
@asset(key=["discogs_raw", "raw_label_releases"])
def raw_label_releases():
    con = duckdb.connect(DB_PATH)
    n = load_label_releases(con, LABEL_ID)
    con.close()
    return n


# every dbt model, test and snapshot becomes an asset
@dbt_assets(manifest=dbt_project.manifest_path)
def discogs_dbt_assets(context: AssetExecutionContext, dbt: DbtCliResource):
    yield from dbt.cli(["build"], context=context).stream()


weekly_job = define_asset_job("weekly_refresh", selection="*")
weekly_schedule = ScheduleDefinition(job=weekly_job, cron_schedule="0 6 * * 1")   # Mondays 06:00

defs = Definitions(
    assets=[raw_label_releases, discogs_dbt_assets],
    jobs=[weekly_job],
    schedules=[weekly_schedule],
    resources={"dbt": DbtCliResource(project_dir=dbt_project)},
)
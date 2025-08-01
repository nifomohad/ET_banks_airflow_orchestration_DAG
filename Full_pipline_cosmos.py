import os
from datetime import datetime
from pendulum import timezone
from airflow import DAG
from cosmos import DbtTaskGroup, ProjectConfig, ProfileConfig, ExecutionConfig, RenderConfig
from cosmos.profiles import SnowflakeUserPasswordProfileMapping

# Define paths and environment
DBT_PROJECT_PATH = os.path.expanduser("~/projects/dbt_my_project/Ethiopian-Banks-and-Insurance-year_on_year-growth")
DBT_EXECUTABLE_PATH = os.path.expanduser("~/miniconda3/envs/dbtenv/bin/dbt")


profile_config = ProfileConfig(
    profile_name="bank",          # Matches your profiles.yml top-level key
    target_name="prod_2",         # Matches the Snowflake target in profiles.yml
    profile_mapping=SnowflakeUserPasswordProfileMapping(
        conn_id="my_snowflake",   # Airflow connection ID
        profile_args={
            "database": "ET_BANKS",  # Override if different from connection
            "schema": "banks",      # Override if needed
            "threads": 4            # Optional: Matches your profiles.yml
        },
    ),
)

# Project configuration
project_config = ProjectConfig(
    dbt_project_path=DBT_PROJECT_PATH,
    manifest_path=os.path.join(DBT_PROJECT_PATH, "target/manifest.json"),
)

# Execution configuration
execution_config = ExecutionConfig(
    dbt_executable_path=DBT_EXECUTABLE_PATH,
)

with DAG(
    dag_id="dbt_snowflake_cosmos",
    start_date=datetime(2025, 7, 1, tzinfo=timezone("UTC")),  # Changed tz to tzinfo
    schedule=None,
    catchup=False,
    tags=["dbt", "snowflake", "cosmos"],
) as dag:

    # Main dbt pipeline (run + test)
    with DbtTaskGroup(
        group_id="dbt_full_pipeline",
        project_config=project_config,
        profile_config=profile_config,
        execution_config=execution_config,

        render_config=RenderConfig(
            select=["path:models"]  # Run all models
        ),
         operator_args={
        "command": "build",  # Runs "dbt build" (run + test in one)
        # "full_refresh": True  # Optional flags
    }
    ) as dbt_tg:
        pass  
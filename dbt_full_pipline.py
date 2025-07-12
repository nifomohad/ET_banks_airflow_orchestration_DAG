
from __future__ import annotations

import pendulum

from airflow.models.dag import DAG
from airflow.operators.bash import BashOperator

# Define the dbt project directory and Conda environment
DBT_PROJECT_PATH = "~/projects/dbt_my_project/Ethiopian-Banks-and-Insurance-year_on_year-growth"
CONDA_ENV_NAME = "dbtenv"

# Define the shell command to execute
# Note: Since 'conda activate' sets up the environment in the current shell context, 
# we combine the commands with '&&' to ensure they run sequentially in the same shell session.
# We also use 'source $(conda info --base)/etc/profile.d/conda.sh' to initialize conda in the script environment
# if it's not already initialized.
BASE_DBT_COMMAND_PREFIX = f"""
source $(conda info --base)/etc/profile.d/conda.sh && 
cd {DBT_PROJECT_PATH} && 
conda activate {CONDA_ENV_NAME} 
"""


with DAG(
    dag_id="dbt_full_pipeline_dag",
    start_date=pendulum.datetime(2025, 7, 1, tz="UTC"),
    schedule=None,
    catchup=False,
    tags=["dbt", "full_pipeline"],
) as dag:

    validate_dbt_environment = BashOperator(
        task_id="validate_dbt_environment",
        bash_command=BASE_DBT_COMMAND_PREFIX + "dbt debug",
    )

    dbt_run_task = BashOperator(
        task_id="run_dbt_models",
        bash_command=BASE_DBT_COMMAND_PREFIX + "dbt run",
    )

    dbt_test_task = BashOperator(
        task_id="run_dbt_tests",
        bash_command=BASE_DBT_COMMAND_PREFIX + "dbt test",
    )

    dbt_build_task = BashOperator(
        task_id="build_dbt_project",
        bash_command=BASE_DBT_COMMAND_PREFIX + "dbt build",
    )

    dbt_docs_generate_task = BashOperator(
        task_id="generate_dbt_docs",
        bash_command=BASE_DBT_COMMAND_PREFIX + "dbt docs generate",
    )

    # Define dependencies
    validate_dbt_environment >> dbt_run_task >> dbt_test_task >> dbt_build_task >> dbt_docs_generate_task
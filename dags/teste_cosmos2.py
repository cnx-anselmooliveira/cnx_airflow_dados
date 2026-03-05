from datetime import datetime
from pathlib import Path

from airflow import DAG

from cosmos import (
    DbtTaskGroup,
    ExecutionConfig,
    ExecutionMode,
    ProfileConfig,
    ProjectConfig,
    RenderConfig,
)
from cosmos.profiles import TrinoLDAPProfileMapping

DBT_IMAGE = "anselmomendes/airflow_ek8:1.0"

# O projeto está embutido na imagem — mesmo path no Airflow e no pod K8s
AIRFLOW_PROJECT_DIR = Path("/opt/airflow/lakehouse")
K8S_PROJECT_DIR = "/opt/airflow/lakehouse"

with DAG(
    dag_id="lakehouse_dbt_kubernetes",
    schedule=None,
    start_date=datetime(2026, 3, 3),
    catchup=False,
    tags=["dbt", "lakehouse", "kubernetes"],
) as dag:
    run_models = DbtTaskGroup(
        group_id="run_models",
        project_config=ProjectConfig(
            dbt_project_path=AIRFLOW_PROJECT_DIR,
            project_name="lakehouse",
        ),
        profile_config=ProfileConfig(
            profile_name="lakehouse",
            target_name="prod",
            # Usado apenas para o Airflow parsear o projeto (não exposto no pod K8s)
            profile_mapping=TrinoLDAPProfileMapping(
                conn_id="trino_conn",
            ),
        ),
        render_config=RenderConfig(dbt_project_path=AIRFLOW_PROJECT_DIR),
        execution_config=ExecutionConfig(
            execution_mode=ExecutionMode.KUBERNETES,
            dbt_project_path=K8S_PROJECT_DIR,
        ),
        operator_args={
            "image": DBT_IMAGE,
            "image_pull_policy": "Always",
            "get_logs": True,
            "is_delete_operator_pod": True,
            "in_cluster": True,
        },
    )

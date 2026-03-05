from datetime import datetime

from cosmos import DbtDag, ProjectConfig, ProfileConfig, ExecutionConfig, RenderConfig
from cosmos.profiles import TrinoUserPasswordProfileMapping
from cosmos.constants import TestBehavior, ExecutionMode
from kubernetes.client import models as k8s

DBT_IMAGE = "anselmomendes/airflow_ek8:1.0"
DBT_PROJECT_PATH = "/opt/airflow/lakehouse"

profile_config = ProfileConfig(
    profile_name="lakehouse",
    target_name="dev",
    profile_mapping=TrinoUserPasswordProfileMapping(
        conn_id="trino_conn",
        profile_args={"schema": "publdeltaic"},
    ),
)

execution_config = ExecutionConfig(
    execution_mode=ExecutionMode.KUBERNETES,
)

lakehouse_dag = DbtDag(
    dag_id="lakehouse_dbt_kubernetes",
    project_config=ProjectConfig(
        dbt_project_path=DBT_PROJECT_PATH,
        project_name="lakehouse",
    ),
    profile_config=profile_config,
    execution_config=execution_config,
    render_config=RenderConfig(
        test_behavior=TestBehavior.AFTER_EACH,
    ),
    operator_args={
        "image": DBT_IMAGE,
        "image_pull_policy": "Always",
        "get_logs": True,
        "is_delete_operator_pod": True,
        "in_cluster": True,
        "env_vars": [
            k8s.V1EnvVar(
                name="DBT_PACKAGES_INSTALL_PATH",
                value="/opt/airflow/lakehouse/dbt_packages",
            ),
        ],
    },
    schedule=None,
    start_date=datetime(2026, 3, 3),
    catchup=False,
    tags=["dbt", "lakehouse", "kubernetes"],
)

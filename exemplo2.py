from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from datetime import datetime

# Funções de teste
def task_1():
    print("Executando subtask 1")

def task_2():
    print("Executando subtask 2")

def task_3():
    print("Executando subtask 3")

def task_4():
    print("Executando subtask 4")

def task_5():
    print("Executando subtask 5")


with DAG(
    dag_id="dag_teste_5_subtasks2",
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,
    catchup=False,
    tags=["teste"]
) as dag:

    start = EmptyOperator(task_id="start")

    subtask_1 = PythonOperator(
        task_id="subtask_1",
        python_callable=task_1
    )

    subtask_2 = PythonOperator(
        task_id="subtask_2",
        python_callable=task_2
    )

    subtask_3 = PythonOperator(
        task_id="subtask_3",
        python_callable=task_3
    )

    subtask_4 = PythonOperator(
        task_id="subtask_4",
        python_callable=task_4
    )

    subtask_5 = PythonOperator(
        task_id="subtask_5",
        python_callable=task_5
    )

    end = EmptyOperator(task_id="end")

    # Definição das dependências
    start >> subtask_1 >> subtask_2 >> subtask_3 >> subtask_4 >> subtask_5 >> end
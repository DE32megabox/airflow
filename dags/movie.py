from datetime import datetime, timedelta
from textwrap import dedent

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator

from airflow.operators.python import (
        PythonOperator,
        PythonVirtualenvOperator,
        BranchPythonOperator,
)

with DAG(
    'movie',
    default_args={
        'depends_on_past': False,
        'retries': 1,
        'retry_delay': timedelta(seconds=3),
    },

    max_active_runs=1,
    max_active_tasks=3,
    description='movie',
    schedule="10 2 * * *",
    start_date=datetime(2024, 7, 24),
    catchup=True,
    tags=['movie', 'megabox', 'team'],
) as dag:

    start = EmptyOperator(task_id='start')
    
    extract = EmptyOperator(task_id='movie.extract')
    transform = EmptyOperator(task_id='movie.transform')
    load = EmptyOperator(task_id='movie.load')

    end = EmptyOperator(task_id='end')
    
    start >> extract >> transform >> load >> end

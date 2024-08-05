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
    'extract_q3',
    default_args={
        'depends_on_past': False,
        'retries': 1,
        'retry_delay': timedelta(seconds=3),
    },

    max_active_runs=1,
    max_active_tasks=3,
    description='movie',
    schedule="10 2 * * *",
    start_date=datetime(2021, 9, 1),
    end_date=datetime(2021, 12, 31),
    catchup=True,
    tags=['movie', 'megabox', 'team'],
) as dag:
    
    def extract(**kwargs):
        from movie_extract.movie_e import df2parquet
        date = kwargs['ds_nodash']
        df = df2parquet(load_dt=date)

    
    start = EmptyOperator(task_id='start')
    
    t_extract = PythonVirtualenvOperator(
            task_id='movie.extract',
            python_callable=extract,
            requirements=["git+https://github.com/DE32megabox/extract.git@dev/d2.0.0"],
            system_site_packages=False
    )

    end = EmptyOperator(task_id='end')
    
    start >> t_extract >> end

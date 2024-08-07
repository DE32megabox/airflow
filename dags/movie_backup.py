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
    'movie_backup',
    default_args={
        'depends_on_past': False,
        'retries': 1,
        'retry_delay': timedelta(seconds=3),
    },

    max_active_runs=1,
    max_active_tasks=3,
    description='movie',
    schedule="10 2 * * *",
    start_date=datetime(2021, 12, 1),
    end_date=datetime(2021, 12, 31),
    catchup=True,
    tags=['movie', 'megabox', 'team'],
) as dag:
    
    def extract():
        from movie_extract.movie_e import ice_breaking
        ice_breaking()
    
    def transform():
        from transform.call import ice_breaking
        ice_breaking()

    def load():
        from de32_megabox_l.movie_l import ice_breaking
        ice_breaking()

    start = EmptyOperator(task_id='start')
    
    t_extract = PythonVirtualenvOperator(
            task_id='movie.extract',
            python_callable=extract,
            requirements=["git+https://github.com/DE32megabox/extract.git@dev/d1.0.0"],
            system_site_packages=False
    )

    t_transform = PythonVirtualenvOperator(
            task_id='movie.transform',
            python_callable=transform,
            requirements=["git+https://github.com/DE32megabox/transform.git@dev/d1.0.1"],
            system_site_packages=False
    )
    
    t_load = PythonVirtualenvOperator(
            task_id='movie.load',
            python_callable=load,
            requirements=["git+https://github.com/DE32megabox/load.git@dev/d1.0.0"],
            system_site_packages=False
    )

    end = EmptyOperator(task_id='end')
    
    start >> t_extract >> t_transform >> t_load >> end

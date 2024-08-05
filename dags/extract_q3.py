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
    
    def branch_func(ds_nodash):
        import os
        home_dir = os.path.expanduser("~")
        path = os.path.join(home_dir, f'megabox/tmp/movie_parquet/load_dt={{ds_nodash}}')
        if os.path.exists(path):
            return "rm.dir"
        else:
            return "movie.extract"
    
    start = EmptyOperator(task_id='start')
    
    branch_op = BranchPythonOperator(
            task_id="branch.op",
            python_callable=branch_func
    )

    rm_dir = BashOperator(
            task_id="rm.dir",
            bash_command="rm -rf ~/megabox/tmp/movie_parquet/load_dt={{ds_nodash}}",
    )

    t_extract = PythonVirtualenvOperator(
            task_id='movie.extract',
            python_callable=extract,
            requirements=["git+https://github.com/DE32megabox/extract.git@dev/d2.0.0"],
            system_site_packages=False,
            trigger_rule='all_done'
    )

    end = EmptyOperator(task_id='end')
    
    start >> branch_op >> t_extract 
    branch_op >> rm_dir >> t_extract 
    t_extract >> end

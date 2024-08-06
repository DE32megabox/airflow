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
    'load',
    default_args={
        'depends_on_past': False,
        'retries': 1,
        'retry_delay': timedelta(seconds=3),
    },

    max_active_runs=1,
    max_active_tasks=3,
    description='movie',
    schedule="10 2 * * *",
    start_date=datetime(2024, 8, 1),
    catchup=True,
    tags=['movie', 'megabox', 'team'],
) as dag:
    
    # Functions

    def load_data():
        from de32_megabox_l.movie_l import make_million_chart
        df = make_million_chart()
    
    def print_data():
        from de32_megabox_l.movie_l import make_million_chart, print_df
        print_df(df=make_million_chart())

    def save_data():
        from de32_megabox_l.movie_l import make_million_chart, save2parquet
        save2parquet(df=make_million_chart())

    
    # Tasks

    ## Start
    start = EmptyOperator(task_id='start')

    ## Process
    l_ld = PythonVirtualenvOperator(
            task_id='load.df',
            python_callable=load_data,
            requirements=["git+https://github.com/DE32megabox/load.git"],
            system_site_packages=False,
            trigger_rule='all_success'
            )

    l_pd = PythonVirtualenvOperator(
            task_id='load.pd',
            python_callable=print_data,
            requirements=["git+https://github.com/DE32megabox/load.git"],                      system_site_packages=False,
            trigger_rule='all_success'
            )


    l_sq = PythonVirtualenvOperator(
            task_id='load.sq',
            python_callable=save_data,
            requirements=["git+https://github.com/DE32megabox/load.git"],                      system_site_packages=False,
            trigger_rule='all_success'
            )

    ## End
    end = EmptyOperator(task_id='end')

    start >> l_ld >> l_pd >> l_sq >> end

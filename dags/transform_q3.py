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
    'transform_q3',
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
    end_date=datetime(2022, 1, 1),
    catchup=True,
    tags=['movie', 'megabox', 'team'],
) as dag:
    
    '''
    def extract(**kwargs):
        from movie_extract.movie_e import df2parquet
        date = kwargs['ds_nodash']
        df = df2parquet(load_dt=date)
    '''
    

    def process(ds_nodash):
        from transform.call import apply_type2df
        df = apply_type2df(load_dt=ds_nodash)

    def transform(ds_nodash):
        from transform.call import transform2df
        df = transform2df(load_dt=ds_nodash)

    def save_func(ds_nodash):
        from transform.call import save2df
        df = save2df(load_dt=ds_nodash)
    
    start = EmptyOperator(task_id='start')
    
    rm_dir = BashOperator(
            task_id="rm.dir",
            bash_command="rm -rf ~/megabox/tmp/transform_parquet/load_dt={{ds_nodash}}"
    ) 
    
    t_process = PythonVirtualenvOperator(
            task_id='movie.process',
            python_callable=process,
            requirements=["git+https://github.com/DE32megabox/transform.git"],
            system_site_packages=False,
            trigger_rule='all_success'
    )

    t_transform = PythonVirtualenvOperator(
            task_id='movie.transform',
            python_callable=transform,
            requirements=["git+https://github.com/DE32megabox/transform.git"],
            system_site_packages=False,
            trigger_rule='all_success'
    )

    save_data = PythonVirtualenvOperator(
            task_id='save.data',
            python_callable=save_func,
            requirements=["git+https://github.com/DE32megabox/transform.git"],
            system_site_packages=False,
            trigger_rule='all_success'
    )

    end = EmptyOperator(task_id='end')
    
    start >> t_process >> t_transform >> rm_dir >> save_data >> end

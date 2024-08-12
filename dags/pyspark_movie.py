from datetime import datetime, timedelta
from textwrap import dedent
from pprint import pprint

# The DAG object; we'll need this to instantiate a DAG
from airflow import DAG

# Operators; we need this to operate!
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator

from airflow.operators.python import (
        PythonOperator, 
        BranchPythonOperator, 
        PythonVirtualenvOperator,

)

with DAG(
    'pyspark_movie',
    default_args={
        'depends_on_past': True,
        'retries': 1,
        'retry_delay': timedelta(seconds=3),
    },
    max_active_runs=1,
    max_active_tasks=3,
    description='processing pyspark for movie data',
    schedule="10 2 * * *",
    start_date=datetime(2015, 1, 1),
    end_date=datetime(2025, 1, 5),
    catchup=True,
    tags=['api', 'movie', 'amt', 'pyspark'],
) as dag:

    def re_partition(ds_nodash):
        from spark_airflow.rep import re_partition
        df_row_cnt, read_path, write_path= re_partition(ds_nodash)
        print(f'df_row_cnt:{df_row_cnt}')
        print(f'read_path:{read_path}')
        print(f'write_path:{write_path}')
    

    re_task = PythonVirtualenvOperator(
        task_id='re.partition',
        python_callable=re_partition,
        system_site_packages=False,
        requirements=["git+https://github.com/tbongkim03/spark_airflow.git@0.2.0/airflowdag"],
        venv_cache_path='/home/tbongkim03/venv_cache/pyspark_movie/re_task'
    )

    join_df = BashOperator(
        task_id='join.df',
        bash_command='''
            $SPARK_HOME/bin/spark-submit /home/tbongkim03/code/spark_airflow/pyspark/join_df.py "join_df_app" {{ds_nodash}}
            '''
    )

    agg_df = BashOperator(
        task_id='agg.df',
        bash_command='''
            $SPARK_HOME/bin/spark-submit /home/tbongkim03/code/spark_airflow/pyspark/agg_movie.py "agg_movie_app" {{ds_nodash}}
            '''
    )
    
    start = EmptyOperator(task_id='start')
    end = EmptyOperator(task_id='end')
    
    # flow
    start >> re_task >> join_df >> agg_df >> end

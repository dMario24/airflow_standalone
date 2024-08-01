from datetime import datetime, timedelta

from airflow import DAG

from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator

from airflow.operators.python import (
        BranchPythonOperator, 
        PythonVirtualenvOperator,

)

with DAG(
    'movie_summary',
    default_args={
        'depends_on_past': False,
        'retries': 1,
        'retry_delay': timedelta(seconds=3),
    },
    max_active_runs=1,
    max_active_tasks=3,
    description=' movie summary',
    schedule="30 2 * * *",
    start_date=datetime(2024, 7, 24),
    catchup=True,
    tags=['api', 'movie', 'amt', 'agg', 'summary'],
) as dag:
    def get_empty(task_id):
        task = EmptyOperator(task_id="apply.type")
    
    start = get_empty('start')
    end = get_empty('start')
    apply_type = get_empty('apply.type')
    apply_type = get_empty('merge.df')
    de_dup = get_empty('de.dup ')
    summary_df  = get_empty('summary.df')


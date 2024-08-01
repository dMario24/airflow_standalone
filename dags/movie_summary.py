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
    def get_empty(id):
        task = EmptyOperator(task_id=id)
        return task
    
    start = get_empty('start')
    end = get_empty('end')
    apply_type = get_empty('apply.type')
    merge_df = get_empty('merge.df')
    de_dup = get_empty('de.dup')
    summary_df  = get_empty('summary.df')

    start >> apply_type >> merge_df >> de_dup >> summary_df >> end

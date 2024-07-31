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
    tags=['api', 'movie', 'amt'],
) as dag:

    def get_data(ds_nodash):
        from mov.api.call import save2df
        df = save2df(ds_nodash)
        print(df.head(5))

    def save_data(ds_nodash):
        from mov.api.call import apply_type2df
        
        df = apply_type2df(load_dt=ds_nodash)
        
        print("*" * 33)
        print(df.head(10))
        print("*" * 33)
        print(df.dtypes)

        # 개봉일 기준 그룹핑 누적 관객수 합
        print("개봉일 기준 그룹핑 누적 관객수 합")
        g = df.groupby('openDt')
        sum_df = g.agg({'audiCnt': 'sum'}).reset_index()
        print(sum_df)

    
    def branch_fun(ds_nodash):
        import os
        home_dir = os.path.expanduser("~")
        path = os.path.join(home_dir, f"tmp/test_parquet/load_dt={ds_nodash}")
        if os.path.exists(path):
            return rm_dir.task_id
        else:
            return "get.data", "echo.task"
            

    branch_op = BranchPythonOperator(
        task_id="branch.op",
        python_callable=branch_fun
    )
    
    get_data = PythonVirtualenvOperator(
        task_id='get.data',
        python_callable=get_data,
        requirements=["git+https://github.com/dMario24/mov.git@0.3/api"],
        system_site_packages=False,
        trigger_rule="all_done",
        #venv_cache_path="/home/diginori/tmp2/air_venv/get_data"
    )

    save_data = PythonVirtualenvOperator(
        task_id='save.data',
        python_callable=save_data,
        system_site_packages=False,
        trigger_rule="one_success",
        requirements=["git+https://github.com/dMario24/mov.git@0.3/api"],
        #venv_cache_path="/home/diginori/tmp2/air_venv/get_data"
    )

    rm_dir = BashOperator(
        task_id='rm.dir',
        bash_command='rm -rf ~/tmp/test_parquet/load_dt={{ ds_nodash }}',
    )

    echo_task = BashOperator(
        task_id='echo.task',
        bash_command="echo 'task'"
    )
    
    start = EmptyOperator(task_id='start')
    end = EmptyOperator(task_id='end')
    
    multi_y = EmptyOperator(task_id='multi.y') # 다양성 영화 유무
    multi_n = EmptyOperator(task_id='multi.n')
    nation_k = EmptyOperator(task_id='nation.k') # 한국외국영화
    nation_f = EmptyOperator(task_id='nation.f')
    
    join_task = BashOperator(
            task_id='join',
            bash_command="exit 1",
            trigger_rule="all_done"

    )
    
    start >> branch_op
    start >> join_task >> save_data

    branch_op >> rm_dir >> [get_data, multi_y, multi_n, nation_k, nation_f] 
    branch_op >> echo_task >> save_data
    branch_op >> [get_data, multi_y, multi_n, nation_k, nation_f]

    [get_data, multi_y, multi_n, nation_k, nation_f] >> save_data >> end


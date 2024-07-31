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

    def common_get_data(ds_nodash, url_param):
        from mov.api.call import save2df
        df = save2df(load_dt=ds_nodash, url_param=url_param)
        print(df[['movieCd', 'movieNm']].head(5))
        
        for k, v in url_param.items():
            df[k] = v
        
        p_cols = ['load_dt'] + list(url_param.keys())
        df.to_parquet('~/tmp/test_parquet', partition_cols=p_cols)

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
            return "get.start", "echo.task"
            

    branch_op = BranchPythonOperator(
        task_id="branch.op",
        python_callable=branch_fun
    )
    
    get_data = PythonVirtualenvOperator(
        task_id='get.data',
        python_callable=get_data,
        requirements=["git+https://github.com/dMario24/mov.git@0.3.3/url_param"],
        system_site_packages=False,
    )

    save_data = PythonVirtualenvOperator(
        task_id='save.data',
        python_callable=save_data,
        system_site_packages=False,
        trigger_rule="one_success",
        requirements=["git+https://github.com/dMario24/mov.git@0.3.3/url_param"],
    )

    # 다양성 영화 유무
    multi_y = PythonVirtualenvOperator(
        task_id='multi.y',
        python_callable=common_get_data,
        system_site_packages=False,
        requirements=["git+https://github.com/dMario24/mov.git@0.3.3/url_param"],
        #op_args=["{{ds_nodash}}", "{{ds}}"],
        op_kwargs={
            "url_param": {"multiMovieYn": "Y"}
        },
    )

    multi_n = PythonVirtualenvOperator(
        task_id='multi.n',
        python_callable=common_get_data,
        system_site_packages=False,
        requirements=["git+https://github.com/dMario24/mov.git@0.3.3/url_param"],
        #op_args=["{{ds_nodash}}", "{{ds}}"],
        op_kwargs={
            "url_param": {"multiMovieYn": "N"}
        }
    )

    nation_k = PythonVirtualenvOperator(
        task_id='nation.k',
        python_callable=common_get_data,
        system_site_packages=False,
        requirements=["git+https://github.com/dMario24/mov.git@0.3.3/url_param"],
        #op_args=["{{ds_nodash}}", "{{ds}}"],
        op_kwargs={
            "url_param": {"repNationCd": "K"}
        }
    )

    nation_f = PythonVirtualenvOperator(
        task_id='nation.f',
        python_callable=common_get_data,
        system_site_packages=False,
        requirements=["git+https://github.com/dMario24/mov.git@0.3.3/url_param"],
        #op_args=["{{ds_nodash}}", "{{ds}}"],
        op_kwargs={
            "url_param": {"repNationCd": "F"}
        }
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
    
    
    
    get_start = EmptyOperator(
                    task_id='get.start',
                    trigger_rule="all_done"
                )
    get_end = EmptyOperator(task_id='get.end')
    
    throw_err = BashOperator(
            task_id='throw.err',
            bash_command="exit 1",
            trigger_rule="all_done"

    )
    
    start >> branch_op
    start >> throw_err >> save_data

    branch_op >> rm_dir >> get_start
    branch_op >> get_start
    branch_op >> echo_task
    get_start >> [get_data, multi_y, multi_n, nation_k, nation_f] >> get_end

    get_end >> save_data >> end


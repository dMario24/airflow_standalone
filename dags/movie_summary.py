from datetime import datetime, timedelta

from airflow import DAG

from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator

from airflow.operators.python import (
        BranchPythonOperator, 
        PythonVirtualenvOperator,
        PythonOperator,
)

#import pprint
from pprint import pprint as pp

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
    REQUIREMENTS = [
                "git+https://github.com/dMario24/mov.git@0.3.3/url_param",
                ]
    def gen_empty(*ids):
        tasks = []
        for id in ids:
            task = EmptyOperator(task_id=id)
            tasks.append(task)
        return tuple(tasks) # (t, )

    def gen_vpython(**kw):
        #task = PythonVirtualenvOperator(
        task = PythonOperator(
                task_id=kw['id'],
                python_callable=kw['fun_obj'],
                #system_site_packages=False,
                #requirements=REQUIREMENTS,
                op_kwargs=kw['op_kw']
            )
        return task
    
    def pro_data(**params):
        print("@" * 33)
        print(params['task_name'])
        pp(params) # 여기는 task_name
        print("@" * 33)

    def pro_data2(task_name, **params):
        print("@" * 33)
        print(task_name)
        pp(params) # 여기는 task_name 없을 것으로 예상
        if "task_name" in params.kesy():
            print("============== 있음")
        else:
            print("============== 없음")
        print("@" * 33)

    def pro_data3(task_name):
        print("@" * 33)
        print(task_name)
        #print(params) # 여기는 task_name 없을 것으로 예상
        print("@" * 33)
    
    def pro_data4(task_name, ds_nodash, **kwargs):
        print("@" * 33)
        print(task_name)
        print(ds_nodash)
        pp(kwargs) # 여기는 task_name 없을 것으로 예상, ds_nodash 도 없 ...
        print("@" * 33)
    
    start, end = gen_empty('start', 'end')
    
    apply_type = gen_vpython(
            id = "appply.type",
            fun_obj = pro_data,
            op_kw = { "task_name": "apply_type!!!" }
            )
    merge_df = gen_vpython(
            id = "merge.df",
            fun_obj = pro_data2,
            op_kw = { "task_name": "merge_df!!!" }
            )
    de_dup = gen_vpython(
            id = "de.dup",
            fun_obj = pro_data3,
            op_kw = { "task_name": "du_dup!!!" }
            )
    summary_df = gen_vpython(
            id = "summary.df",
            fun_obj = pro_data4,
            op_kw = { "task_name": "summary_df!!!" }
            )

    start >> apply_type >> merge_df
    merge_df >> de_dup >> summary_df >> end

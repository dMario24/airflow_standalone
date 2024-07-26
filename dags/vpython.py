from datetime import datetime, timedelta
from textwrap import dedent

# The DAG object; we'll need this to instantiate a DAG
from airflow import DAG

# Operators; we need this to operate!
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import (
    ExternalPythonOperator,
    PythonOperator,
    PythonVirtualenvOperator,
    is_venv_installed,
)


with DAG(
    'vpy',
    # These args will get passed on to each operator
    # You can override them on a per-task basis during operator initialization
    default_args={
        'depends_on_past': True,
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5)
    },
    description='hello world DAG',
    schedule="10 2 * * *",
    start_date=datetime(2024, 7, 24),
    catchup=True,
    tags=['helloworld'],
) as dag:

    # t1, t2 and t3 are examples of tasks created by instantiating operators
    t1 = BashOperator(
        task_id='print_date',
        bash_command='date',
    )

    def callable_virtualenv(ds, **kwargs):
        """
        Example function that will be performed in a virtual environment.

        Importing at the module level ensures that it will not attempt to import the
        library before it is installed.
        """
        # from time import sleep

        # from colorama import Back, Fore, Style

        # print(Fore.RED + "some red text")
        # print(Back.GREEN + "and with a green background")
        # print(Style.DIM + "and in dim text")
        # print(Style.RESET_ALL)
        # for _ in range(4):
        #     print(Style.DIM + "Please wait...", flush=True)
        #     sleep(1)
        # print("Finished")

        print( "*" * 30)
        print(ds)
        print(kwargs)
        print( "*" * 30)

        from mah.db.utils import read_data
        df = read_data()
        print(df.head(10))

    virtualenv_task = PythonVirtualenvOperator(
        task_id="virtualenv_python",
        python_callable=callable_virtualenv,
        requirements=[
            "colorama==0.4.0",
            "git+https://github.com/dMario24/mah.git@0.2.0/args"               ],
        system_site_packages=False,
    )

    

    start = EmptyOperator(task_id='start')
    end = EmptyOperator(task_id='end')
    
    start >> t1 >> end
    start >> virtualenv_task >> end

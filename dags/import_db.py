from datetime import datetime, timedelta
from textwrap import dedent

from airflow import DAG

from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.models import Variable

MY_NAME = Variable.get("MY_NAME")

with DAG(
        'import_db',
    default_args={
        'depends_on_past': False,
        'retries': 1,
        'retry_delay': timedelta(seconds=3)
    },
    description='history log 2 mysql db',
    schedule="10 4 * * *",
    start_date=datetime(2024, 7, 10),
    catchup=True,
    tags=['simple', 'bash', 'etl', 'shop', "db", "history"],
) as dag:

    

    task_check = BashOperator(
        task_id="check.done",
        bash_command="""
            bash {{ var.value.CHECK_SH }} ~/data/done/{{ds_nodash}}/_DONE
            """
    )

    task_to_csv = BashOperator(
        task_id="to.csv",
        bash_command="""
            echo "to.csv"

            U_PATH=~/data/count/{{ds_nodash}}/count.log
            CSV_PATH=~/data/csv/{{ds_nodash}}
            CSV_FILE=~/data/csv/{{ds_nodash}}/csv.csv
            
            mkdir -p $CSV_PATH
           
            cat $U_PATH | awk '{print "^{{ds}}^,^" $2 "^,^" $1 "^"}' > ${CSV_FILE}
            echo $CSV_PATH
        """
    )

    task_create_tbl = BashOperator(
        task_id="create.table",
        bash_command="""
            SQL={{ var.value.SQL_PATH }}/create_db_table.sql
            echo "SQL_PATH=$SQL"
            MYSQL_PWD='{{ var.value.DB_PASSWD }}' mysql -u root < $SQL
        """
    )

    task_to_tmp = BashOperator(
        task_id="to.tmp",
        bash_command="""
            echo "to.tmp"
            CSV_FILE=~/data/csv/{{ds_nodash}}/csv.csv
            echo $CSV_FILE
            bash {{ var.value.SH_HOME }}/csv2mysql.sh $CSV_FILE {{ ds }}
        """
    )

    task_to_base = BashOperator(
        task_id="to.base",
        bash_command="""
            echo "to.base"
            bash {{ var.value.SH_HOME }}/tmp2base.sh {{ ds }}
        """
    )

    task_done = BashOperator(
        task_id="make.done",
        bash_command="""
            figlet "make.done.start"
            
            DONE_PATH={{ var.value.IMPORT_DONE_PATH }}/{{ds_nodash}}
            mkdir -p $DONE_PATH
            echo "IMPORT_DONE_PATH=$DONE_PATH"
            touch $DONE_PATH/_DONE

            figlet "make.done.end"
        """
    )

    task_err = BashOperator(
        task_id="err.report",
        bash_command="""
            echo "err report"
        """,
        trigger_rule="one_failed"
    )

    task_end = EmptyOperator(task_id='end', trigger_rule="all_done")
    task_start = EmptyOperator(task_id='start')

    task_start >> task_check >> task_to_csv
    task_to_csv >> task_create_tbl
    task_create_tbl >> task_to_tmp >> task_to_base >> task_done
    task_done >> task_end

    task_check >> task_err >> task_end

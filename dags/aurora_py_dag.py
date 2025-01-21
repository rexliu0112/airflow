from __future__ import annotations

import textwrap
from datetime import datetime, timedelta
from airflow.operators.python import PythonOperator 
from airflow.operators.bash import BashOperator
from airflow.models.dag import DAG
from airflow.operators.bash import BashOperator
import pendulum
# [END import_module]
def execute_py(script_path):
    import subprocess
    subprocess.run(['python', script_path], check=True)

# [START instantiate_dag]
with DAG(
    "aurora_py_etl",
    # [START default_args]
    # These args will get passed on to each operator
    # You can override them on a per-task basis during operator initialization
    default_args={ 
        "owner" : 'Rex',
        "depends_on_past": False,
        "email": ["leo22155@yahoo.com.tw"],
        "email_on_failure": False,
        "email_on_retry": False,
        "retries": 1,
        "retry_delay": timedelta(minutes=5),
        # 'queue': 'bash_queue',
        # 'pool': 'backfill',
        # 'priority_weight': 10,
        # 'end_date': datetime(2016, 1, 1),
        # 'wait_for_downstream': False,
        # 'sla': timedelta(hours=2),
        # 'execution_timeout': timedelta(seconds=300),
        # 'on_failure_callback': some_function, # or list of functions
        # 'on_success_callback': some_other_function, # or list of functions
        # 'on_retry_callback': another_function, # or list of functions
        # 'sla_miss_callback': yet_another_function, # or list of functions
        # 'on_skipped_callback': another_function, #or list of functions
        # 'trigger_rule': 'all_success'
    },
    # [END default_args]
    description="aurora_py_dag",
    schedule=timedelta(days=1),
    start_date=pendulum.today('UTC').add(days=-2),
    catchup=False,
    tags=["Rex_Test","ETL","aurora"],
) as dag:
    # [END instantiate_dag]

    # t1, t2 and t3 are examples of tasks created by instantiating operators


    #dag.doc_md = __doc__  # providing that you have a docstring at the beginning of the DAG; OR
    dag.doc_md = """
    This is a documentation placed anywhere
    """  # otherwise, type it like this
    
     # 執行 print_trend.py
    task_print_trend = BashOperator(
        task_id='run_print_trend',
        bash_command='python3 /mnt/d/aurora/py_scripts/print_trend.py',
    )
    # 執行 revenue_trend.py
    task_revenue_trend = BashOperator(
        task_id='run_revenue_trend',
        bash_command='python3 /mnt/d/aurora/py_scripts/evenue_trend.py',
    )

    # 執行 print_peak_IOT.py
    task_print_peak_IOT = BashOperator(
        task_id='run_print_peak_IOT',
        bash_command='python3 /mnt/d/aurora/py_scripts/print_peak_IOT.py',
    )

    # 執行 print_peak_month.py
    task_print_peak_month = BashOperator(
        task_id='run_print_peak_month',
        bash_command='python3 /mnt/d/aurora/py_scripts/print_peak_month.py',
    )

    # 定義執行順序
[task_print_trend, task_revenue_trend] >> task_print_peak_IOT >> task_print_peak_month    # [END documentation]

    # [START jinja_template]

# [END tutorial]
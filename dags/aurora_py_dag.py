from __future__ import annotations

import textwrap
from datetime import datetime, timedelta
from airflow.operators.python import PythonOperator 
from airflow.operators.bash import BashOperator
from airflow.models.dag import DAG
from airflow.operators.bash import BashOperator
import pendulum
from airflow import DAG
from airflow.utils.dates import days_ago

# 設定時區為台北時間
local_tz = pendulum.timezone("Asia/Taipei")

# [END import_module]
def execute_py(script_path):
    import subprocess
    subprocess.run(['python', script_path], check=True)
Comp = 'A0A2'
today = '2025-02-12'
dbo_schema = 'dbo_A0A2' 
datamart_schema = 'datamart_A0A2'
feature_schema = 'feature_A0A2'

def run_py_command(py_file,Comp,today,dbo_schema,datamart_schema,feature_schema):
    command_string = f"python3 /mnt/d/aurora/py_scripts/{py_file} --Comp {Comp} --today {today} --dbo_schema {dbo_schema} --datamart_schema {datamart_schema} --feature_schema {feature_schema}"
    return command_string
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
    },#
    # [END default_args]
    description="aurora_py_dag",
    schedule="18 19 * * 1-5",
    start_date=pendulum.datetime(2025, 2, 11, tz="Asia/Taipei"),
    catchup=False,
    tags=["Rex_Test","ETL","aurora"],
) as dag:
   


    #dag.doc_md = __doc__  # providing that you have a docstring at the beginning of the DAG; OR
    dag.doc_md = """
    This is a documentation placed anywhere
    """  # otherwise, type it like this
    
     # 執行 print_trend.py
    task_print_trend = BashOperator(
        task_id='run_print_trend',
        bash_command=run_py_command('print_trend.py',Comp,today,dbo_schema,datamart_schema,feature_schema),
    )
    # 執行 revenue_trend.py
    task_revenue_trend = BashOperator(
        task_id='run_revenue_trend',
        bash_command=run_py_command('revenue_trend.py',Comp,today,dbo_schema,datamart_schema,feature_schema),
    )

    # 執行 print_peak_IOT.py
    task_print_peak_IOT = BashOperator(
        task_id='run_print_peak_IOT',
        bash_command=run_py_command('print_peak_IOT.py',Comp,today,dbo_schema,datamart_schema,feature_schema),
    )

    # 執行 print_peak_month.py
    task_print_peak_month = BashOperator(
        task_id='run_print_peak_month',
        bash_command=run_py_command('print_peak_month.py',Comp,today,dbo_schema,datamart_schema,feature_schema),
    )

    # 定義執行順序
[task_print_trend, task_revenue_trend] >> task_print_peak_IOT >> task_print_peak_month    # [END documentation]


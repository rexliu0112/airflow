from __future__ import annotations

import textwrap
from datetime import datetime, timedelta
from airflow.operators.python import PythonOperator 
from airflow.operators.bash import BashOperator
from airflow.models.dag import DAG
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
import pendulum
from airflow.utils.dates import days_ago

# 設定時區為台北時間
local_tz = pendulum.timezone("Asia/Taipei")

Comp = 'A0A2'
today = '2025-02-17'
dbo_schema = 'dbo_A0A2' 
datamart_schema = 'datamart_A0A2'
feature_schema = 'feature_A0A2'

# [START instantiate_dag]
with DAG(
    "aurora_datamart_ETL_dag",
    # [START default_args]
    # These args will get passed on to each operator
    # You can override them on a per-task basis during operator initialization
    default_args={ 
        "owner" : 'Rex',
        "depends_on_past": False,
        "email": ["leo22155@yahoo.com.tw"],
        "email_on_failure": False,
        "email_on_retry": False,
        "retries": 0,
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
    description="aurora_datamart_ETL_dag",
    schedule="38 15 * * 1-5",
    start_date=pendulum.today('Asia/Taipei').add(days=-3),
    #start_date=pendulum.datetime(2025, 2, 15, tz="Asia/Taipei"),
    catchup=False,
    tags=["Rex_Test","ETL","aurora"],
) as dag:
   


    #dag.doc_md = __doc__  # providing that you have a docstring at the beginning of the DAG; OR
    dag.doc_md = """
    This is a documentation placed anywhere
    """
    
     # 執行 print_trend.py
    DLContractMaster = SQLExecuteQueryOperator( 
        task_id='DLContractMaster',
        sql = 'sql_scripts/1_DLContractMaster.sql',
        conn_id='aurora_stg',
        params={"dbo_schema": dbo_schema,"datamart_schema":datamart_schema,'today':today}
     
    )
    #
    
    # 執行 print_trend.py
    DLContractPrintRange = SQLExecuteQueryOperator(
        task_id='DLContrDLContractPrintRangeactMaster',
        sql = 'sql_scripts/2_DLContractPrintRange.sql',
        conn_id='aurora_stg',
        params={"dbo_schema": dbo_schema,"datamart_schema":datamart_schema,'today':today}
    )
    
    
    CONSUMP_DETAIL = SQLExecuteQueryOperator(
        task_id='CONSUMP_DETAIL',
        sql = 'sql_scripts/3_CONSUMP_DETAIL.sql',
        conn_id='aurora_stg',
        params={"dbo_schema": dbo_schema,"datamart_schema":datamart_schema,'today':today}
    )
    

    # 定義執行順序
DLContractMaster >> DLContractPrintRange >> CONSUMP_DETAIL    # [END documentation]


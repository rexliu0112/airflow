from __future__ import annotations

import textwrap
from datetime import datetime, timedelta
from airflow.models.dag import DAG
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.operators.email import EmailOperator
from airflow.operators.empty import EmptyOperator
import pendulum
#from airflow.utils.dates import days_ago
from airflow.models import Variable
from airflow.datasets import Dataset


# 設定時區為台北時間
local_tz = pendulum.timezone("Asia/Taipei")
#設定一些程式要跑的參數
Comp = 'A0A2'
today = '2025-02-17'
dbo_schema = 'dbo_A0A2' 
datamart_schema = 'datamart_A0A2'
feature_schema = 'feature_A0A2'

#設定email 的 LIST
etl_email_list = Variable.get("etl_email_list",deserialize_json=True)

#設定Dataset作為依賴關係用
datamart_update = Dataset("mysql://aurora/datamart/schema_update")

# [START instantiate_dag]
with DAG(
    "aurora_datamart_ETL_dag",
    # [START default_args]
    # These args will get passed on to each operator
    # You can override them on a per-task basis during operator initialization
    default_args={ 
        "owner" : 'Rex',
        "depends_on_past": False,
        "email": etl_email_list,
        "email_on_failure": True,
        "email_on_retry": True,
        "retries": 0,
        "retry_delay": timedelta(seconds=5),
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
    schedule="45 14 * * 3-4",
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
        task_id='DLContractPrintRange',
        sql = 'sql_scripts/2_DLContractPrintRange.sql',
        conn_id='aurora_stg',
        params={"dbo_schema": dbo_schema,"datamart_schema":datamart_schema,'today':today}
    )
    
    
    CONSUMP_DETAIL = SQLExecuteQueryOperator(
        task_id='CONSUMP_DETAIL',
        sql = 'sql_scripts/3_CONSUMP_DETAIL.sql',
        conn_id='aurora_stg',
        params={"dbo_schema": dbo_schema,"datamart_schema":datamart_schema,'today':today},
        
    )
    
    current_time = datetime.now()
    html_content = f"""
        <html>
        <head>
            <meta charset="UTF-8">
            <title>aurora_datmart_ETL 完成通知</title>
        </head>
        <body>
            <p>您好，</p>
            <p>系統通知：<strong>datmart_ETL</strong> 已於 <strong>{current_time}</strong> 順利完成。</p>
            <p>請前往檢查相關結果。</p>
            <p>謝謝！</p>
        </body>
        </html>
        """
    email_args = {
    'email_on_failure': True,
    'email_on_rerty' :True,
    'retries':1
    }
    
    EMAIL_TASK  = EmailOperator(
        task_id = 'SEDN_MAIL',
        to = etl_email_list,
        subject = 'datamart_ETL 完成通知',
        html_content= html_content,
        conn_id='airflow_email'
    )
    
    
    TAG_UPDATED = EmptyOperator(
        task_id="TAG_UPDATED",
        outlets=[datamart_update]
    )
    
    
    

    # 定義執行順序
DLContractMaster >> DLContractPrintRange >> CONSUMP_DETAIL >> [EMAIL_TASK,TAG_UPDATED] # [END documentation]


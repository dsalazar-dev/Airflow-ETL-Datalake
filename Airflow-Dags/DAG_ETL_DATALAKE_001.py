from datetime import datetime
from airflow import DAG

import os

from airflow.operators.python_operator import PythonOperator

from PACKAGE.DATALAKE.INT_ETL_BRONZE import EL_TO_BRONZE
from PACKAGE.DATALAKE.INT_ETL_SILVER import TL_TO_SILVER
from PACKAGE.DATALAKE.INT_ETL_GOLD import TL_TO_GOLD


default_args={
    'owner': 'DanielDev',
    'depends_on_past': False,
    'email': 'salazarm.daniel@outlook.com'
}

CUR_DIR = os.path.abspath(os.path.dirname(__file__))

with DAG(
    dag_id = 'DAG_ETL_DATALAKE_001',
    description = 'ETL PROCESS DATALAKE',
    start_date = datetime(2021, 4, 1),
    default_args = default_args,
) as dag:
    
    dag_etl_datalake_bronce = PythonOperator(
        task_id = 'task_etl_datalake_bronze',
        python_callable = EL_TO_BRONZE,
        op_kwargs = {
            'layer': 'INT_DATALAKE_BRONZE',
            'table': 'TBL_RAW_DATA',
            'source_type': 'file',
            'source_dir' : CUR_DIR,
            'file_path': CUR_DIR + '/PACKAGE/DATALAKE/Data/trips.csv',
        }
    )

    dag_etl_datalake_silver = PythonOperator(
        task_id = 'task_etl_datalake_silver',
        python_callable = TL_TO_SILVER,
        op_kwargs = {
            'layer_source': 'INT_DATALAKE_BRONZE',
            'table_source': 'TBL_RAW_DATA',
            'layer': 'INT_DATALAKE_SILVER',
            'table': 'TBL_REFINED_DATA',
            'source_dir' : CUR_DIR,
        }
    )

    dag_etl_datalake_gold = PythonOperator(
        task_id = 'task_etl_datalake_gold',
        python_callable = TL_TO_GOLD,
        op_kwargs = {
            'layer_source': 'INT_DATALAKE_SILVER',
            'table_source': 'TBL_REFINED_DATA',
            'layer': 'INT_DATALAKE_GOLD',
            'table': 'TBL_AGG_DATA',
            'source_dir' : CUR_DIR,
        }
    )

    dag_etl_datalake_bronce >> dag_etl_datalake_silver >> dag_etl_datalake_gold
    

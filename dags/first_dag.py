from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.hooks.postgres_hook import PostgresHook
from datetime import datetime, timedelta
import pandas as pd

from sqlalchemy import create_engine
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2024, 1, 1),
}

def extract_data(**context):
    crash_file_csv = '/usr/local/airflow/data/transformation_data/traffic_crashes.csv'
    vehicle_crash_csv = '/usr/local/airflow/data/transformation_data/traffic_crash_vehicle.csv'
    try:
        df_crashes = pd.read_csv(crash_file_csv)
        df_vehicles = pd.read_csv(vehicle_crash_csv)
        logger.info("CSV files read successfully")
        
        # Push the extracted data to XCom
        context['ti'].xcom_push(key='df_crashes', value=df_crashes.to_json())
        context['ti'].xcom_push(key='df_vehicles', value=df_vehicles.to_json())
        logger.info("Extracted data pushed to XCom")
    except Exception as e:
        logger.error(f"An error occurred while reading CSV files: {e}")

def transform_data(**context):
    try:
        # Pull the extracted data from XCom
        instance = context['ti']
        df_crashes_json = instance.xcom_pull(key='df_crashes', task_ids='extract_data')
        df_vehicles_json = instance.xcom_pull(key='df_vehicles', task_ids='extract_data')
        df_crashes = pd.read_json(df_crashes_json)
        df_vehicles = pd.read_json(df_vehicles_json)
        logger.info("Extracted data loaded successfully from XCom")

        # Clean data
        threshold_removed = df_crashes.dropna(axis='index', thresh=2, inplace=False)
        threshold_row = df_crashes[~df_crashes.index.isin(threshold_removed.index)]
        df_crashes.fillna(value={'report_type': 'ON SCENE'}, inplace=True)
        
        # Merge crashes and vehicles
        df = df_crashes.merge(df_vehicles, how='left', on='crash_record_id', suffixes=('_left', '_right'))
        df_agg = df.groupby('vehicle_type').agg({'crash_record_id': 'count'}).reset_index()
        
        # Transform column names for output data
        vehicle_mapping = {'vehicle_type': 'vehicletypes'}
        df_agg = df_agg.rename(columns=vehicle_mapping)
        
        # Push the transformed DataFrame to XCom
        df_agg.to_csv('/tmp/agg_data.csv', index=None, header=False)
    except Exception as e:
        logger.error(f"An error occurred during data transformation: {e}")

def load_data():
  try:
    # Connect to the Postgres connection
    hook = PostgresHook(postgres_conn_id = 'postgres')

    # Insert the data from the CSV file into the postgres database
    hook.copy_expert(
        sql = "COPY pipeline FROM stdin WITH DELIMITER as ','",
        filename='/tmp/agg_data.csv'
    )
    logger.info("Data loaded successfully")
  except Exception as e:
      logger.error(f'An error occured during data loading{e}')

with DAG(
    dag_id='first_dag',
    default_args=default_args,
    schedule_interval='@daily',
    catchup=False
) as dag:
        #dag 1
    extract_task = PythonOperator(
        task_id='extract_data',
        python_callable=extract_data,
        provide_context=True,
    )
        #dag 2
    create_table = PostgresOperator(
        task_id = 'create_table',
        postgres_conn_id='postgres',
        sql='''
            drop table if exists pipeline;
            create table pipeline(
                vehicletypes TEXT,
                crash_record_id INT
            );
        '''
    )
       #dag 3
    transform_task = PythonOperator(
        task_id='transform_data',
        python_callable=transform_data,
        provide_context=True,
    )
        #dag 4
    load_task = PythonOperator(
        task_id='load_data',
        python_callable=load_data,
        provide_context=True,
    )

    # Set task dependencies
    extract_task >> create_table >> transform_task >> load_task


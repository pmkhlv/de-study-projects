from email.policy import default
from airflow.contrib.sensors.file_sensor import FileSensor
from airflow.operators.python import PythonOperator
from airflow.providers.vertica.operators.vertica import VerticaOperator
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor
from airflow.models import Variable
from airflow.decorators import dag
from airflow.utils.dates import days_ago
from airflow.hooks.base import BaseHook
import boto3
import json
from airflow import DAG
import logging
import vertica_python


default_args = {
    'owner': 'airflow_admin',
    'email': ['airflow_admin@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'start_date':days_ago(1)
    }

vertica_conn = BaseHook.get_connection('vertica_conn')

AWS_ACCESS_KEY_ID = Variable.get("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = Variable.get("AWS_SECRET_ACCESS_KEY")

filename = 'group_log.csv'

def get_files():

    logging.info(f'Connecting to Yandex-S3')

    session = boto3.session.Session()
    s3_client = session.client(
        service_name='s3', 
        endpoint_url='https://storage.yandexcloud.net',
        aws_access_key_id=AWS_ACCESS_KEY_ID,
        aws_secret_access_key=AWS_SECRET_ACCESS_KEY
    )
    
    logging.info(f'Start downloading {filename} from Yandex-S3')
    
    s3_client.download_file(
        Bucket='sprint6',
        Key=filename,
        Filename=f"/data/{filename}")

def load_to_stg():
        
    with vertica_python.connect(vertica_conn) as conn:
        cur = conn.cursor()
        cur.execute("""
                    COPY TELEGRAMTELEGRAMYANDEXRU__STAGING.group_log (
                        group_id,user_id,user_id_from,event,datetime
                    )
                    FROM LOCAL '/data/group_log.csv'
                    DELIMITER ','
                    REJECTED DATA AS TABLE group_log_rej;
                    """)
    logging.info("Data was succesfully loaded to STD.")

with DAG(
    'project_5_loading_group_log',
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    tags=['project_5'],
    is_paused_upon_creation=False
) as dag:

    s3_file_sensor = S3KeySensor(
        task_id='s3_check_if_file_exists',
        poke_interval=2,
        timeout=1,
        retries=1, 
        bucket_key='group_log.csv', 
        bucket_name='sprint_6', 
        aws_conn_id="s3_conn",
        dag=dag)

    
    get_file_from_s3 = PythonOperator(
        task_id='download_group_log',   
        python_callable=get_files,
        dag=dag
    )

    local_file_sensor = FileSensor(
        task_id='sensing_group_log',
        filepath='/data/group_log.csv',
        fs_conn_id='fs_local',
        poke_interval=5,
        dag=dag
    )

    load_file_to_stg = PythonOperator(
        task_id='load_to_stg',
        python_callable=load_to_stg,
        dag=dag
    )

    load_dwh_l_user_group_activity = VerticaOperator(
        task_id='load_l_user_group_activity',
        vertica_conn_id='vertica_conn',
        sql="""
            INSERT INTO TELEGRAMTELEGRAMYANDEXRU__DWH.l_user_group_activity
            (hk_l_user_group_activity, hk_user_id,hk_group_id,load_dt,load_src)

            SELECT DISTINCT

                hash(hu.hk_user_id, hg.hk_group_id),
                hu.hk_user_id,
                hg.hk_group_id,
                now() as load_dt,
                's3' as load_src

            FROM TELEGRAMTELEGRAMYANDEXRU__STAGING.group_log as gl
            LEFT JOIN TELEGRAMTELEGRAMYANDEXRU__DWH.h_users hu ON gl.user_id = hu.user_id 
            LEFT JOIN TELEGRAMTELEGRAMYANDEXRU__DWH.h_groups hg ON gl.group_id = hg.group_id 
            WHERE hash(hu.hk_user_id, hg.hk_group_id) NOT IN (
                SELECT hk_l_user_group_activity 
                FROM TELEGRAMTELEGRAMYANDEXRU__DWH.l_user_group_activity
	        ); 
            """
    )

    load_dwh_s_auth_history = VerticaOperator(
        task_id='load_dwh_s_auth_history',
        vertica_conn_id='vertica_conn',
        sql="""
            INSERT INTO TELEGRAMTELEGRAMYANDEXRU__DWH.s_auth_history
            (hk_l_user_group_activity, user_id_from, event, event_dt, load_dt, load_src)

            SELECT
                luga.hk_l_user_group_activity,
                gl.user_id_from,
                gl.event,
                gl.group_log_dt AS event_dt,
                now()::timestamp AS load_dt,
                's3' AS load_src
            from TELEGRAMTELEGRAMYANDEXRU__STAGING.group_log as gl
            left join TELEGRAMTELEGRAMYANDEXRU__DWH.h_groups as hg on gl.group_id = hg.group_id
            left join TELEGRAMTELEGRAMYANDEXRU__DWH.h_users as hu on gl.user_id = hu.user_id
            left join TELEGRAMTELEGRAMYANDEXRU__DWH.l_user_group_activity as luga on hg.hk_group_id = luga.hk_group_id and hu.hk_user_id = luga.hk_user_id;
            """
    )


    get_file_from_s3 >> local_file_sensor >> load_file_to_stg >> load_dwh_l_user_group_activity >> load_dwh_s_auth_history

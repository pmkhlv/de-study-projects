import requests
import logging
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.operators.python import PythonOperator
from datetime import datetime
from airflow import DAG


default_args = {
  'owner': 'airflow_admin',
  'email': ['airflow_admin@example.com'],
  'email_on_failure': False,
  'email_on_retry': False
  }


url = 'https://d5d04q7d963eapoepsqr.apigw.yandexcloud.net/'
postgres_conn_id = 'PG_CONN'
hook = PostgresHook(postgres_conn_id=postgres_conn_id)
pg_conn = hook.get_conn()
headers={
    "X-API-KEY": "25c27781-8fde-4b30-a22e-524044a7580f",
    "X-Nickname": "pmkhlv",
    "X-Cohort": "3"
    }
source_obj = ['restaurants', 'couriers', 'deliveries']


def stg_load():

    for obj in source_obj:
        logging.info(f'--- Start working with {obj} source. ---')
        if obj != 'deliveries':
            cur = pg_conn.cursor()
            cur.execute(
                    """
                    TRUNCATE TABLE stg.{} RESTART IDENTITY;
                    """.format(obj)
                )
            pg_conn.commit()
            cur.close()
            
        cur = pg_conn.cursor()
        cur.execute(
                """
                SELECT column_name
                FROM information_schema.columns
                WHERE table_schema = 'stg'
                AND table_name = '{}';
                """.format(obj, obj)
            )
        cols = [col[0] for col in cur.fetchall()]
        logging.info(f'--- Following columns were received from database: {cols}. ---')
        cur.close()
        
        rows = []
        offset = 0 

        while True:
            responce = requests.get(
                url = f"{url}{obj}?offset={offset}", 
                headers=headers)
            line = responce.json()
            if len(rows) == 0:
                logging.info(f'--- There is no data in source {obj} to download, check source. ---')
            if len(line) == 0:
                logging.info(f'--- Total {len(rows)} were received from source {obj}. ---')
                break
            rows.extend(line)
            offset += 50

        insert_rows = [tuple(row.values()) for row in rows]

        if obj == 'deliveries':
            hook.insert_rows(
                'stg.' + obj,
                insert_rows, 
                cols, 
                replace=True, 
                replace_index=['order_id', 'delivery_id'])
        else:
            hook.insert_rows(
                'stg.' + obj, 
                insert_rows, 
                cols)
            
        logging.info(f'Dimension {obj} has been succesfully uploaded to STG.')
    pg_conn.close()


with DAG(
        'project_4',
        schedule_interval=None,
        default_args=default_args,
        description='project_4',
        catchup=False,
        start_date = datetime.today()
) as dag:

    # Loading data to staging

    load_to_stg = PythonOperator(
        task_id='load_to_stg',
        python_callable=stg_load)

    # Loading data to dds from staging

    load_dds_orders_ts = PostgresOperator(
        task_id='load_dds_orders_ts',
        postgres_conn_id=postgres_conn_id,
        sql="""
        INSERT INTO dds.order_ts
        (order_ts, order_year, order_month, order_day)
        SELECT 
            order_ts,
            date_part('year', order_ts)::integer,
            date_part('month', order_ts)::integer,
            date_part('day', order_ts)::integer
        FROM stg.deliveries
        ON CONFLICT (order_ts) DO NOTHING;
        """)

    load_dds_deliveries_ts = PostgresOperator(
        task_id='load_dds_deliveries_ts',
        postgres_conn_id=postgres_conn_id,
        sql="""
        INSERT INTO dds.delivery_ts
        (delivery_ts, delivery_year, delivery_month, delivery_day)
        SELECT 
            delivery_ts,
            date_part('year', delivery_ts)::integer,
            date_part('month', delivery_ts)::integer,
            date_part('day', delivery_ts)::integer
        FROM stg.deliveries
        ON CONFLICT (delivery_ts) DO NOTHING;
        """)

    load_dds_couriers = PostgresOperator(
        task_id='load_dds_couriers',
        postgres_conn_id=postgres_conn_id,
        sql="""
        INSERT INTO dds.couriers (courier_id, courier_name)
        SELECT courier_id, courier_name 
        FROM stg.couriers
        ON CONFLICT (courier_id) DO UPDATE 
        SET
        courier_name = excluded.courier_name;
        """)

    load_dds_deliveries = PostgresOperator(
        task_id='load_dds_deliveries',
        postgres_conn_id=postgres_conn_id,
        sql="""
        INSERT INTO dds.deliveries
        (delivery_id, courier_id, delivery_address, delivery_ts, rate, tip_sum, delivery_ts_id)
        SELECT d.delivery_id, c.id, d.address, d.delivery_ts, d.rate, d.tip_sum, dts.id 
        FROM stg.deliveries d 
        INNER JOIN dds.couriers c ON d.courier_id = c.courier_id
        INNER JOIN dds.delivery_ts dts ON d.delivery_ts = dts.delivery_ts 
        ON CONFLICT (delivery_id) DO UPDATE 
        SET
        delivery_ts = excluded.delivery_ts,
        rate = excluded.rate,
        tip_sum = excluded.tip_sum;
        """)

    load_dds_orders = PostgresOperator(
    task_id='load_dds_orders',
    postgres_conn_id=postgres_conn_id,
    sql="""
        INSERT INTO dds.orders 
        (order_id, order_ts, order_sum, order_ts_id)
        SELECT d.order_id, d.order_ts, d.order_sum, ots.id 
        FROM stg.deliveries d
        INNER JOIN dds.order_ts ots ON d.order_ts = ots.order_ts 
        ON CONFLICT (order_id) DO 
        UPDATE SET
        order_ts = excluded.order_ts,
        order_sum = excluded.order_sum;
        """)

    load_dds_fct_delivery = PostgresOperator(
    task_id='load_dds_fct_delivery',
    postgres_conn_id=postgres_conn_id,
    sql="""
        INSERT INTO dds.fct_delivery
        (order_id, order_ts_id, delivery_id, delivery_ts_id, courier_id, rate, order_sum, tip_sum)
        SELECT 
                o.id AS order_id, 
                ots.id AS order_ts_id, 
                d.id AS delivery_id, 
                dts.id AS delivery_ts_id, 
                d.courier_id, 
                d.rate, 
                o.order_sum, 
                d.tip_sum
        FROM stg.deliveries sd 
        INNER JOIN dds.deliveries d ON sd.delivery_id = d.delivery_id 
        INNER JOIN dds.orders o ON sd.order_id = o.order_id
        INNER JOIN dds.delivery_ts dts ON sd.delivery_ts = dts.delivery_ts 
        INNER JOIN dds.order_ts ots ON sd.order_ts = ots.order_ts 
        ON CONFLICT (order_id, delivery_id) DO 
        UPDATE SET
        order_ts_id = excluded.order_ts_id,
        delivery_ts_id = excluded.delivery_ts_id,
        courier_id = excluded.courier_id,
        rate = excluded.rate,
        order_sum = excluded.order_sum,
        tip_sum = excluded.tip_sum;
        """)
    
    # Loading data to cdm layer 
    
    load_cdm_dm_courier_ledger = PostgresOperator(
    task_id='load_cdm_dm_courier_ledger',
    postgres_conn_id=postgres_conn_id,
    sql="""
        INSERT INTO cdm.dm_courier_ledger (
            courier_id,
            courier_name,
            settlement_year,
            settlement_month,
            orders_count,
            orders_total_sum,
            rate_avg,
            order_processing_fee,
            courier_order_sum,
            courier_tips_sum,
            courier_reward_sum
            )
        SELECT *,
            (ag2.courier_order_sum + ag2.courier_tips_sum * 0.95) AS courier_reward_sum
        FROM (
            SELECT courier_id,
                courier_name,
                settlement_year,
                settlement_month,
                sum(orders_count) AS orders_count,
                sum(orders_total_sum) AS orders_total_sum,
                sum(rate_avg) AS rate_avg,
                (sum(ag.orders_total_sum) * 0.25)::NUMERIC(14, 2) AS order_processing_fee,
                sum(CASE 
                        WHEN ag.rate_avg < 4
                            THEN GREATEST(0.05 * ag.orders_total_sum, 100)::NUMERIC(14, 2)
                        WHEN ag.rate_avg >= 4 AND ag.rate_avg < 4.5
                            THEN GREATEST(0.07 * ag.orders_total_sum, 150)::NUMERIC(14, 2)
                        WHEN ag.rate_avg >= 4.5 AND ag.rate_avg < 4.9
                            THEN GREATEST(0.08 * ag.orders_total_sum, 175)::NUMERIC(14, 2)
                        WHEN ag.rate_avg >= 4.9
                            THEN GREATEST(0.10 * ag.orders_total_sum, 200)::NUMERIC(14, 2)
                        END) AS courier_order_sum,
                sum(courier_tips_sum) AS courier_tips_sum
            FROM (
                SELECT fd.courier_id,
                    c.courier_name,
                    ots.order_year AS settlement_year,
                    ots.order_month AS settlement_month,
                    count(fd.order_id)::NUMERIC(14, 2) AS orders_count,
                    sum(fd.order_sum)::NUMERIC(14, 2) AS orders_total_sum,
                    avg(fd.rate)::NUMERIC(3, 2) AS rate_avg,
                    sum(fd.tip_sum)::NUMERIC(14, 2) AS courier_tips_sum
                FROM dds.fct_delivery fd
                INNER JOIN dds.order_ts ots ON fd.order_ts_id = ots.id
                INNER JOIN dds.couriers c ON fd.courier_id = c.id
                GROUP BY 1, 2, 3, 4
                ) AS ag
            GROUP BY 1, 2, 3, 4
            ) AS ag2
        ON CONFLICT (courier_id, settlement_year, settlement_month) DO UPDATE
        SET 
            courier_name = excluded.courier_name,
            orders_count = excluded.orders_count,
            orders_total_sum = excluded.orders_total_sum,
            rate_avg = excluded.rate_avg,
            order_processing_fee = excluded.order_processing_fee,
            courier_order_sum = excluded.courier_order_sum,
            courier_tips_sum = excluded.courier_tips_sum,
            courier_reward_sum = excluded.courier_reward_sum;
        """)

    load_to_stg >> [load_dds_deliveries_ts, load_dds_couriers, load_dds_orders_ts]
    load_dds_deliveries_ts >> load_dds_deliveries # >> load_dds_fct_delivery
    load_dds_couriers >>  load_dds_deliveries # >> load_dds_fct_delivery
    load_dds_orders_ts >> load_dds_orders # >> load_dds_fct_delivery
    [load_dds_orders, load_dds_deliveries] >> load_dds_fct_delivery >> load_cdm_dm_courier_ledger





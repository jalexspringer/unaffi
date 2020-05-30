"""
Code that goes along with the Airflow located at:
http://airflow.readthedocs.org/en/latest/tutorial.html
"""
from airflow import DAG
from airflow.operators import PythonOperator, DummyOperator
from airflow.hooks.S3_hook import S3Hook

from datetime import datetime, timedelta
import toml, json, logging
from netimpact import awin, admitad, linkshare


c = toml.load('/usr/local/airflow/dags/config.toml')

def awin_get_transactions(accts, execution_date, **_):
    hook = S3Hook('s3')
    date_format = '%Y-%m-%d'
    start = (execution_date - timedelta(1)).strftime(date_format)
    end = (execution_date - timedelta(1)).strftime(date_format)
    for acct,_ in accts.items():
        a = awin.AWin(c['Awin']['oauth'])
        transactions_dict = a.transaction_request(acct, start, end, 'pending')
        logging.info(type(transactions_dict))
        hook.load_string(transactions_dict, f'pending/{acct}/{end}.json', 'cross-network-asos')

def admitad_get_transactions(accts, execution_date, **_):
    hook = S3Hook('s3')
    date_format = '%d.%m.%Y'
    start = (execution_date - timedelta(2)).strftime(date_format)
    end = (execution_date - timedelta(1)).strftime(date_format)
    acct_name = c['Admitad']['account_name']
    for acct,_ in accts.items():
        a = admitad.Admitad(c['Admitad']['client_id'], c['Admitad']['client_secret'])
        transactions_dict = a.transaction_request(acct_name, start, end, 'pending')
        logging.info(type(transactions_dict))
        hook.load_string(transactions_dict, f'pending/{acct}/{end}.json', 'cross-network-asos')


default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2020, 5, 29),
    "email": ["alexspringer@pm.me"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    # 'queue': 'bash_queue',
    # 'pool': 'backfill',
    # 'priority_weight': 10,
    # 'end_date': datetime(2016, 1, 1),
}

dag = DAG("network_update_admitad1",
            default_args=default_args,
            schedule_interval=timedelta(1))

# awin_pending_transactions = PythonOperator(
#     task_id="awin_pending",
#     python_callable=awin_get_transactions,
#     op_kwargs = {
#         'accts': c['Awin']['account_ids'],
#     },
#     provide_context=True,
#     dag=dag)

admitad_pending_transactions = PythonOperator(
    task_id="admitad_pending",
    python_callable=admitad_get_transactions,
    op_kwargs = {
        'accts': c['Admitad']['account_ids'],
    },
    provide_context=True,
    dag=dag)

op = DummyOperator(task_id='dummy', dag=dag)

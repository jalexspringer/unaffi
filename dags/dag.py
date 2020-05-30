"""
Code that goes along with the Airflow located at:
http://airflow.readthedocs.org/en/latest/tutorial.html
"""
from airflow import DAG
from airflow.operators import PythonOperator, DummyOperator, BashOperator
from airflow.hooks.S3_hook import S3Hook

from datetime import datetime, timedelta
import toml, json, logging
from netimpact import awin, admitad, linkshare


c = toml.load('/usr/local/airflow/dags/config.toml')


default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2020, 5, 25),
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

partner_dag = DAG("partner_update",
            default_args=default_args,
            schedule_interval=timedelta(1))

start_task = DummyOperator(task_id='start', dag=partner_dag)

awin_partners = BashOperator(
    task_id='awin_partner_update',
    bash_command='netimpact -c /usr/local/airflow/dags/config.toml -p awin',
    dag=partner_dag
)

admitad_partners = BashOperator(
    task_id='admitad_partner_update',
    bash_command='netimpact -c /usr/local/airflow/dags/config.toml -p admitad',
    dag=partner_dag
)

linkshare_partners = BashOperator(
    task_id='linkshare_partner_update',
    bash_command='netimpact -c /usr/local/airflow/dags/config.toml -p linkshare',
    dag=partner_dag
)

end_task = DummyOperator(task_id='end', dag=partner_dag)




transaction_dag = DAG("transaction_update",
            default_args=default_args,
            schedule_interval=timedelta(1))

start_transactions = DummyOperator(task_id='start_transactions', dag=transaction_dag)

awin_transactions = BashOperator(
    task_id='awin_transaction_update',
    bash_command='netimpact -c /usr/local/airflow/dags/config.toml -td {{ ds }} -s cross-network-asos --no_upload awin',
    dag=transaction_dag
)

admitad_transactions = BashOperator(
    task_id='admitad_transaction_update',
    bash_command='netimpact -c /usr/local/airflow/dags/config.toml -td {{ ds }} -s cross-network-asos --no_upload admitad',
    dag=transaction_dag
)

linkshare_transactions = BashOperator(
    task_id='linkshare_transaction_update',
    bash_command='netimpact -c /usr/local/airflow/dags/config.toml -td {{ ds }} -s cross-network-asos --no_upload linkshare',
    dag=transaction_dag
)

# TODO :: Modify action status in transactions based on modifications file
end_transactions = DummyOperator(task_id='end_transactions', dag=transaction_dag)



start_task >> [awin_partners, admitad_partners, linkshare_partners] >> end_task
start_transactions >> [awin_transactions,admitad_transactions,linkshare_transactions] >> end_transactions
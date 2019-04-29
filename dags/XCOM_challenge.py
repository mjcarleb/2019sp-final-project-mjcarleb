from airflow import DAG
from airflow.operators.sensors import S3KeySensor
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta

import pandas as pd

yday = datetime.combine(datetime.today() - timedelta(1),
                                  datetime.min.time())


def push_via_return():
    return 10

def pull_via_return(**context):
    value1 = context["task_instance"].xcom_pull(task_ids="t_push_via_return")
    print(f"Value pulled via XCOM pull by return (10?) = {value1}")
    return "Finished pull via return"


def push_via_key(**kwargs):
    kwargs["ti"].xcom_push(key="pushed_via_key", value=20)

def pull_via_key(**context):
    value1 = context["task_instance"].xcom_pull(task_ids="t_push_via_key",
                                                key="pushed_via_key")
    print(f"Value pulled via XCOM pull by key (20?) = {value1}")
    return "Finished pull via key"


default_args = {
    'owner': 'mjcarleb',
    'depends_on_past': False,
    'start_date': yday,
    'email_on_failure': False,
    'retries': 0
}

dag = DAG('demo_XCOM9',
          default_args=default_args,
          schedule_interval='@once')

t1 = PythonOperator(
    task_id = "t_push_via_return",
    provide_context=False,
    python_callable=push_via_return,
    dag=dag)


t2 = PythonOperator(
    task_id = "t_pull_via_return",
    provide_context=True,
    python_callable=pull_via_return,
    dag=dag)

t3 = PythonOperator(
    task_id = "t_push_via_key",
    provide_context=True,
    python_callable=push_via_key,
    dag=dag)


t4 = PythonOperator(
    task_id = "t_pull_via_key",
    provide_context=True,
    python_callable=pull_via_key,
    dag=dag)

t2.set_upstream([t1])
t4.set_upstream([t3])

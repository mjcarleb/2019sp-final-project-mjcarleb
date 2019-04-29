#########################################
#          Challenge:  do something with file
#########################################
#
#  Use file inside python (e.g., pandas)
#
#########################################

from airflow import DAG
from airflow.operators.sensors import S3KeySensor
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta

import pandas as pd

yday = datetime.combine(datetime.today() - timedelta(1),
                                  datetime.min.time())


def read_from_csv(ds, **kwargs):

    print("hello world")

    df = pd.read_csv("~/yelp1.csv")
    print(df.shape)
    return "Whatever you return gets printed in logs"


default_args = {
    'owner': 'mjcarleb',
    'depends_on_past': False,
    'start_date': yday,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=5)
}

dag = DAG('read_csv_into_dataframe',
          default_args=default_args,
          schedule_interval='@once')

t1 = S3KeySensor(
    task_id='detect',
    poke_interval=0,
    timeout=10,
    bucket_key='s3://cscie29-data/pset5/yelp_data/yelp_subset_1.csv',
    bucket_name=None,
    aws_conn_id = "aws_default",
    dag=dag)

t2 = BashOperator(
    task_id='change_dir',
    depends_on_past=False,
    bash_command="cd $AIRFLOW_HOME",
    dag=dag)

t3 = BashOperator(
    task_id='touch_as_proof',
    depends_on_past=False,
    bash_command="touch ~/iwashere.txt",
    dag=dag)

t4 = BashOperator(
    task_id='copy_s3_local',
    depends_on_past=False,
    bash_command="aws s3 cp s3://cscie29-data/pset5/yelp_data/yelp_subset_1.csv ~/yelp1.csv",
    dag=dag)


t5 = PythonOperator(
    task_id = "pandas_python_operator",
    provide_context=True,
    python_callable=read_from_csv,
    dag=dag
)

t2.set_upstream(t1)
t3.set_upstream(t2)
t4.set_upstream(t3)
t5.set_upstream(t4)

#########################################
# Old Tutorial Code
#########################################
'''
Code that goes along with the Airflow tutorial located at:
https://github.com/apache/airflow/blob/master/airflow/example_dags/tutorial.py

from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from datetime import datetime, timedelta


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2015, 6, 1),
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    # 'queue': 'bash_queue',
    # 'pool': 'backfill',
    # 'priority_weight': 10,
    # 'end_date': datetime(2016, 1, 1),
}

dag = DAG('tutorial', default_args=default_args, schedule_interval=timedelta(days=1))

# t1, t2 and t3 are examples of tasks created by instantiating operators
t1 = BashOperator(
    task_id='print_date',
    bash_command='date',
    dag=dag)

t2 = BashOperator(
    task_id='sleep',
    bash_command='sleep 5',
    retries=3,
    dag=dag)

templated_command = """
    {% for i in range(5) %}
        echo "{{ ds }}"
        echo "{{ macros.ds_add(ds, 7)}}"
        echo "{{ params.my_param }}"
    {% endfor %}
"""

t3 = BashOperator(
    task_id='templated',
    bash_command=templated_command,
    params={'my_param': 'Parameter I passed in'},
    dag=dag)

t2.set_upstream(t1)
t3.set_upstream(t1)
'''

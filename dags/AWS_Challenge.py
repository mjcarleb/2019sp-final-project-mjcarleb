#########################################
#          Challenge:  FileDependence
#########################################
#
#  Make a task depend on existence of file
#
#########################################

from airflow import DAG
from airflow.operators.sensors import S3KeySensor
from airflow.operators import BashOperator
from datetime import datetime, timedelta

yday = datetime.combine(datetime.today() - timedelta(1),
                                  datetime.min.time())

default_args = {
    'owner': 'mjcarleb',
    'depends_on_past': False,
    'start_date': yday,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

dag = DAG('s3_justkey_sensor', default_args=default_args, schedule_interval='@once')

t1 = S3KeySensor(
    task_id='s3_file_test',
    poke_interval=0,
    timeout=10,
    bucket_key='s3://cscie29-data/pset5/yelp_data/yelp_subset_*.csv',
    bucket_name=None,
    wildcard_match=True,
    aws_conn_id = "aws_default",
    dag=dag)

t2 = BashOperator(
    task_id='task2',
    depends_on_past=False,
    bash_command='echo a big hadoop job putting files on s3',
    trigger_rule='all_failed',
    dag=dag)

t3 = BashOperator(
    task_id='task3',
    depends_on_past=False,
    bash_command='echo im next job using s3 files',
    dag=dag)

t2.set_upstream(t1)
t3.set_upstream(t1)
t3.set_upstream(t2)

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

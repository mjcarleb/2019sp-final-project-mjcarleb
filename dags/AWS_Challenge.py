#########################################
#          Challenge:  FileDependence
#########################################
#
#  Make a task depend on existence of file
#
#########################################

from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.contrib.sensors.file_sensor import FileSensor
from datetime import datetime, timedelta

# Define common default arguments for all operators in this example DAG
default_args = {
    "owner": 'mjcarleb',
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "start_date": datetime(2019, 4, 28),
    "end_date": datetime(2019,4,29),
    "depends_on_past": False,
}


# Create example DAG to demonstrate salting of file names
dag = DAG('localfile6',
          default_args=default_args,
          schedule_interval="@once")


# Add first task to DAG we of this example
t1 = FileSensor(
    filepath="tmp.tmp",
    task_id= "depend_on_tmptmp",
    fs_conn_id="fs_default2",
    dag=dag)



# Add first task to DAG we of this example
t2 = BashOperator(
    task_id= "echo3",
    bash_command= "echo 3",
    dag=dag)


t2.set_upstream(t1)



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

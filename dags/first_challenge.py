#########################################
#        My First Challenge
#########################################
#  Create salted set of tasks
#  a) Create a task dependent on existence of 2 local (or AWS) file
#  b) Zip contents of the two files together
#  c) Create output file of combined data where name includes hash of input files
#  d) Create new file if combined does not exist
#  e) Create new file if tag in file name does not match salt of previous file
#########################################

from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from datetime import datetime, timedelta

# Define common default arguments for all operators in this example DAG
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


# Create example DAG to demonstrate salting of file names
dagFIRST = DAG('first',
               default_args=default_args,
               schedule_interval=timedelta(days=1)
               )


# Add first task to DAG we of this example
t1 = BashOperator(
    task_id= "echo1",
    bash_command= "echo 1",
    dag=dagFIRST)



# Add first task to DAG we of this example
t2 = BashOperator(
    task_id= "echo2",
    bash_command= "echo 2",
    dag=dagFIRST)


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

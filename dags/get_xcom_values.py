############################################################
############################################################
# Imports
############################################################
############################################################

from airflow import DAG
from airflow.operators.sensors import S3KeySensor
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.contrib.sensors.file_sensor import FileSensor
from datetime import datetime, timedelta

############################################################
############################################################
# Define Default Attributes for Operators and DAG
############################################################
############################################################

# Parameters passed automatically by DAG to operators
# See BaseOperator source code for full set of possibilities
default_args = {
    'owner': 'mjcarleb',
    'start_date': datetime.today(),
    'retries': 0
}

# Define DAG with minimum number of parameters
dag = DAG('bash_xcom_pull_POC',
          default_args=default_args,
          schedule_interval='@once')

############################################################
############################################################
# Create PyhthonOperator:  Read and Print Some Values
############################################################
############################################################

def push_and_return_xcom_values(**context):


    # Push flags to XCOM to provoke downstream tasks
    context["ti"].xcom_push(key="phoney_hash", value="bash.tmp")

    # Message to the log
    return str(f"I am not sure xcom on return!!!!")

t1 = PythonOperator(
    task_id = "push_and_return_xcom_values",
    python_callable=push_and_return_xcom_values,
    provide_context=True,
    dag=dag)

t2 = BashOperator(
    task_id = "pull_from_bash",
    bash_command = "touch $AIRFLOW_HOME/{{ti.xcom_pull(task_ids='push_and_return_xcom_values', key='phoney_hash')}}",
    dag=dag)

t1>>t2

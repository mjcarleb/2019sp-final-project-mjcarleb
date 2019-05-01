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
dag = DAG('Salted_Graph_Practice2',
          default_args=default_args,
          schedule_interval='@once')

############################################################
############################################################
# Create PyhthonOperator:  Read and Print Some Values
############################################################
############################################################

def read_some_xcom_values(**context):


    old_key_value = context["task_instance"].xcom_pull(task_ids="read_some_xcom_values",
                                                           key="value_of_17")

    # Push flags to XCOM to provoke downstream tasks
    context["ti"].xcom_push(key="value_of_17_orbadNone", value=old_key_value)

    # Message to the log
    return str(f"copy of old key value={old_key_value}")



t = PythonOperator(
    task_id = "read_some_xcom_values",
    python_callable=read_some_xcom_values,
    provide_context=True,
    dag=dag)


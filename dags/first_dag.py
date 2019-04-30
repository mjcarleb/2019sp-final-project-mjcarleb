from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.contrib.sensors.file_sensor import FileSensor
from datetime import datetime

# Define common default arguments for all operators in this example DAG
default_args = {
    "owner": 'mjcarleb',
    "retries": 0,
    "start_date": datetime(2019, 4, 28),
    "end_date": datetime(2019,4,29),
    "depends_on_past": False,
}

# Create example DAG to demonstrate "sensing" local file and renaming
dag = DAG('local_sensor1',
          default_args=default_args,
          schedule_interval="@once")

# Add first task to DAG to sense local file
t1 = FileSensor(
    task_id= "sense_local_file",
    filepath="local_file.txt",
    fs_conn_id="fs_default2",
    dag=dag)

# Add second task to DAG to rename the file
t2 = BashOperator(
    task_id= "rename_file",
    bash_command= "mv $AIRFLOW_HOME/local_file.txt $AIRFLOW_HOME/local_file.bak",
    dag=dag)

# Make the 2nd task depend on completion of the first
t2.set_upstream(t1)

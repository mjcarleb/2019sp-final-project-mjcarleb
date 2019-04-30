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
dag = DAG('Mixed_Failing_Demo',
          default_args=default_args,
          schedule_interval='@once')


############################################################
############################################################
# Create 5aFileSensor Task:  Sense_Copied_Data
############################################################
############################################################

# Sensor Task to verify existence of copied data from S3
t5a = FileSensor(
    task_id= "5a_Sense_Copied_Data",
    fs_conn_id="fs_default2",
    filepath="copied/yelp_subset_1.csv",
    poke_interval=10,
    timeout=20,
    dag=dag)

############################################################
############################################################
# Create 5bileSensor Task:  Sense_Transformed_Data
############################################################
############################################################

# Sensor Task to verify existence of transformed data from copied
t5b = FileSensor(
    task_id= "5b_Sense_Transformed_Data",
    fs_conn_id="fs_default2",
    filepath="transformed/yelp_subset_1.csv",
    poke_interval=10,
    timeout=20,
    dag=dag)


############################################################
############################################################
# Create 6aBashOperator that Runs if either above succeeds
############################################################
############################################################

t6a = BashOperator(
    task_id='6a_change_dir1',
    depends_on_past=False,
    bash_command="cd dfsjkl$AIRFLOW_HOME",
    trigger_rule="one_success",
    dag=dag)


############################################################
############################################################
# Create 6bBashOperator that Runs if 6a fails
############################################################
############################################################

t6b = BashOperator(
    task_id='6b_change_dir2',
    depends_on_past=False,
    bash_command="cd $AIRFLOW_HOME",
    trigger_rule="one_failed",
    dag=dag)

t5a>>t6a
t5b>>t6a
t6a>>t6b

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
import pandas as pd

############################################################
############################################################
# Define Global Attributes
############################################################
############################################################

############################################################
############################################################
# Define Default Attributes for Operators and DAG
############################################################
############################################################

# Fix this to make today
this_day = datetime.combine(datetime.today() - timedelta(1),
                             datetime.min.time())

# Parameters passed automatically by DAG to operators
# See BaseOperator source code for full set of possibilities
default_args = {
    'owner': 'mjcarleb',
    'depends_on_past': False,
    'start_date': this_day,
    'email_on_failure': False,
    'retries': 0
}

# Define DAG with minimum number of parameters
dag = DAG('Salted_Graph_Practice1',
          default_args=default_args,
          schedule_interval='@once')

############################################################
############################################################
# Create S3KeySensor Task:  Sense_S3_Source_Data
############################################################
############################################################

t1 = S3KeySensor(
    task_id='Sense_S3_Source_Data',
    poke_interval=10,
    timeout=30,
    bucket_key='s3://cscie29-data/pset5/yelp_data/yeld_subset_1.csv',
    bucket_name=None,
    aws_conn_id = "aws_default",
    dag=dag)

############################################################
############################################################
# Create FileSensor Task:  Sense_Copied_Data
############################################################
############################################################


############################################################
############################################################
# Create FileSensor Task:  Sense_Transformed_Data
############################################################
############################################################


############################################################
############################################################
# Create FileSensor Task:  Sense_Report
############################################################
############################################################


############################################################
############################################################
# Create PythonOperator Task:  Check_Copied_Hashes_Match
############################################################
# Read the copied data and calculate copied_hash_current
#
# Try to pull copied_hash_past from last run of Pass_or_Copy
#
# If hashes match, push Re_Do_Copy = Re_Do_Report = False
#
# Else (mismatch or can't pull copied_hash_past) ...
# ... push Re_Do_Copy = Re_Do_Report = True
############################################################



############################################################
############################################################
# Create BashOperator Task:  Pass_or_Copy
############################################################
# Pull Re_Do_Copy from last run of Check_Copied_Hashes_Match
#
# Iff Re_Do_Copy == True, run Bash AWS Copy command
############################################################


############################################################
############################################################
# Create PythonOperator Task:  Check_Transformed_Hashes_Match
############################################################
# Read the transformed data and calculate transformed_hash_current
#
# Try to pull transformed_hash_past from last run of Pass_or_Transform
#
# If hashes match, push Re_Do_Transform = Re_Do_Report = False
#
# Else (mismatch or can't pull transformed_hash_past) ...
# ... push Re_Do_Transform = Re_Do_Report = True
############################################################


############################################################
############################################################
# Create PythonOperator Task:  Pass_or_Transform
############################################################
# Pull Re_Do_Transform from last run of Check_Transform_Hashes_Match
#
# Iff Re_Do_Transform == True, run python transformation
############################################################




############################################################
############################################################
# Create PythonOperator Task:  Check_Report_Hashes_Match
############################################################
# Read the report and calculate report_hash_current
#
# Try to pull report_hash_past from last run of Pass_or_Generate_Report
#
# If mismatch or can't pull report_hash_past ...
# ... push Re_Do_Report = True
#
# Else, push Re_Do_Report = False
############################################################


############################################################
############################################################
# Create PythonOperator Task:  Pass_or_Generate_Report
############################################################
# Pull Re_Report from last run of Check_Copied_Hashes_Match
# Pull Re_Report from last run of Check_Transformed_Hashes_Match
# Pull Re_Report from last run of Check_Report_Hashes_Match
#
# Iff any of flags == True, run python report generator
############################################################



############################################################
############################################################
# Define Dependencies of Tasks in the DAG
############################################################
############################################################


"""
############# STARTER CODE BELOW ###########################
yday = datetime.combine(datetime.today() - timedelta(1),
                                  datetime.min.time())


def print_my_name(my_name):

    print(f"hello, {my_name}")

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

dag = DAG('pass_arg_print_name2',
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
    provide_context=False,
    python_callable=print_my_name,
    op_kwargs={"my_name": "MarkJoseph"},
    dag=dag)

t2.set_upstream(t1)
t3.set_upstream(t2)
t4.set_upstream(t3)
t5.set_upstream(t4)

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

"""

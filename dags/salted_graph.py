############################################################
############################################################
# Imports
############################################################
############################################################

import os
import hashlib

from airflow import DAG
from airflow.operators.sensors import S3KeySensor
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.contrib.sensors.file_sensor import FileSensor
from datetime import datetime, timedelta

############################################################
############################################################
# Define Global Variables
#
# In future implementation these could be passed in as
# arguments, picked up from traditional OS environmental
# variables or Airflow variables
############################################################
############################################################

# This is path to source code for this DAG
DAG_PY_FILE = os.path.join(os.environ["AIRFLOW_HOME"],
                           "dags",
                           os.path.basename(__file__))


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
dag = DAG('Salted_Graph_Practice1',
          default_args=default_args,
          schedule_interval=None)

############################################################
############################################################
# Create PythonOperator Task:
# 1.Generate_DAG_hash
############################################################
############################################################

def push_DAG_hash(**context):
    """Push hash of this DAG's source code to xcom

    Parameters
    context (dict): provided by calling PythonOperator

    Returns string written to log with status info
    """

    # Get salt from os environment as bytes
    salt = os.environ["CSCI_SALT"]
    salt_b = bytes.fromhex(salt)

    # Get this DAG's source as bytes
    with open(DAG_PY_FILE, "r") as dag_py_file:
        dag_source = dag_py_file.read()
    dag_source_b = dag_source.encode()

    # Hash the salt and source code
    hasher = hashlib.sha256()
    hasher.update(salt_b)
    hasher.update(dag_source_b)

    # Take last 8 bytes as "the hash" for source
    dag_hash = hasher.digest().hex()[:8]

    # Push hash to XCOM as keyed value
    context["ti"].xcom_push(key="DAG_hash", value=dag_hash)

    # Message to the log
    return str(f"Pushed DAG_hash to XCOM {dag_hash}")

# Operator to calculate and push this DAG's hash to xcom
t1 = PythonOperator(
    task_id = "1.Generate_DAG_hash",
    python_callable=push_DAG_hash,
    provide_context=True,
    dag=dag)

############################################################
############################################################
# Create FileSensor Task:
# 2.Sense_Report.hash
############################################################
############################################################

# Sensor Task to verify existence of report from transformed data
t2 = FileSensor(
    task_id="Sense_Report.hash",
    fs_conn_id="fs_default2",
    filepath="reports/yelp_subset_1.txt",
    poke_interval=10,
    timeout=20,
    dag=dag)

############################################################
############################################################
# Create FileSensor Task:
# 3.Sense_Transformed.hash
############################################################
############################################################

# Sensor Task to verify existence of transformed data from copied
t3 = FileSensor(
    task_id= "Sense_Transformed_Data",
    fs_conn_id="fs_default2",
    filepath="transformed/yelp_subset_1.csv",
    poke_interval=10,
    timeout=20,
    dag=dag)


############################################################
############################################################
# Create FileSensor Task:
# 4.Sense_Copied.hash
############################################################
############################################################

# Sensor Task to verify existence of copied data from S3
t4 = FileSensor(
    task_id= "Sense_Copied_Data",
    fs_conn_id="fs_default2",
    filepath="copied/yelp_subset_1.csv",
    poke_interval=10,
    timeout=20,
    dag=dag)


############################################################
############################################################
# Create S3KeySensor Task:
# 5.Sense_S3_Source
############################################################
############################################################

# Sensor task to verify existence of source data on S3
t5 = S3KeySensor(
    task_id='Sense_S3_Source_Data',
    aws_conn_id = "aws_default",
    bucket_key='s3://cscie29-data/pset5/yelp_data/yelp_subset_1.csv',
    bucket_name=None,
    poke_interval=10,
    timeout=20,
    dag=dag)

############################################################
############################################################
# Create BashOperator Task:
# 6.Generate_Copied.hash
############################################################
############################################################


############################################################
############################################################
# Create PythonOperator Task:
# 7.Generate_Transformed.hash
############################################################
############################################################



############################################################
############################################################
# Create PythonOperator Task:
# 8.Generate_Report.hash
############################################################
############################################################



############################################################
############################################################
# Define Dependencies of Tasks in the DAG
#
# t2 and t8 are terminal tasks, so DAG succeeds...
# ...iff t2 or t8 succeed
############################################################
############################################################

# t2 depends on t1 succeeding (trigger_rule = default, "all success")
t1 >> t2

# t2 depends on t3 failing (trigger_rule = "one failure")
t2 >> t3

# t4 depends on t3 failing (trigger_rule = "one failure")
t3 >> t4

# t5 depends on t4 failing (trigger_rule = "one failure")
t4 >> t5

# t6 depends on t5 succeeding (trigger_rule = default, "all success")
#t5 >> t6

# t7 depends on either t6 or t4 succeeding (trigger_rule = "one success")
#t6 >> t7
#t4 >> t7

# t8 depends on either t7 or t3 succeeding (trigger_rule = "one success")
#t7 >> t8
#t3 >> t8

"""

def check_copied_hashes_match(**context):

    import pandas as pd

    # Try to pull copied_hash_past from last run of Pass_or_Copy
    copied_hash_past = context["task_instance"].xcom_pull(task_ids="Pass_or_Copy",
                                                          key="copied_hash_past")

    #@@@@@@@@@@@ REMOVE
    copied_hash_past = 17 #### Just a temporary plug

    # If copied_hash_past does not exist, provoke downstream via XCOM
    if copied_hash_past is None:

        # Push flags to XCOM to provoke downstream tasks
        context["ti"].xcom_push(key="Re_Do_Copy", value=True)
        context["ti"].xcom_push(key="Re_Do_Transform", value=True)
        context["ti"].xcom_push(key="Re_Do_Report", value=True)

        # Message to the log
        return "copied_hash_past was None"

    else:

        # @@@@@@@@@@@ REMOVE
        # Calculate copied_hash_current
        copied_hash_current = 17 #### Just a temporary plug

        # @@@@@@@@@@@ CREATE DO PROGRAMMING
        # Read copied data into pd.dataframe
        #df = pd.read_csv("copied/yelp_subset_1.csv")
        #Create hash of df content


        # If past and current hashes of copied file do not match, provoke downstream via XCOM
        if copied_hash_past != copied_hash_current:
            context["ti"].xcom_push(key="Re_Do_Copy", value=True)
            context["ti"].xcom_push(key="Re_Do_Transform", value=True)
            context["ti"].xcom_push(key="Re_Do_Report", value=True)

            # Message to the log
            return "hashes did not match"


        # If past and current hashes of copied file match, do not provoke downstream via XCOM
        else:
            context["ti"].xcom_push(key="Re_Do_Copy", value=False)
            context["ti"].xcom_push(key="Re_Do_Transform", value=False)
            context["ti"].xcom_push(key="Re_Do_Report", value=False)

            # Message to the log
            return "hashes did match"


t5 = PythonOperator(
    task_id = "Check_Copied_Hashes_Match",
    python_callable=check_copied_hashes_match,
    provide_context=True,
    dag=dag)



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

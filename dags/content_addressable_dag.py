############################################################
############################################################
# Imports
############################################################
############################################################

import os
import hashlib
from pathlib import Path
import pandas as pd

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
# variables or Airflow variables and involve more complex
# file and path name processing--which I am NOT trying
# to simulate here.  Thus, I am largely stipulating values
# I use below without too much programming.
############################################################
############################################################

# This is path to source code for this DAG
dag_source_path = os.path.join(os.environ["AIRFLOW_HOME"],
                               "dags",
                               os.path.basename(__file__))

# These are paths under AIRFLOW_HOME to where data used by DAG is stored
copied_path = os.path.join("copied_data")
transformed_path = os.path.join("transformed_data")
reports_path = os.path.join("reports")

# Attributes of file we are processing...again, in real world
# we might process many files and include much more copmlex
# logic to parse and manipulate path and file names with
# utilities from os.path, etc.
source_data_path = "s3://cscie29-data/pset5/yelp_data/yelp_subset_1.csv"
source_data_stem = Path(source_data_path).stem
source_data_suffix = Path(source_data_path).suffix
copied_data_suffix = source_data_suffix
transformed_data_suffix = ".parquet"
report_suffix = ".txt"

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
    with open(dag_source_path, "r") as dag_py_file:
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

# Sensor Task to verify existence of report with proper dag_hash as suffix
# Airflow supports Jinja Templating, used to pull hash from xcom as shown below
# fs_conn_id is setup in Admin panel on Web UI and specifies starting path on local fs
t2 = FileSensor(
    task_id="2.Sense_Report.hash",
    fs_conn_id="fs_airflowhome",
    filepath=os.path.join(reports_path,
                          source_data_stem+"."+
                          "{{ti.xcom_pull(task_ids='1.Generate_DAG_hash', key='DAG_hash')}}") + report_suffix,
    poke_interval=5,
    timeout=5,
    dag=dag)

############################################################
############################################################
# Create FileSensor Task:
# 3.Sense_Transformed.hash
############################################################
############################################################

# Sensor Task to verify existence of transformed data with proper dag_hash as suffix
# Airflow supports Jinja Templating, used to pull hash from xcom as shown below
# fs_conn_id is setup in Admin panel on Web UI and specifies starting path on local fs
t3 = FileSensor(
    task_id="3.Sense_Transformed.hash",
    fs_conn_id="fs_airflowhome",
    filepath=os.path.join(transformed_path,
                          source_data_stem + "." +
                          "{{ti.xcom_pull(task_ids='1.Generate_DAG_hash', key='DAG_hash')}}") + transformed_data_suffix,
    poke_interval=5,
    timeout=5,
    trigger_rule="one_failed",
    dag=dag)

############################################################
############################################################
# Create FileSensor Task:
# 4.Sense_Copied.hash
############################################################
############################################################

# Sensor Task to verify existence of copied data with proper dag_hash as suffix
# Airflow supports Jinja Templating, used to pull hash from xcom as shown below
# fs_conn_id is setup in Admin panel on Web UI and specifies starting path on local fs
t4 = FileSensor(
    task_id="4.Sense_Copied.hash",
    fs_conn_id="fs_airflowhome",
    filepath=os.path.join(copied_path,
                          source_data_stem + "." +
                          "{{ti.xcom_pull(task_ids='1.Generate_DAG_hash', key='DAG_hash')}}") + source_data_suffix,
    poke_interval=5,
    timeout=5,
    trigger_rule="one_failed",
    dag=dag)

############################################################
############################################################
# Create S3KeySensor Task:
# 5.Sense_S3_Source
############################################################
############################################################

# Sensor task to verify existence of source data on S3
# aws_conn_id is setup in Admin panel on Web UI (I used default provided by Airflow)
# In this case, AWS CLI is getting credentials from ~/.aws, though these could be
# encrypted as variables in Airflow
t5 = S3KeySensor(
    task_id='5.Sense_S3_Source',
    aws_conn_id = "aws_default",
    bucket_key=source_data_path,
    bucket_name=None,
    poke_interval=5,
    timeout=5,
    trigger_rule="one_failed",
    dag=dag)

############################################################
############################################################
# Create BashOperator Task:
# 6.Generate_Copied.hash
############################################################
############################################################

# This BashOperator copies data from S3 to local drive
# using AWS CLI (which needs to be available in OS)
# In this case, AWS CLI is getting credentials from ~/.aws, though these could be
# encrypted as variables in Airflow
t6 = BashOperator(
    task_id='6.Generate_Copied.hash',
    bash_command="aws s3 cp " + source_data_path + " " +
                 os.path.join(os.environ['AIRFLOW_HOME'],
                              copied_path,
                              source_data_stem + "." +
                              "{{ti.xcom_pull(task_ids='1.Generate_DAG_hash', key='DAG_hash')}}")+source_data_suffix,
    dag=dag)

############################################################
############################################################
# Create PythonOperator Task:
# 7.Generate_Transformed.hash
############################################################
############################################################

def transform_data(**context):
    """Transform data from copied path & put in transformed path

    Parameters
    context (dict): provided by calling PythonOperator

    Returns string written to log with status info
    """

    # Get the dag_hash
    dag_hash = context["ti"].xcom_pull(task_ids='1.Generate_DAG_hash', key='DAG_hash')


    # Read in the csv data from the copied directory
    copied_file_path = os.path.join(os.environ["AIRFLOW_HOME"],
                                     copied_path,
                                     source_data_stem+"."+dag_hash+source_data_suffix)
    df = pd.read_csv(copied_file_path)

    # Do something to transform the data though in this demo I do not

    # Write the data out to the correct directory
    # with dag_hash as suffix
    transformed_file_path = os.path.join(os.environ["AIRFLOW_HOME"],
                                     transformed_path,
                                     source_data_stem+"."+dag_hash+transformed_data_suffix)
    df.to_parquet(transformed_file_path)

    # Message to the log
    return str(f"Transformed copied data with hash = {dag_hash}")

# This PythonOperator reads data from copied directory
# transforms data and writes it out to the correct
# transformed directory.  Though, in this demo I just convert
# to .parquet and do not do any transformations of the CSV data.
# Since the trigger_rule is "one_success" this task is triggered
# if t6 or t4 succeeds.
t7 = PythonOperator(
    task_id = "7.Generate_Transform.hash",
    python_callable=transform_data,
    provide_context=True,
    trigger_rule="one_success",
    dag=dag)

############################################################
############################################################
# Create PythonOperator Task:
# 8.Generate_Report.hash
############################################################
############################################################

def generate_report(**context):
    """Read transform data and create report

    Parameters
    context (dict): provided by calling PythonOperator

    Returns string written to log with status info
    """

    # Get the dag_hash
    dag_hash = context["ti"].xcom_pull(task_ids='1.Generate_DAG_hash', key='DAG_hash')


    # Read in the transformed data from the transformed directory
    transformed_file_path = os.path.join(os.environ["AIRFLOW_HOME"],
                                     transformed_path,
                                     source_data_stem+"."+dag_hash+transformed_data_suffix)
    df = pd.read_parquet(transformed_file_path)

    # Use the data to create report
    textlines = df.text.tolist()


    # Write the text to a report in report directory
    report_file_path = os.path.join(os.environ["AIRFLOW_HOME"],
                                     reports_path,
                                     source_data_stem+"."+dag_hash+report_suffix)
    with open(report_file_path, mode="w") as f:
        for line in textlines:
            f.write(line)

    # Message to the log
    return str(f"Generated report with hash = {dag_hash}")

# This PythonOperator reads data from copied directory
# transforms data and writes it out to the correct
# transformed directory.  Though, in this demo I just convert
# to .parquet and do not do any transformations of the CSV data.
# Since the trigger_rule is "one_success" this task is triggered
# if t6 or t4 succeeds.
t8 = PythonOperator(
    task_id = "8.Generate_Report.hash",
    python_callable=generate_report,
    provide_context=True,
    trigger_rule="one_success",
    dag=dag)

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
t5 >> t6

# t7 depends on either t6 or t4 succeeding (trigger_rule = "one success")
t6 >> t7
t4 >> t7

# t8 depends on either t7 or t3 succeeding (trigger_rule = "one success")
t7 >> t8
t3 >> t8

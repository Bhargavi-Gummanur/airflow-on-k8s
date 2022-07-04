
# -*- coding: utf-8 -*-
#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

"""
### Tutorial Documentation
Documentation that goes along with the Airflow tutorial located
[here](https://airflow.incubator.apache.org/tutorial.html)
"""
from datetime import timedelta
from datetime import datetime
import pandas as pd
import json
import airflow
from airflow import DAG
import logging
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.models import DagBag
#from airflow.models import Variable
import airflow.models.taskinstance as task




def extract_metadata(**context):
    #store = ti.xcom_pull(key = 'store')
    #filepath = Variable.get("datapath")
    #folder_path = "data/adult_data.csv"
    
    dag_id = context['task_instance'].dag_id
    #print("run id")
    run_id = context['dag_run'].run_id
    #task_id
    task_id = context['task_instance'].task_id
    context['task_instance'].xcom_push(key='file', value=(dag_id,task_id,run_id)) 
    #print(os.environ["AIRFLOW_VAR_PATH"])
    return (dag_id,task_id,run_id)
  

def print_stats_schema(dag_id,task_id,run_id):
    #dag_id = ti.xcom_pull(key="file", task_ids='Tasks1')
    print("dag_id",dag_id)
    print(type(json.loads(dag_id)))
    print(dag_id[0])
    print("task_id",task_id)
    print("run_id",run_id)
    


    

   




# These args will get passed on to each operator
# You can override them on a per-task basis during operator initialization
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': airflow.utils.dates.days_ago(2),
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'provide_context': True
   
    # 'queue': 'bash_queue',
    # 'pool': 'backfill',
    # 'priority_weight': 10,
    # 'end_date': datetime(2016, 1, 1),
    # 'wait_for_downstream': False,
    # 'dag': dag,
    # 'adhoc':False,
    # 'sla': timedelta(hours=2),
    # 'execution_timeout': timedelta(seconds=300),
    # 'on_failure_callback': some_function,
    # 'on_success_callback': some_other_function,
    # 'on_retry_callback': another_function,
    # 'trigger_rule': u'all_success'
}

with DAG(
    'analyse_csv_datav1',
    default_args=default_args,
    description='titanic dataset',
    #schedule_interval="@daily",
    schedule_interval=timedelta(seconds=10),
    max_active_runs=3
) as dag:

# t1, t2 and t3 are examples of tasks created by instantiating operators
    t1 = PythonOperator(
        task_id='Task1',
        python_callable=extract_metadata,
        provide_context=True
        #executor_config={"KubernetesExecutor": {"image": "docker.io/glmlopsuser/airflow-metadata:0.1"}}
        #executor_config={"KubernetesExecutor": {"image": "python:3.8"}}
    )

    t2 = PythonOperator(
        task_id='print_metadata',
        python_callable = print_stats_schema,
        #executor_config={"KubernetesExecutor": {"image": "docker.io/glmlopsuser/airflow-metadata:0.1"}}
        #executor_config={"KubernetesExecutor": {"image": "python:3.8"}}
        op_kwargs={'dag_id': "{{ task_instance.xcom_pull(task_ids='Task1',key='file') }}", 'task_id': "{{ task_instance.xcom_pull(task_ids='Task1',key='file') }}",'run_id':"{{ task_instance.xcom_pull(task_ids='Task1',key='file') }}"}
    )

'''
dag_id = extract_metadata()[0]
task_id = extract_metadata()[1]
run_id = extract_metadata()[2]
print(dag_id,task_id,run_id)
'''
    

#t1
t1 >> t2


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
import tensorflow_data_validation as tfdv
import airflow
from airflow import DAG
import logging
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.models import DagBag
#from airflow.models import Variable
import airflow.models.taskinstance as task
from tensorflow_data_validation.utils.schema_util import write_schema_text
from google.protobuf import json_format
import json
from ml_metadata import metadata_store
from ml_metadata.proto import metadata_store_pb2
from os.path import exists
import pickle
from cmflib import cmfquery
from cmflib import cmf
import sys
import os
import pickle
from metadatalib.datastats.record_stats_schema import log_stats_schema
import metadatalib.datapackager.package as datapack




def extract_metadata(**context):
    #store = ti.xcom_pull(key = 'store')
    #filepath = Variable.get("datapath")
    folder_path = "data/adult_data.csv"
    
    dag_id = context['task_instance'].dag_id
    #print("run id")
    run_id = context['task_instance'].run_id
    #task_id
    task_id = context['task_instance'].task_id
    #print(os.environ["AIRFLOW_VAR_PATH"])
    file_name = "/usr/local/src/mlops_dvc_ver1/mlmd"
    os.environ['FILE_PATH'] = file_name
    dataset_name = "titanic dataset"
    params = {
        "folder_path":folder_path,
        "file_name":file_name,
        "dag_id":dag_id,
        "run_id":run_id,
        "task_id":task_id,
        "dataset_name":dataset_name
    }
    
    
    logging.info("schema saved")
   
    print("here")
    log_stats_schema(params)

    context['task_instance'].xcom_push(key='pushing params',value = params)
    
def print_stats_schema(**kwargs):
    ti = kwargs['task_instance']
    params = ti.xcom_pull(task_ids = 'extraction_of_metadata',key = 'pushing params')
    #print(params)
    file_name = params["file_name"]
    os.environ['FILE_PATH'] = file_name
    dag_id = params["dag_id"]
    task_id = params["task_id"]
    query = cmfquery.CmfQuery(file_name)
    stages = query.get_pipeline_stages(dag_id)
    #file_name = Variable.get("path")

    executions = query.get_all_executions_in_stage(task_id)
    print(params['run_id'])
    print(executions.columns)

    artifacts = query.get_all_artifacts_for_execution(executions['id'][1])
    print(artifacts.columns)
    #print(executions)
    #print(artifacts1['quality'][0])
    #print(artifacts['quality'][0])

    json_temp_stat = json.loads(artifacts['statistics'][0])
    json_temp_schema = json.loads(artifacts['schema'][0])

    print("stats")
    print(type(json_temp_stat))
    print(json_temp_stat)

    print("schema")
    print(type(json_temp_schema))
    print(json_temp_schema)
    '''
    cmflib = datapack.CmfLib(source=file_name)
    cmflib.data_package_format()

    # Serializing json 
    datapackage_json = json.dumps(cmflib.datapackage.printjson(), sort_keys=True)
    with open('datapackage_json.json','w') as f:
        json.dump(json.loads(datapackage_json),f)
    print(datapackage_json)
    logging.debug(datapackage_json)
    '''

    

   




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
    'analyse_csv_data',
    default_args=default_args,
    description='titanic dataset',
    #schedule_interval="@daily",
    schedule_interval=timedelta(seconds=10),
    max_active_runs=3
) as dag:

# t1, t2 and t3 are examples of tasks created by instantiating operators
    t1 = PythonOperator(
        task_id='extraction_of_metadata',
        python_callable=extract_metadata,
        executor_config={"KubernetesExecutor": {"image": "docker.io/glmlopsuser/airflow-metadata"}}
    )


    
    t2 = PythonOperator(
        task_id='print_metadata',
        python_callable = print_stats_schema,
        executor_config={"KubernetesExecutor": {"image": "docker.io/glmlopsuser/airflow-metadata"}}
    )
    

#t1
t1 >> t2

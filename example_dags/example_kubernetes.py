import logging

from airflow import DAG
from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import KubernetesPodOperator
from airflow.utils.dates import days_ago
from datetime import datetime, timedelta
log = logging.getLogger(__name__)
from kubernetes.client import models as k8s

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2000,1,1)
}

with DAG(
    dag_id='example_kubernetes_operatortest',
    default_args=default_args,
    schedule_interval=timedelta(minutes=10),
    max_active_runs=1,
    tags=['example'],
) as dag:

    tolerations = [
        {
            'key': "key",
            'operator': 'Equal',
            'value': 'value'
        }
    ]
    resource_config = {'limit_memory': '1024Mi', 'limit_cpu': '500m'}
    k = KubernetesPodOperator(
        namespace='rakeshl-test',
        image="glmlopsuser/my-airflow-python:0.2",
        image_pull_secrets=[k8s.V1LocalObjectReference('airflow-secretv2')],
        cmds=["python"],
        arguments=['task2.py','print("helloarguements")'],
        resources=resource_config,
        name="airflow-test-pod",
        task_id="task",
        
    )
import logging

from airflow import DAG
from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import KubernetesPodOperator
from airflow.utils.dates import days_ago
from datetime import datetime, timedelta
log = logging.getLogger(__name__)


default_args = {
    'owner': 'airflow',
    'start_date': days_ago(2)
}

with DAG(
    dag_id='example_kubernetes_operator',
    default_args=default_args,
    schedule_interval=timedelta(minutes=10),
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
        image_pull_secrets=[k8s.V1LocalObjectReference('airflow-metadata2')],
        cmds=["bash", "-cx"],
        arguments=["echo hello here"],
        resources=resource_config,
        name="airflow-test-pod",
        task_id="task",
        
    )
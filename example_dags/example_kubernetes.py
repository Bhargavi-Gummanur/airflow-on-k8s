import logging

from airflow import DAG
from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import KubernetesPodOperator
from airflow.operators.bash_operator import BashOperator
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
    return_value = {"hi":"1"}
    k = KubernetesPodOperator(
        namespace='rakeshl-test',
        image="glmlopsuser/my-airflow-python:0.2",
        image_pull_secrets=[k8s.V1LocalObjectReference('airflow-secretv2')],
        cmds=["sh", "-c", "mkdir -p /airflow/xcom/;echo '[1,2,3,4]' > /airflow/xcom/return.json"],
        #arguments=['echo \'{}\' > /airflow/xcom/return.json'.format(return_value)],
        resources=resource_config,
        name="airflow-test-pod",
        task_id="task",
        do_xcom_push=True
        
    )
    pod_task_xcom_result = BashOperator(
        bash_command="echo \"{{ task_instance.xcom_pull('write-xcom')[0] }}\"",
        task_id="pod_task_xcom_result",
    )
    k >> pod_task_xcom_result
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
    schedule_interval=timedelta(minutes=1),
    tags=['example'],
) as dag:

    tolerations = [
        {
            'key': "key",
            'operator': 'Equal',
            'value': 'value'
        }
    ]

    k = KubernetesPodOperator(
        namespace='default',
        image="ubuntu:16.04",
        cmds=["bash", "-cx"],
        arguments=["echo hello here"],
     
        name="airflow-test-pod",
        task_id="task",
        
    )
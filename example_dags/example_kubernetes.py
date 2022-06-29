import logging

from airflow import DAG
from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import KubernetesPodOperator
from airflow.utils.dates import days_ago
from datetime import datetime, timedelta
log = logging.getLogger(__name__)


default_args = {
    'owner': 'airflow',
    'start_date': datetime(1970,1,1)
}

with DAG(
    dag_id='example_kubernetes_operator',
    default_args=default_args,
    schedule_interval=None,
    tags=['example'],
) as dag:

    tolerations = [
        {
            'key': "key",
            'operator': 'Equal',
            'value': 'value'
        }
    ]

    '''
        image="ubuntu:16.04",
        cmds=["bash", "-cx"],
        arguments=["echo hello here"],
        labels={"foo": "bar"},
        name="airflow-test-pod",
        node_selectors={"kubernetes.io/hostname": "gl1-cp-tr-node1.gl-hpe.local"},
    '''
    #namespace="airflowop-system",
    #pod_template_file="/usr/local/airflow/dags/gitdags/example_dags/example-python.yaml",
    k = KubernetesPodOperator(
        labels={"foo": "bar"},
        cmds=["bash", "-cx"],
        arguments=["echo hello here"],
        task_id="task",
        pod_template_file="pod_test.yaml",
        get_logs=True,
        is_delete_operator_pod=False
       
        #tolerations=tolerations
    )
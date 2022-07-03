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

# volume = k8s.V1Volume(
#     name='workspace-3-volume',
#     # persistent_volume_claim=k8s.V1PersistentVolumeClaimVolumeSource(claim_name='workspace-volume-3-claim'),
#     host_path=k8s.V1HostPathVolumeSource(path='/workspace'),
# )

volume_mounts = [
    k8s.V1VolumeMount(
        mount_path='/workspace', name='workspace-3-volume', sub_path=None,
        read_only=False
    )
]
with DAG(
    dag_id='example_kubernetes_operatortest',
    default_args=default_args,
    schedule_interval=timedelta(minutes=10),
    max_active_runs=1,
    concurrency = 1,

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
        image="glmlopsuser/my-airflow-metadata:0.4",
        image_pull_secrets=[k8s.V1LocalObjectReference('airflow-secretv2')],
        volume_mounts=volume_mounts,
        cmds=["python"],
        arguments=['task1.py','print("helloagain!")'],
        resources=resource_config,
        name="airflow-test-pod",
        task_id="task",
        
    )
    k1 = KubernetesPodOperator(
        namespace='rakeshl-test',
        image="glmlopsuser/my-airflow-metadata:0.4",
        image_pull_secrets=[k8s.V1LocalObjectReference('airflow-secretv2')],
        volume_mounts=volume_mounts,
        cmds=["python"],
        arguments=['task2.py','print("helloarg!!!!!")'],
        resources=resource_config,
        name="airflow-test-pod2",
        task_id="task2",
        
    )
    k >> k1
import logging

from airflow import DAG
from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import KubernetesPodOperator
from airflow.utils.dates import days_ago
from datetime import datetime, timedelta
from airflow.operators.python_operator import PythonOperator
log = logging.getLogger(__name__)
from kubernetes.client import models as k8s
import airflow.kubernetes.volume
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2000,1,1)
}
path_task1 = "/sharedvol/fsmount/repo/data/mlops_dvc/task1.py"
path_task2 = "/sharedvol/fsmount/repo/data/mlops_dvc/task2.py"
'''
volume = k8s.V1Volume(
    name='workspace-3-volume',
    #persistent_volume_claim=k8s.V1PersistentVolumeClaimVolumeSource(claim_name='workspace-volume-3-claim'),
    host_path=k8s.V1HostPathVolumeSource(path='/tmp'),
)

volume_mounts = [
    k8s.V1VolumeMount(
        mount_path='/sharedvol', name='workspace-3-volume', sub_path=None,
        read_only=False
    )
]
'''
volume_config= {
    'persistentVolumeClaim':
      {
        'claimName': 'pvc-airflow'
      }
    }
volume = airflow.kubernetes.volume.Volume(name='mapr-pv-airflow-1', configs=volume_config)

volume_mounts = [
    k8s.V1VolumeMount(
        mount_path='/sharedvol', name='mapr-pv-airflow-1', sub_path=None,
        read_only=False
    )
]

def extract_metadata(**context):
    #store = ti.xcom_pull(key = 'store')
    #filepath = Variable.get("datapath")
    #folder_path = "data/adult_data.csv"
    
    #dag_id = context['task_instance'].dag_id
    #print("run id")
    run_id = context['dag_run'].run_id
    #task_id
    #task_id = context['task_instance'].task_id
    #params = {"dag_id":dag_id,"task_id":task_id,"run_id":run_id}
    context['task_instance'].xcom_push(key='file', value=run_id) 
    #print(os.environ["AIRFLOW_VAR_PATH"])
    return run_id
with DAG(
    dag_id='example_kubernetes_operatortest',
    default_args=default_args,
    schedule_interval=timedelta(seconds=10),
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

    t1 = PythonOperator(
        task_id='extract_run_id',
        python_callable=extract_metadata,
        provide_context=True
        #executor_config={"KubernetesExecutor": {"image": "docker.io/glmlopsuser/airflow-metadata:0.1"}}
        #executor_config={"KubernetesExecutor": {"image": "python:3.8"}}
    )
    k = KubernetesPodOperator(
        namespace='sureshtest-dontdelete',
        image="glmlopsuser/my-airflow-tfdv:0.4",
        image_pull_secrets=[k8s.V1LocalObjectReference('airflow-secretv3')],
        volumes=[volume],
        volume_mounts=volume_mounts,
        cmds=["python"],
        arguments=[path_task1,"example_kubernetes_operatortest","extract_metadata_stats_schema","{{ task_instance.xcom_pull(task_ids='extract_run_id',key='file') }}"],
        resources=resource_config,
        name="airflow-test-pod",
        task_id="extract_metadata_stats_schema",
        
    )
    k1 = KubernetesPodOperator(
        namespace='sureshtest-dontdelete',
        image="glmlopsuser/my-airflow-tfdv:0.4",
        image_pull_secrets=[k8s.V1LocalObjectReference('airflow-secretv3')],
        volumes=[volume],
        volume_mounts=volume_mounts,
        cmds=["python"],
        arguments=[path_task2,"example_kubernetes_operatortest","extract_metadata_stats_schema","{{ task_instance.xcom_pull(task_ids='extract_run_id',key='file') }}"],
        resources=resource_config,
        name="airflow-test-pod2",
        task_id="print_stats_schema",
        
    )
    t1 >> k >> k1
"""
This is an example dag for using the KubernetesPodOperator.
"""
import logging
from airflow.configuration import conf
from airflow import DAG
from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import \
    KubernetesPodOperator
from airflow.operators.dummy_operator import DummyOperator
from datetime import datetime, timedelta
from airflow.kubernetes import pod
from airflow.kubernetes.volume import Volume 
#from airflow.kubernetes.volume import Volume
from kubernetes.client import models as k8s
from kubernetes.client.models import V1Volume
import airflow.kubernetes.volume

#from airflow.contrib.kubernetes import pod
import json

pod_resources = pod.Resources()
pod_resources.request_cpu = '1000m'
pod_resources.request_memory = '2048Mi'
pod_resources.limit_cpu = '2000m'
pod_resources.limit_memory = '4096Mi'
# log = logging.getLogger(__name__)

namespace = conf.get("kubernetes", "NAMESPACE")
print("namespace",namespace)

compute_resources = k8s.V1ResourceRequirements(
    limits={"cpu": "800m", "memory": "3Gi"},
    requests={"cpu": "800m", "memory": "3Gi"}
)
resource_config = {'limit_memory': '1024Mi', 'limit_cpu': '500m'}
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime.utcnow(),
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}
volume_config= {
    'persistentVolumeClaim':
      {
        'claimName': 'pvc-airflow'
      }
    }
volume = airflow.kubernetes.volume.Volume(name='mapr-pv-airflow-1', configs=volume_config)
# volume = k8s.V1Volume(
#     name='workspace-3-volume',
#     persistent_volume_claim=k8s.V1PersistentVolumeClaimVolumeSource(claim_name='twitter-stream-pvc'),
#     #host_path=k8s.V1HostPathVolumeSource(path='/tmp'),
# )

volume_mounts = [
    k8s.V1VolumeMount(
        mount_path='/sharedvol', name='mapr-pv-airflow-1', sub_path=None,
        read_only=False
    )
]

dag = DAG(
    'kubernetes_sample_v3', default_args=default_args,
    schedule_interval=timedelta(seconds=10),max_active_runs=1,concurrency = 1, tags=['example'])

start = DummyOperator(task_id='run_this_first', dag=dag)
'''
def sample_fun():
    #value = {"hi":"hello"}
    #print(value)
    f = open("/bd-fs-mnt/TenantShare/repo/data/wordcount.txt", "r")
    print(f.read())
    return "print('value')"
    #with open("sample.json","w") as f:
    #    json.dump(value,f)

'''
path = "/sharedvol/fsmount/repo/data/mlops_dvc/data/train.csv"
python_task = KubernetesPodOperator(namespace='sureshtest-dontdelete',
                                    image="glmlopsuser/sample-path-check:0.4",
                                    image_pull_secrets=[k8s.V1LocalObjectReference('airflow-secretv3')],
                                    cmds=["python"],
                                    arguments=["test.py",path],
                                    resources=resource_config,
                                    #labels={"foo": "bar"},
                                    name="passing-python",
                                    task_id="passing-task-python",
                                    volumes=[volume],
                                    volume_mounts=volume_mounts,
                                    #resources=pod_resources,
                                    #pod_template_file="/opt/airflow/dags/example-python.yaml",
                                   
                                    #do_xcom_push = True,
                                    #is_delete_operator_pod=False,
                                    
                                    dag=dag
                                    )
'''
bash_task = KubernetesPodOperator(namespace='default',
                                  image="ubuntu:16.04",
                                  cmds=["bash", "-cx"],
                                  arguments=["date"],
                                  labels={"foo": "bar"},
                                  name="passing-bash",
                                  # is_delete_operator_pod=False,
                                  task_id="passing-task-bash",
                                  #get_logs=True,
                                  dag=dag
                                  )
'''
python_task.set_upstream(start)
#bash_task.set_upstream(start)
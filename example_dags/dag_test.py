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
from kubernetes.client import models as k8s
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

dag = DAG(
    'kubernetes_sample_v2', default_args=default_args,
    schedule_interval=timedelta(minutes=10), tags=['example'])

start = DummyOperator(task_id='run_this_first', dag=dag)
def sample_fun():
    value = {"hi":"hello"}
    print(value)
    #with open("sample.json","w") as f:
    #    json.dump(value,f)


python_task = KubernetesPodOperator(namespace='default',
                                    image="python:3.6",
                                    #cmds=["python", "-c"],
                                    arguments=[sample_fun()],
                                    #labels={"foo": "bar"},
                                    name="passing-python",
                                    task_id="passing-task-python",
                                    #resources=pod_resources,
                                    #pod_template_file="/opt/airflow/dags/example-python.yaml",
                                   
                                    #do_xcom_push = True,
                                    #is_delete_operator_pod=False,
                                    
                                    dag=dag
                                    )

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

python_task.set_upstream(start)
bash_task.set_upstream(start)
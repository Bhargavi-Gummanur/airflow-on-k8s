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

# log = logging.getLogger(__name__)

namespace = conf.get("kubernetes", "NAMESPACE")
print("namespace",namespace)

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

python_task = KubernetesPodOperator(namespace=namespace,
                                    image="python:3.6",
                                    cmds=["python", "-c"],
                                    arguments=["print('hello world')"],
                                    labels={"foo": "bar"},
                                    name="passing-python",
                                    task_id="passing-task-python",
                                    get_logs=True,
                                    resources=pod.Resources(request_cpu=0.2,limit_cpu=0.5,request_memory='512Mi',limit_memory='1536Mi'),
                                    do_xcom_push = True,
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
                                  get_logs=True,
                                  dag=dag
                                  )

python_task.set_upstream(start)
bash_task.set_upstream(start)
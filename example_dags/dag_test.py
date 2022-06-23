import logging

from airflow import DAG
from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import \
    KubernetesPodOperator
from airflow.operators.dummy_operator import DummyOperator
from datetime import datetime, timedelta

# log = logging.getLogger(__name__)

def return_hello_world(s):
    print(s)
    params = {
        "1":"one",
        "2":"two"
    }
    return params

def print_another(params):
    print(params)
    
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
    'kubernetes_sample_testv2', default_args=default_args,
    schedule_interval=timedelta(minutes=10), tags=['example'])

start = DummyOperator(task_id='run_this_first', dag=dag)

python_task = KubernetesPodOperator(namespace='default',
                                    image="python:3.6",
                                    #cmds=["python", "-c"],
                                    arguments=['echo \'{}\' > /airflow/xcom/return.json'.format(return_hello_world("hi"))],
                                    labels={"foo": "bar"},
                                    name="passing-python",
                                    task_id="passing-task-python",
                                    get_logs=True,
                                    do_xcom_push = True,
                                    dag=dag
                                    )


bash_task = KubernetesPodOperator(namespace='default',
                                    image="python:3.6",
                                    cmds=["python", "-c"],
                                    arguments=['{{task_instance.xcom_pull('passing-task-python')}}'],
                                    labels={"foo": "bar"},
                                    name="passing-python1",
                                    task_id="passing-task-python1",
                                    get_logs=True,
                                    dag=dag
                                  )

#python_task.set_upstream(start)
#bash_task.set_upstream(start)
start >> python_task >> bash_task
import logging
import json
from airflow import DAG
import airflow.models.taskinstance as task
from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import \
    KubernetesPodOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.bash_operator import BashOperator
from datetime import datetime, timedelta

# log = logging.getLogger(__name__)

def return_hello_world(**context):
    
    params = {
        "1":"one",
        "2":"two"
    }
    save_path = "/airflow/xcom"
    filename = "return.json"
    comp = os.path.join(save_path,filename)
    #context['task_instance'].xcom_push(key='pushing params',value = params)
    with open(comp, 'w+') as json_file:
        json.dump(params,json_file)
    return "print('hello world')"
def pull_xcom(**kwargs):
    ti = kwargs['task_instance']
    params = ti.xcom_pull(task_ids = 'passing-task-python')
    print(params)

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
    'retry_delay': timedelta(minutes=5),
    'provide_context' : True
}

dag = DAG(
    'kubernetes_sample_testv2', default_args=default_args,
    schedule_interval=timedelta(minutes=10), tags=['example'])

start = DummyOperator(task_id='run_this_first', dag=dag)

write_xcom = KubernetesPodOperator(
        namespace='default',
        image='alpine',
        cmds = ["python","-c"],
        arguments = [return_hello_world()],
        #cmds=["sh", "-c", "mkdir -p /airflow/xcom/;echo '[1,2,3,4]' > /airflow/xcom/return.json"],
        name="write-xcom",
        do_xcom_push=True,
        is_delete_operator_pod=True,
        in_cluster=True,
        task_id="write-xcom",
        get_logs=True,
        dag=dag,
)

pod_task_xcom_result = BashOperator(
        bash_command="echo \"{{ task_instance.xcom_pull('write-xcom')[0] }}\"",
        task_id="pod_task_xcom_result",
        dag=dag,
)

write_xcom >> pod_task_xcom_result

#python_task.set_upstream(start)
#bash_task.set_upstream(start)
#start >> python_task >> bash_task
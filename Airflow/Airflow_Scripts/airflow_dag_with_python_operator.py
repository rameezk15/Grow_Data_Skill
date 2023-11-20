from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago

def hello_world_py(*args, **kwargs):
    print('Hello World from PythonOperator')

default_args = {
    'start_date': days_ago(1),
}

dag = DAG(
    'dummy_dag',
    default_args=default_args,
    description='A dummy DAG',
    schedule_interval=None,
)

t1 = BashOperator(
    task_id='bash_hello',
    bash_command='echo "Hello World from BashOperator"',
    dag=dag,
)

t2 = PythonOperator(
    task_id='python_hello',
    python_callable=hello_world_py,
    dag=dag,
)

t1 >> t2  # Specifies that t2 should follow t1
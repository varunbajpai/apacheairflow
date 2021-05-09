from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.bash import BashOperator
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.sensors.http_sensor import HttpSensor
from datetime import timedelta

dag = DAG(dag_id = 'helloworld_dag',schedule_interval='@daily',start_date=days_ago(1))


#Custom operator
class MyOperator(BaseOperator):
    @apply_defaults
    def __init__(self,name,*args,**kwargs):
        super().__init__(*args,**kwargs)
        self.name = name

    def execute(self,context):
        message = 'Hello {}'.format(self.name)
        print(message)
        return message


task1 =  BashOperator(task_id = 't1',bash_command = 'echo hello && exit 1',dag=dag)  #This exit 1 will make the task fail since it returns False

task2 =  BashOperator(task_id = 't2',bash_command = 'echo t2',dag=dag)

task3 =  BashOperator(task_id = 't3',bash_command = 'echo t3',dag=dag,trigger_rule='all_failed') 

task4 = MyOperator(name='abc',task_id='t4',dag=dag,trigger_rule='one_success')

'''
Use trigger rule for if else type conditions
Example  : If task1 is success then only execute task2 and task3
For all such if else type of stuff look at trigger rules

'''

sensor = HttpSensor(
    task_id = 'sensor',
    endpoint = '/',
    http_conn_id = 'my_httpcon',
    dag = dag,
    retries=20,
    retry_delay=timedelta(seconds=100)
)


sensor >> task1 >> [task2,task3] >> task4
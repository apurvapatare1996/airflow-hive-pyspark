from airflow import DAG
from datetime import datetime
#from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator

default_args = {
    'start_date': datetime(2019, 4, 19),
    'owner': 'Airflow'
}

dag = DAG('apurva_dag2',default_args=default_args)

#hello_operator = PythonOperator(task_id='hello_task', python_callable=print_hello, dag=dag)


parquet2hive = BashOperator(
    task_id='loop',
    bash_command='/opt/mapr/spark/spark-2.1.0/bin/spark-submit /home/mapr/Apurva/assignment/parquet2hive.py',
    dag=dag,
)

hive2csv = BashOperator(
    task_id='loop1',
    bash_command='/opt/mapr/spark/spark-2.1.0/bin/spark-submit /home/mapr/Apurva/assignment/hive2csv.py',
    dag=dag,
)

queries2json = BashOperator(
    task_id='loop2',
    bash_command='/opt/mapr/spark/spark-2.1.0/bin/spark-submit /home/mapr/Apurva/assignment/queries2json.py',
    dag=dag,
)

json2paraquet = BashOperator(
    task_id='loop3',
    bash_command='/opt/mapr/spark/spark-2.1.0/bin/spark-submit /home/mapr/Apurva/assignment/json2parquet.py',
    dag=dag,
)

# run_this >> hello_operator

parquet2hive >> hive2csv >> queries2json >> json2paraquet
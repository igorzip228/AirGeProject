from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
import pymysql
import great_expectations as ge
import pendulum




def execute_sql_query():
   context = ge.get_context()


   print("========= crating validator ..... ========")
   # соединяемся с данными
   validator = context.sources.pandas_default.read_csv(
       "/Users/igorzagorodniy/Documents/MY_FOLDER/STUDY/AirflowGeProject/my_data.csv")
   print("========= validator created ========")
   print(validator.head())




default_args = {
   'owner': 'airflow',
   'depends_on_past': False,
   'start_date': pendulum.today('UTC').add(days=-2),
   'email': ['airflow@example.com'],
   'email_on_failure': False,
   'email_on_retry': False,
   'retries': 1
}


dag = DAG(
   '2207_1_ai_ge_file', # will be displayed in airflow
   default_args=default_args,
   schedule=None,
)


execute_sql_task = PythonOperator(
   task_id='execute_sql_query',
   python_callable=execute_sql_query,
   dag=dag,
)

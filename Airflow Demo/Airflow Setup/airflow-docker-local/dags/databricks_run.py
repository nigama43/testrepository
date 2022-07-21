import airflow
from datetime import timedelta
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.providers.databricks.operators.databricks import DatabricksRunNowOperator

default_args = {
    'owner': 'airflow',
    'email': ['airflow@example.com']        ,
    'depends_on_past': False,
    'start_date': airflow.utils.dates.days_ago(0)
}
# create a DAG definition
dag = DAG(
dag_id ='databricks_run',
default_args=default_args,
schedule_interval=timedelta(days=1),
)

job_id=413053696957568
#job_id=715038344298837
notebook_params = {
 "input": "manish Kumar Mehta"
}
#python_params = [ "manish Kumar Mehta"]

#jar_params = ["douglas adams", "42"]

#spark_submit_params = ["--class", "org.apache.spark.examples.SparkPi"]

notebook_run = DatabricksRunNowOperator(
    job_id=job_id,
    task_id="demo",
    notebook_params=notebook_params,
    #python_params=python_params,
    #jar_params=jar_params,
    #spark_submit_params=spark_submit_params
    dag=dag
)

notebook_run
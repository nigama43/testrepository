import airflow
from datetime import timedelta
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
#from airflow.operators.bash_operator import BashOperator
from airflow.providers.databricks.operators.databricks import DatabricksSubmitRunOperator
#from airflow.contrib.operators.databricks_operator import DatabricksSubmitRunOperator
# default arguments for all the tasks
default_args = {
    'owner': 'airflow',
    'email': ['airflow@example.com']        ,
    'depends_on_past': False,
    'start_date': airflow.utils.dates.days_ago(0)
}
# create a DAG definition
dag = DAG(
'databricks_test',
default_args=default_args,
schedule_interval=timedelta(days=1),
)
# create a simple task that prints todays date
date_task = BashOperator(
    task_id='print_date',
    bash_command='date',
    dag=dag,
)    

# create a cluster config
new_cluster_conf = {
    'spark_version': '10.4.x-scala2.12',
    'node_type_id': 'Standard_DS3_v2import airflow
from datetime import timedelta
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
#from airflow.operators.bash_operator import BashOperator
from airflow.providers.databricks.operators.databricks import DatabricksRunNowOperator
#from airflow.contrib.operators.databricks_operator import DatabricksSubmitRunOperator
# default arguments for all the tasks
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

#job_id=413053696957568
#job_id=715038344298837
job_id=779925352567627
notebook_params = {
  "filename": "testing.csv",
  "fileloc": "/FileStore/tables/"
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

notebook_run#>>bash_task',
    'autoscale' : {
        'min_workers': 1,
        'max_workers': 8
    },
    'spark_conf': {
        'spark.databricks.delta.preview.enabled': 'true',
        'spark.sql.crossJoin.enabled': 'true',
        },
        'spark_env_vars': {
            'PYSPARK_PYTHON': '/databricks/python3/bin/python3'
        },
}    

notebook_task_params = {
        'new_cluster': new_cluster_conf,
        'notebook_task': {
        'notebook_path': '/Users/manish.mehta@tigeranalytics.com/demo-workbook',
        }
}
# create a task to run a notebook using above config
notebook_task = DatabricksSubmitRunOperator(
            task_id='notebook_task',
            dag=dag,
            json=notebook_task_params,
            do_xcom_push = True
)


# set the order of tasks
date_task >> notebook_task
import os
from datetime import datetime, timedelta

from airflow.models import DAG

try:
    from airflow.operators.empty import EmptyOperator
except ModuleNotFoundError:
    from airflow.operators.dummy import DummyOperator as EmptyOperator  # type: ignore
from airflow.providers.databricks.operators.databricks import DatabricksRunNowOperator
from airflow.operators.bash_operator import BashOperator
from airflow.providers.microsoft.azure.operators.data_factory import AzureDataFactoryRunPipelineOperator
from airflow.providers.microsoft.azure.sensors.data_factory import AzureDataFactoryPipelineRunStatusSensor
from airflow.utils.edgemodifier import Label
from airflow.operators.email_operator import EmailOperator

DAG_ID = "AzureDatafactory_Databricks_run_pipeline"

with DAG(
    dag_id=DAG_ID,
    start_date=datetime(2022, 7, 6),
    schedule_interval="@daily",
    catchup=False,
    default_args={
        "retries": 1,
        "retry_delay": timedelta(minutes=3),
        'email': ['abhishek.nigam@tigeranalytics.com'], 
        "azure_data_factory_conn_id": "Azure_data_factory_conn",
        "factory_name": "AzureDataFact0425",  # This can also be specified in the ADF connection.
        "resource_group_name": "Azure_Training",  # This can also be specified in the ADF connection.
    },
    default_view="graph",
) as dag:

    begin = EmptyOperator(task_id="begin")

    end = EmptyOperator(task_id="end")

    # [START howto_operator_adf_run_pipeline]
    run_pipeline1 = AzureDataFactoryRunPipelineOperator(
        task_id="Adf_run_pipeline_copyActivity",
        pipeline_name="pipeline3",
        parameters={"raw_filename": "testing.csv",
                    "landing_filename":"testing_landing.csv"
                    },
        do_xcom_push=True,            
    )

job_id=779925352567627
notebook_params = {
  "filename": "testing_landing.csv" 
}

notebook_run = DatabricksRunNowOperator(
    job_id=job_id,
    task_id="Databricks_processing",
    notebook_params=notebook_params,
    dag=dag,
)

    
send_email = EmailOperator( 
task_id='email_alert', 
to='abhishek.nigam@tigeranalytics.com',
subject='table created successfully', 
html_content="Date: {{ ds }} {{run_id}}", 
dag=dag,)

begin >> run_pipeline1>>notebook_run>> send_email>>end


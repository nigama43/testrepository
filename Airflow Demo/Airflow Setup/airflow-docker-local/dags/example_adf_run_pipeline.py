import os
from datetime import datetime, timedelta

from airflow.models import DAG

try:
    from airflow.operators.empty import EmptyOperator
except ModuleNotFoundError:
    from airflow.operators.dummy import DummyOperator as EmptyOperator  # type: ignore

from airflow.providers.microsoft.azure.operators.data_factory import AzureDataFactoryRunPipelineOperator
from airflow.providers.microsoft.azure.sensors.data_factory import AzureDataFactoryPipelineRunStatusSensor
from airflow.utils.edgemodifier import Label


DAG_ID = "example_adf_run_pipeline"


with DAG(
    dag_id=DAG_ID,
    start_date=datetime(2022, 7, 6),
    schedule_interval="@daily",
    catchup=False,
    default_args={
        "retries": 1,
        "retry_delay": timedelta(minutes=3),
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
        task_id="run_pipeline1",
        pipeline_name="pipeline2",
        #parameters={"myParam": "value"},
    )
    

    pipeline_run_sensor = AzureDataFactoryPipelineRunStatusSensor(
        task_id="pipeline_run_sensor",
        run_id=run_pipeline1.output["run_id"],
        #destination=run_pipeline1.output["Destination"],
    )
    # [END howto_operator_adf_run_pipeline_async]

    begin >> Label("No async wait") >> run_pipeline1>>pipeline_run_sensor >> end
    #begin >> Label("Do async wait with sensor") >> run_pipeline2
    #[run_pipeline1, pipeline_run_sensor] >> end

    # Task dependency created via XComArgs:
    #   run_pipeline2 >> pipeline_run_sensor

    

    # This test needs watcher in order to properly mark success/failure
    # when "tearDown" task with trigger rule is part of the DAG
#    list(dag.tasks) >> watcher()
#
#from tests.system.utils import get_test_run  # noqa: E402
#
## Needed to run the example DAG with pytest (see: tests/system/README.md#run_via_pytest)
#
#test_run = get_test_run(dag)
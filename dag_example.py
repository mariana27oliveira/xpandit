import sys
import os

dags_folder = os.environ.get("AIRFLOW_HOME")
sys.path.append(os.path.join(dags_folder, "dags", "databricks", "include"))

from ifw_functions_dbk import check_ingestion_status
from log_analytics_functions import send_starting_log, send_success_log, send_failure_log
from utils import build_notebook_params

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.sensors.python import PythonSensor
from airflow.utils.dates import days_ago
from airflow.models import Variable
from airflow.datasets import Dataset
from airflow.utils.trigger_rule import TriggerRule
from airflow.providers.databricks.operators.databricks import DatabricksRunNowOperator

import logging
import pendulum

local_tz = pendulum.timezone("Europe/Lisbon")
logger = logging.getLogger("airflow.mlops")
logger.setLevel(logging.INFO)

silver_gfgeralveiculos = Dataset("silver-scdt2-bae-opasm_sftp_xrp_viewreports-gfgeralveiculos") 
silver_gfgeralfornecedores = Dataset("silver-scdt2-bae-opasm_sftp_xrp_viewreports-gfgeralfornecedores") 
silver_gfgeralcontratos = Dataset("silver-scdt2-bae-opasm_sftp_xrp_viewreports-gfgeralcontratos") 
silver_gfgeralutilizadores = Dataset("silver-scdt2-bae-opasm_sftp_xrp_viewreports-gfgeralutilizadores") 
silver_gfgeraldepartamentos = Dataset("silver-scdt2-bae-opasm_sftp_xrp_viewreports-gfgeraldepartamentos") 
silver_gfgeralcentroscusto = Dataset("silver-scdt2-bae-opasm_sftp_xrp_viewreports-gfgeralcentroscusto") 
silver_gfgeralprocregistokm = Dataset("silver-scdt1-bae-opasm_sftp_xrp_viewreports-gfgeralprocregistokm") 
silver_gfgeraldespesasimportadas = Dataset("silver-scdt2-bae-opasm_sftp_xrp_viewreports-gfgeraldespesasimportadas") 
silver_gfgeralprocessospool = Dataset("silver-scdt2-bae-opasm_sftp_xrp_viewreports-gfgeralprocessospool") 
silver_gfgeralafetacoesveiculos = Dataset("silver-scdt1-bae-opasm_sftp_xrp_viewreports-gfgeralafetacoesveiculos") 
gold_frota = Dataset("gold-models-bae-opasm_sftp_xrp_fleetmanagement") 


ENV = Variable.get("AIRFLOW_ENV_DBK", default_var="qa") 

MONITORING_PARAMS = {
  "TAGS": {
        "data_product": "OPASM", 
        "brisa_group": "BAE" 
    }
}

def get_default_params():
    base_params = {
        "qa": {
            "DATABRICKS_CONN_ID": "dbw-App-Dbricks-Framework-dbt-QA-001",
            "JOB_ID": "845810166358249", 
            "AZURE_CONN_ID": "id-astro-dataplatform-framework-devqa-001",
            **MONITORING_PARAMS,
            "DATABRICKS_NOTEBOOK_PARAMS": {
                "model_paths":"[\"gold\"]", 
                "tags": "{\"data_product\":\"OPASM\", \"brisa_group\":\"BAE\"}",
                "job_name": "$PROCESS_NAME",
                "run_id": "$RUN_ID", 
                "extra_flags": "{\"gold\":  {\"vars\": {\"apply_comments\": \"false\", \"exec_date\":\"\"}}, \"dbt_test\":  {\"vars\": {\"exec_date\":\"\"}}}",
                "job_id": "845810166358249"
            }
        },
        "prod": {
            "DATABRICKS_CONN_ID": "dbw-App-Dbricks-Framework-dbt-QA-001",
            "JOB_ID": "845810166358249", 
            "AZURE_CONN_ID": "id-astro-dataplatform-framework-devqa-001",
            **MONITORING_PARAMS,
            "DATABRICKS_NOTEBOOK_PARAMS": {
                "model_paths":"[\"gold\"]",
                "tags": "{\"data_product\":\"OPASM\", \"brisa_group\":\"BAE\"}",
                "job_name": "$PROCESS_NAME",
                "run_id": "$RUN_ID", 
                "extra_flags": "{\"gold\":  {\"vars\": {\"apply_comments\": \"false\", \"exec_date\":\"\"}}, \"dbt_test\":  {\"vars\": {\"exec_date\":\"\"}}}",
                "job_id": "845810166358249"
            }
        }
    }

    return base_params

DEFAULT_PARAMS = get_default_params()

with DAG(
        dag_id="dag_gold-bae-opasm_sftp_xrp_fleetmanagement", 
        description="Trigger Gold Processing", 
        schedule=[silver_gfgeralveiculos, silver_gfgeralfornecedores, silver_gfgeralcontratos,
                  silver_gfgeralutilizadores, silver_gfgeraldepartamentos, silver_gfgeralcentroscusto,
                  silver_gfgeralprocregistokm, silver_gfgeraldespesasimportadas, silver_gfgeralprocessospool,
                  silver_gfgeralafetacoesveiculos], 
        start_date=days_ago(1),
        catchup=False,
        default_args={
            "owner": "airflow",
            "retries": 0,
        },
        params=DEFAULT_PARAMS[ENV],
        tags=["databricks", "brisagroup_bae", "dp_opasm"],
        render_template_as_native_obj=True
) as dag:
    starting_log = PythonOperator(
        task_id="starting_log",
        python_callable=send_starting_log
    )

    prepare_params = PythonOperator(
        task_id="prepare_params",
        python_callable=build_notebook_params,
        op_kwargs={"notebook_params": "{{ params.DATABRICKS_NOTEBOOK_PARAMS }}"},
        provide_context=True,
    )

    run_databricks_job = DatabricksRunNowOperator(
        task_id='run_databricks_job',
        job_id=dag.params["JOB_ID"],
        databricks_conn_id=dag.params["DATABRICKS_CONN_ID"],
        notebook_params="{{ task_instance.xcom_pull(task_ids='prepare_params') }}"
    )

    success_log = PythonOperator(
        task_id="success_log",
        python_callable=send_success_log,
        outlets=[gold_frota],
    )

    failure_log = PythonOperator(
        task_id="failure_log",
        python_callable=send_failure_log,
        trigger_rule=TriggerRule.ONE_FAILED,
    )

    starting_log >> prepare_params >> run_databricks_job >> [success_log, failure_log]

# General
import os
from pathlib import Path
ROOT_DIR = Path(__file__).parent.resolve()
from datetime import datetime, timedelta

# Airflow
from airflow.models import DAG, DagBag
from airflow import macros
from airflow.operators.dummy_operator import DummyOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryCheckOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryValueCheckOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.operators.python_operator import PythonOperator


# Call Composer Enviroment Variables
PROJECT_ID = os.environ['GCP_PROJECT']
# Get BQ dataset name from Composer enviroment variable
DATASET = os.environ['BQ_DATASET']
# Get environment name from Composer environment variable
ENV = os.environ['ENVIRONMENT']
EMAIL= 'datamanagementmkt@teco.com.ar' if PROJECT_ID == "teco-prod-adam-85cc" else 'AEOrue@proveedor.teco.com.ar'
#Ruta del modelo usado segun ambiente
MODEL_PATH = 'data_lake_analytics' if PROJECT_ID == "teco-prod-adam-85cc" else 'sdb_datamining'


#---------------------------------------------------------------------------------------------------------------
#Se agrega codigo para limpiear estado 'failed' en Dags de Monitoreo en caso de que la ABT falle.
#---------------------------------------------------------------------------------------------------------------
TARGETS = [
    "monCapro_est",
    "monCapro_per"
]

def get_dags(targets):
    dagbag = DagBag()
    return [dagbag.get_dag(dag) for dag in targets]

def clear_dags(targets):
    dags = get_dags(targets)
    DAG.clear_dags(dags=dags, only_failed=True)
    
#---------------------------------------------------------------------------------------------------------------

# Inicio DAG 
default_args={
    'start_date': datetime(2022, 2, 16),
    "owner": "AEOrue", 
    "email":  [EMAIL], 
    "retries": 5, 
    "retry_delay": timedelta(days=1)
} 


with DAG(
    'abtCapro_m',
    description='DAG de automatización de la tabla ABT Capro',
    max_active_runs=1,
    is_paused_upon_creation=True,
    catchup=True,
    tags=["ml", f"{ENV}", "movil", "capro"], 
    schedule_interval="0 3 16 * *", #dia 16 de cada mes a las 03:00 am UTC..
    template_searchpath=str(ROOT_DIR/"tasks/"),
    user_defined_macros={
        "PROJECT_ID": PROJECT_ID,
        "DATASET": DATASET, 
        "MODEL_PATH":MODEL_PATH,
    },

    user_defined_filters={
        "ym": lambda x: x[:6],
        "first_day":  lambda x: x.replace(day=1),
        "last_day": lambda x: (x.replace(day=1) + macros.dateutil.relativedelta.relativedelta(months=+1, days=-1)),
        "minus_3_months": lambda x: x + macros.dateutil.relativedelta.relativedelta(months=-3),
        "minus_2_months": lambda x: x + macros.dateutil.relativedelta.relativedelta(months=-2),
        "minus_1_months": lambda x: x + macros.dateutil.relativedelta.relativedelta(months=-1),
        "minus_29_days": lambda x: x + macros.dateutil.relativedelta.relativedelta(days=-29),
        "wk_nodash": lambda x: x.strftime('%G%V'),
        "ds_nodash": lambda x: x.strftime('%Y%m%d'),
        "ds": lambda x: x.strftime('%Y-%m-%d')
    }, 
       
    default_args=default_args,
    ) as dag:  
  

    
 # Dummy_Task
#----------------------------------------------------------------------------------
    entry = DummyOperator(task_id="entry-point")
    
 
 # Verificamos si las tablas externas tienen datos del periodo.
#----------------------------------------------------------------------------------
    
    check_vc_parque_movil_unificado_aa= BigQueryValueCheckOperator(
        task_id='check_vc_parque_movil_unificado_aa',
        sql="""                      
            SELECT
            CASE WHEN b.total_particiones = 1 THEN 1 ELSE 0 END
            FROM (
                SELECT COUNT(*) total_particiones
                FROM (
                    SELECT periodo
                    FROM `teco-prod-datalake-8f6a.vw_repo_convergente.vc_parque_movil_unificado_aa`
                    WHERE periodo = CAST ('{{ next_execution_date | minus_1_months | ds_nodash | ym }}'  AS INTEGER)
                    GROUP BY periodo) a ) b
            """,
        pass_value=1, 
        use_legacy_sql=False,
        location='us-east4',
        ) 


    check_ft_prepago_m= BigQueryValueCheckOperator(
        task_id='check_ft_prepago_m',
        sql="""
            SELECT COUNT(*) 
            FROM `teco-prod-adam-85cc.data_lake_analytics`.INFORMATION_SCHEMA.PARTITIONS
            WHERE table_name = 'ft_prepago_m' AND partition_id =  '{{ next_execution_date | ds_nodash | ym }}'
            """,
        pass_value=1, 
        use_legacy_sql=False,
        location='us-east4',
        ) 

    check_stg_traficodatos_m= BigQueryValueCheckOperator(
        task_id='check_stg_traficodatos_m',
        sql="""
            SELECT COUNT(*) 
            FROM `teco-prod-adam-85cc.data_lake_analytics`.INFORMATION_SCHEMA.PARTITIONS
            WHERE table_name = 'stg_traficodatos_m' AND partition_id =  '{{ next_execution_date | minus_1_months | ds_nodash | ym }}'
            """,
        pass_value=1, 
        use_legacy_sql=False,
        location='us-east4',
        ) 

    check_indicadores_psi_bl= BigQueryValueCheckOperator(
        task_id='check_indicadores_psi_bl',
        sql="""          
            SELECT COUNT(*) FROM (
                SELECT MAX (fecha) 
                FROM `{{ PROJECT_ID }}.{{ DATASET }}.indicadores_psi_bl` 
                WHERE modelo = 'Capro')
            """,
        pass_value=1, 
        use_legacy_sql=False,
        location='us-east4',
        ) 

# Construcción de la ABT de Capro
#----------------------------------------------------------------------------------
 
    load_abt_capro_m = BigQueryInsertJobOperator(
        task_id='load_abt_capro_m',
        project_id=PROJECT_ID,
        configuration={
            "query": {
                "query": "{% include 'load_abt_capro_m.sql' %}", 
                "useLegacySql": False,
            }
        }
    )


# Ejecutamos las predicciones batch
#----------------------------------------------------------------------------------

    prediction_abt_capro_m = BigQueryInsertJobOperator(
        task_id='prediction_abt_capro_m',
        project_id=PROJECT_ID,
        configuration={
            "query": {
                "query": "{% include 'prediction_abt_capro_m.sql' %}", 
                "useLegacySql": False,
            }
        }
    )


# Añadimos el score y el bin a la ABT
#----------------------------------------------------------------------------------
 
    insert_score_abt_capro_m = BigQueryInsertJobOperator(
        task_id='insert_score_abt_capro_m',
        project_id=PROJECT_ID,
        configuration={
            "query": {
                "query": "{% include 'insert_score_abt_capro_m.sql' %}", 
                "useLegacySql": False,
            }
        }
    )


# Verificamos si los datos del periodo se cargaron
#----------------------------------------------------------------------------------

    check_abt_capro_m= BigQueryValueCheckOperator(
        task_id='check_abt_capro_m',
        sql="""
            SELECT COUNT(*) 
            FROM `{{ PROJECT_ID }}.{{ DATASET }}`.INFORMATION_SCHEMA.PARTITIONS
            WHERE table_name = 'abt_capro_m' AND partition_id = '{{ next_execution_date | minus_1_months | ds_nodash | ym }}'
            """,
        pass_value=1, 
        use_legacy_sql=False,
        location='us-east4',
        )
        

# Task que ejecuta la tarea de Limpieza de estado en DAG de Monitoreos
#----------------------------------------------------------------------------------
    clear_dags_downstream = PythonOperator(
        task_id="clear_dags_downstream",
        python_callable=clear_dags,
        op_kwargs={
            "targets": TARGETS
        },
        dag=dag
    )


# Dummy_Task
#----------------------------------------------------------------------------------
    endtask = DummyOperator(task_id="endtask")


# Generamos dependencias de las Tareas
#----------------------------------------------------------------------------------
 
entry >> [check_vc_parque_movil_unificado_aa, check_ft_prepago_m, check_stg_traficodatos_m, check_indicadores_psi_bl] >>  load_abt_capro_m >> prediction_abt_capro_m  >> insert_score_abt_capro_m >> check_abt_capro_m >> clear_dags_downstream >> endtask



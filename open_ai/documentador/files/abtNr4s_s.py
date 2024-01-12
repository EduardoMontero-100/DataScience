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
    "monNr4s_est",
    "monNr4s_per"
]

def get_dags(targets):
    dagbag = DagBag()
    return [dagbag.get_dag(dag) for dag in targets]

def clear_dags(targets):
    dags = get_dags(targets)
    DAG.clear_dags(dags=dags, only_failed=True)
    
#---------------------------------------------------------------------------------------------------------------

# Inicio DAG.
default_args={
    'start_date': datetime(2023, 4, 24), 
    "owner": "AEOrue", 
    "email": [EMAIL], 
    "retries": 5, 
    "retry_delay": timedelta(days=1)
} 


with DAG(
    'abtNr4s_s',
    description='DAG de automatización de la tabla ABT Nr4s',
    max_active_runs=1,
    is_paused_upon_creation=True,
    catchup=True,
    tags=["ml", f"{ENV}","movil", "nr4s"], 
    schedule_interval="0 16 * * 1", #Todos los Lunes a las 16:00hs UTC.
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
        "minus_4_months": lambda x: x + macros.dateutil.relativedelta.relativedelta(months=-4),
        "minus_2_months": lambda x: x + macros.dateutil.relativedelta.relativedelta(months=-2),
        "minus_1_months": lambda x: x + macros.dateutil.relativedelta.relativedelta(months=-1),
        "minus_35_days": lambda x: x + macros.dateutil.relativedelta.relativedelta(days=-35),
        "minus_29_days": lambda x: x + macros.dateutil.relativedelta.relativedelta(days=-29),
        "minus_21_days": lambda x: x + macros.dateutil.relativedelta.relativedelta(days=-21),
        "minus_1_days": lambda x: x + macros.dateutil.relativedelta.relativedelta(days=-1),
        "per_parque_movil_unificado_aa":  lambda x: x + macros.dateutil.relativedelta.relativedelta(months=-1, days=-1),
        "wk_nodash": lambda x: x.strftime('%G%V'),
        "ds_nodash": lambda x: x.strftime('%Y%m%d'),
        "ds": lambda x: x.strftime('%Y-%m-%d')
    }, 
       
    default_args=default_args,
    ) as dag:  
  

    
 # Dummy_Task
 #----------------------------------------------------------------------------------
    begin_task = DummyOperator(task_id="begin_task")
    
 
 # Verificamos si las tablas externas tienen datos del periodo.
 #----------------------------------------------------------------------------------
 #1:
    check_vc_parque_movil_unificado_aa= BigQueryValueCheckOperator(
        task_id='check_vc_parque_movil_unificado_aa',
        sql="""                      
            SELECT
            CASE WHEN b.total_particiones >= 1 AND b.total_particiones <=2 THEN 1 ELSE 0 END
            FROM (
                SELECT COUNT(*) total_particiones
                FROM (
                    SELECT periodo
                    FROM `teco-prod-datalake-8f6a.vw_repo_convergente.vc_parque_movil_unificado_aa`
                    WHERE periodo BETWEEN CAST ('{{ next_execution_date | minus_1_days | minus_2_months | ds_nodash | ym }}'  AS INTEGER)
                        AND CAST ('{{ next_execution_date | minus_1_days | minus_1_months | ds_nodash | ym }}'  AS INTEGER)
                    GROUP BY periodo) a ) b
            """,
        pass_value=1, 
        use_legacy_sql=False,
        location='us-east4',
        ) 

 #2:
    check_stg_recargas_d_m= BigQueryValueCheckOperator(
        task_id='check_stg_recargas_d_m',
        sql="""
            SELECT
            CASE WHEN B.Total_Particiones >=2 AND B.Total_Particiones <=3 THEN 1 ELSE 0 END
            FROM (
                SELECT COUNT(*) Total_Particiones
                FROM `teco-prod-adam-85cc.data_lake_analytics`.INFORMATION_SCHEMA.PARTITIONS
                    WHERE table_name = 'stg_recargas_d_m'
                        AND partition_id BETWEEN '{{ next_execution_date | minus_35_days | ds_nodash | ym }}' 
                            AND '{{ next_execution_date | minus_1_days | ds_nodash | ym }}' ) B
            """,
        pass_value=1, 
        use_legacy_sql=False,
        location='us-east4',
        ) 

 #3:
    check_stg_traficodatos_m= BigQueryValueCheckOperator(
        task_id='check_stg_traficodatos_m',
        sql="""
            SELECT COUNT(*) 
            FROM `teco-prod-adam-85cc.data_lake_analytics`.INFORMATION_SCHEMA.PARTITIONS
            WHERE table_name = 'stg_traficodatos_m' 
            AND partition_id BETWEEN  '{{ next_execution_date | minus_1_days | minus_4_months | ds_nodash | ym }}'
                AND '{{ next_execution_date | minus_1_days | minus_1_months | ds_nodash | ym }}'
            """,
        pass_value=4, 
        use_legacy_sql=False,
        location='us-east4',
        ) 

 #4:
    check_ft_prepagobq_s= BigQueryValueCheckOperator(
        task_id='check_ft_prepagobq_s',
        sql="""
            SELECT COUNT(*) 
            FROM `teco-prod-adam-85cc.data_lake_analytics`.INFORMATION_SCHEMA.PARTITIONS
            WHERE table_name = 'ft_prepagobq_s' AND partition_id =  '{{ next_execution_date | ds_nodash }}'
            """,
        pass_value=1, 
        use_legacy_sql=False,
        location='us-east4',
        ) 

 #5: 
    check_registro_actividad_rtdm= BigQueryValueCheckOperator(
        task_id='check_registro_actividad_rtdm',
        sql="""
            SELECT COUNT(*) Total_Particiones
                FROM `teco-prod-datalake-8f6a.stg_rtdm`.INFORMATION_SCHEMA.PARTITIONS
                WHERE table_name = 'registro_actividad_rtdm'
                    AND partition_id  BETWEEN '{{ next_execution_date | minus_21_days | ds_nodash }}'
                        AND '{{ next_execution_date | minus_1_days | ds_nodash }}'
            """,
        pass_value=21, 
        use_legacy_sql=False,
        location='us-east4',
        ) 

 #6:
    check_dim_tipoRecarga_od= BigQueryCheckOperator(
        task_id='check_dim_tipoRecarga_od',
        sql="""
            SELECT COUNT(*)
            FROM `teco-prod-adam-85cc.data_lake_analytics.dim_tipoRecarga_od`
            WHERE
            usuario_perfil_contrato NOT IN ('','N/A','Unknow') AND 
            grupo IN('Pago Electrónico','Saldo Virtual','Tarjeta De Crédito','Tarjeta')
            """,
        use_legacy_sql=False,
        location='us-east4',
        ) 

 #7:
    check_indicadores_psi_bl= BigQueryValueCheckOperator(
        task_id='check_indicadores_psi_bl',
        sql="""          
            SELECT COUNT(*) FROM (
                SELECT MAX (fecha) 
                FROM `{{ PROJECT_ID }}.{{ DATASET }}.indicadores_psi_bl` 
                WHERE modelo = 'Nr4s')
            """,
        pass_value=1, 
        use_legacy_sql=False,
        location='us-east4',
        ) 


# Construcción de la ABT de Nr4s
#----------------------------------------------------------------------------------
 
    insert_abt_nr4s_s = BigQueryInsertJobOperator(
        task_id='insert_abt_nr4s_s',
        project_id=PROJECT_ID,
        configuration={
            "query": {
                "query": "{% include '01_insert_abt_nr4s_s.sql' %}", 
                "useLegacySql": False,
            }
        }
    )


# Ejecutamos las predicciones batch y añadimos el score y bin
#----------------------------------------------------------------------------------

    prediction_abt_nr4s_s = BigQueryInsertJobOperator(
        task_id='prediction_abt_nr4s_s',
        project_id=PROJECT_ID,
        configuration={
            "query": {
                "query": "{% include '02_prediction_abt_nr4s_s.sql' %}", 
                "useLegacySql": False,
            }
        }
    )



# Verificamos si los datos del periodo se cargaron
#----------------------------------------------------------------------------------

    check_abt_nr4s_s= BigQueryValueCheckOperator(
        task_id='check_abt_nr4s_s',
        sql="""
            SELECT COUNT(*) 
            FROM `{{ PROJECT_ID }}.{{ DATASET }}`.INFORMATION_SCHEMA.PARTITIONS
            WHERE table_name = 'abt_nr4s_s' AND partition_id = '{{ next_execution_date | minus_1_days | wk_nodash }}'
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
    end_task = DummyOperator(task_id="end_task")


 # Generamos dependencias de las Tareas
 #----------------------------------------------------------------------------------
begin_task >> [check_vc_parque_movil_unificado_aa, 
                            check_stg_recargas_d_m, 
                            check_stg_traficodatos_m, 
                            check_ft_prepagobq_s, 
                            check_registro_actividad_rtdm, 
                            check_dim_tipoRecarga_od, 
                            check_indicadores_psi_bl 
                            ] >> insert_abt_nr4s_s >> prediction_abt_nr4s_s >> check_abt_nr4s_s >> clear_dags_downstream >> end_task




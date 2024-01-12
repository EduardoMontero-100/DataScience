----------------------------------------------------
-- VARIABLES
----------------------------------------------------
DECLARE filter_date DATE;
DECLARE M_ABT INT64;
          
SET filter_date = '{{ next_execution_date | ds }}';  -- borrar ya que es la fecha de ejecución del DAG
SET M_ABT     = CAST(FORMAT_DATE('%Y%m',DATE_ADD(filter_date, INTERVAL -1 MONTH)) AS INT64); -- Mes usado en abt_capro 

-- Creamos una tabla temporal con las predicciones batch
CREATE OR REPLACE TABLE `{{ PROJECT_ID }}.{{ DATASET }}.tmp_capro_batch_preds` AS
SELECT * 
FROM ML.PREDICT(
        MODEL `{{ PROJECT_ID }}.{{MODEL_PATH}}.capro_bq`, ---<<<---
        (SELECT * FROM `{{ PROJECT_ID }}.{{ DATASET }}.abt_capro_m`  where periodo = M_ABT))


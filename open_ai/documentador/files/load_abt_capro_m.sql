----------------------------------------------------
-- VARIABLES
----------------------------------------------------

DECLARE filter_date DATE;
DECLARE M_CIEGO, M_PREPAGO, M_TRAFICO, M_PREV, M_ABT INT64;
-- Definimos variables que capturan el percentil 99
DECLARE P_99_v1,P_99_v2,P_99_v3,P_99_v4,P_99_v5,P_99_v6,P_99_v7,P_99_v8,P_99_v9,P_99_v10,P_99_v11,P_99_v12,
        P_99_v13,P_99_v14,P_99_v15,P_99_v16,P_99_v17,P_99_v18,P_99_v19,P_99_v20,P_99_v21,P_99_v22,P_99_v23,
        P_99_v24,P_99_v25,P_99_v26,P_99_v27,P_99_v28,P_99_v29,P_99_v30,P_99_v31,P_99_v32,P_99_v33,P_99_v34,
        P_99_v35,P_99_v36,P_99_v37,P_99_v38,P_99_v39,P_99_v40,P_99_v41,P_99_v42,P_99_v43,P_99_v44,P_99_v45,
        P_99_v46,P_99_v47,P_99_v48,P_99_v49,P_99_v50,P_99_v51,P_99_v52,P_99_v53,P_99_v54,P_99_v55,P_99_v56,
        P_99_v57,P_99_v58,P_99_v59,P_99_v60,P_99_v61,P_99_v62,P_99_v63,P_99_v64,P_99_v65,P_99_v66,P_99_v67,
        P_99_v68,P_99_v69,P_99_v70,P_99_v71,P_99_v72,P_99_v73,P_99_v74,P_99_v75,P_99_v76,P_99_v77,P_99_v78,
        P_99_v79,P_99_v80,P_99_v81,P_99_v82,P_99_v83,P_99_v84,P_99_v85,P_99_v86,P_99_v87,P_99_v88,P_99_v89,
        P_99_v90,P_99_v91,P_99_v92,P_99_v93,P_99_v94,P_99_v95,P_99_v96,P_99_v97,P_99_v98,P_99_v99,P_99_v100,
        P_99_v101,P_99_v102,P_99_v103,P_99_v104,P_99_v105,P_99_v106,P_99_v107,P_99_v108, P_99_v109 FLOAT64;
-- Definimos las variables de provincia    
DECLARE Provincia_ah1s, Provincia_ah2s, Provincia_ah3s, Provincia_ah4s ARRAY<STRING>;
          
SET filter_date = '{{ next_execution_date | ds }}'; -- borrar ya que es la fecha de ejecución del DAG
    
SET M_CIEGO   = CAST(FORMAT_DATE('%Y%m',filter_date) AS INT64); -- Mes ejecución
SET M_PREPAGO = CAST(FORMAT_DATE('%Y%m',filter_date) AS INT64); -- Mes usado en la ft_prepago_m
SET M_TRAFICO = CAST(FORMAT_DATE('%Y%m',DATE_ADD(filter_date, INTERVAL -1 MONTH)) AS INT64); -- Mes usado en stg_traficodatos_m
SET M_PREV    = CAST(FORMAT_DATE('%Y%m',DATE_ADD(filter_date, INTERVAL -1 MONTH)) AS INT64); -- Mes usado en stg_prevalencia_m
SET M_ABT     = CAST(FORMAT_DATE('%Y%m',DATE_ADD(filter_date, INTERVAL -1 MONTH)) AS INT64); -- Mes usado en abt_capro 

--------------------------------------------------------------------
---- IMPUTACIÓN DE NULOS
--------------------------------------------------------------------

-- Todas las variables de la ft_prepago_m las imputamos con 0 a excepción:
-- recargas_rec_u1u2_pagos_ratio que imputamos con 1

-- Todas las variables de stg_traficodatos_m las imputamos con 0 datos que indican consumo, 
-- y si están con nulo es porque no consumieron datos 

-- Creamos la tabla con los datos imputados

CREATE OR REPLACE TABLE  `{{ PROJECT_ID }}.{{ DATASET }}.tmp_capro_imp` AS
SELECT 
-- Parque
CAST(A.linea AS INT64)                                          AS linea
-- ft_prepago_m 
,CAST(ofertas_all_u6_pagos_sum AS FLOAT64)                      AS ofertas_all_u6_pagos_sum
,CAST(ofertas_all_u5_pagos_sum AS FLOAT64)                      AS ofertas_all_u5_pagos_sum
,CAST(recargas_mdp_pagoelectronico_u6_pagos_cumsum AS FLOAT64)  AS recargas_mdp_pagoelectronico_u6_pagos_cumsum
,CAST(recargas_mdp_pagoelectronico_u5_pagos_cumsum AS FLOAT64)  AS recargas_mdp_pagoelectronico_u5_pagos_cumsum
,CAST(ofertas_all_u4_pagos_max AS FLOAT64)                      AS ofertas_all_u4_pagos_max
,CAST(ofertas_all_u6_vigencia1_std AS FLOAT64)                  AS ofertas_all_u6_vigencia1_std
,CAST(ofertas_all_u4_pagos_sum AS FLOAT64)                      AS ofertas_all_u4_pagos_sum
,CAST(ofertas_all_u3_pagos_avg AS FLOAT64)                      AS ofertas_all_u3_pagos_avg
,CAST(ofertas_all_u3_pagos_max AS FLOAT64)                      AS ofertas_all_u3_pagos_max
,CAST(ofertas_all_u2_pagos_avg AS FLOAT64)                      AS ofertas_all_u2_pagos_avg
,CAST(ifnull(recargas_rec_u1u2_pagos_ratio, '1') AS FLOAT64)    AS recargas_rec_u1u2_pagos_ratio
,CAST(ofertas_all_u6_vigencia1_avg AS FLOAT64)                  AS ofertas_all_u6_vigencia1_avg
,CAST(ofertas_all_u1_pagos_avg AS FLOAT64)                      AS ofertas_all_u1_pagos_avg
,CAST(recargas_rec_u1u3_pagos_diff AS FLOAT64)                  AS recargas_rec_u1u3_pagos_diff
,CAST(ofertas_all_u2_pagos_sum AS FLOAT64)                      AS ofertas_all_u2_pagos_sum
,CAST(recargas_rec_u1_arbu AS FLOAT64)                          AS recargas_rec_u1_arbu
,CAST(recargas_rec_u1u6_pagos_diff AS FLOAT64)                  AS recargas_rec_u1u6_pagos_diff
,CAST(recargas_rec_u1u5_pagos_diff AS FLOAT64)                  AS recargas_rec_u1u5_pagos_diff
,CAST(recargas_rec_u3_pagos_sum AS FLOAT64)                     AS recargas_rec_u3_pagos_sum
,CAST(recargas_mdp_pagoelectronico_u1_pagos_sum AS FLOAT64)     AS recargas_mdp_pagoelectronico_u1_pagos_sum
,CAST(recargas_rec_u2_arbu AS FLOAT64)                          AS recargas_rec_u2_arbu
,CAST(recargas_rec_u2_pagos_cumsum AS FLOAT64)                  AS recargas_rec_u2_pagos_cumsum
,CAST(recargas_mdp_pagoelectronico_u3_recargas_count AS FLOAT64) AS recargas_mdp_pagoelectronico_u3_recargas_count
,CAST(recargas_rec_u1u2_pagos_diff AS FLOAT64)                  AS recargas_rec_u1u2_pagos_diff
,CAST(recargas_rec_u5_arbu AS FLOAT64)                          AS recargas_rec_u5_arbu
,CAST(recargas_mdp_pagoelectronico_u2_recargas_count AS FLOAT64) AS recargas_mdp_pagoelectronico_u2_recargas_count
,CAST(ofertas_all_u5_vigencia1_avg AS FLOAT64)                  AS ofertas_all_u5_vigencia1_avg
,CAST(ofertas_all_u4_vigencia1_std AS FLOAT64)                  AS ofertas_all_u4_vigencia1_std
,CAST(recargas_rec_u4_arbu AS FLOAT64)                          AS recargas_rec_u4_arbu
,CAST(recargas_mdp_pagoelectronico_u2_pagos_sum AS FLOAT64)     AS recargas_mdp_pagoelectronico_u2_pagos_sum
,CAST(recargas_rec_u6_pagos_sum AS FLOAT64)                     AS recargas_rec_u6_pagos_sum
,CAST(recargas_rec_u4_pagos_sum AS FLOAT64)                     AS recargas_rec_u4_pagos_sum
,CAST(recargas_mdp_pagoelectronico_u3_pagos_sum AS FLOAT64)     AS recargas_mdp_pagoelectronico_u3_pagos_sum
,CAST(recargas_rec_u5_pagos_cumsum AS FLOAT64)                  AS recargas_rec_u5_pagos_cumsum
,CAST(recargas_rec_u3_pagos_cumsum AS FLOAT64)                  AS recargas_rec_u3_pagos_cumsum
,CAST(recargas_rec_u4_pagos_cumsum AS FLOAT64)                  AS recargas_rec_u4_pagos_cumsum
,CAST(recargas_mdp_pagoelectronico_u4_pagos_sum AS FLOAT64)     AS recargas_mdp_pagoelectronico_u4_pagos_sum
,CAST(recargas_rec_u6_arbu AS FLOAT64)                          AS recargas_rec_u6_arbu
,CAST(recargas_rec_u3_arbu AS FLOAT64)                          AS recargas_rec_u3_arbu
,CAST(ofertas_all_u4_vigencia1_avg AS FLOAT64)                  AS ofertas_all_u4_vigencia1_avg
,CAST(recargas_rec_u6_pagos_cumsum AS FLOAT64)                  AS recargas_rec_u6_pagos_cumsum
,CAST(recargas_mdp_pagoelectronico_u4_recargas_count AS FLOAT64) AS recargas_mdp_pagoelectronico_u4_recargas_count
,CAST(recargas_mdp_pagoelectronico_u6_recargas_count AS FLOAT64) AS recargas_mdp_pagoelectronico_u6_recargas_count
,CAST(ofertas_all_u3_vigencia1_avg AS FLOAT64)                   AS ofertas_all_u3_vigencia1_avg
-- stg_traficodatos_m
,CAST(uplink_mb_t_max AS FLOAT64)                               AS uplink_mb_t_max
,CAST(uplink_mb_sh4_std AS FLOAT64)                             AS uplink_mb_sh4_std
,CAST(uplink_mb_t_std AS FLOAT64)                               AS uplink_mb_t_std
,CAST(uplink_mb_s_std AS FLOAT64)                               AS uplink_mb_s_std
,CAST(downlink_mb_fs_max AS FLOAT64)                            AS downlink_mb_fs_max
,CAST(uplink_mb_s_max AS FLOAT64)                               AS uplink_mb_s_max
,CAST(totat_traffic_mb_fs_max AS FLOAT64)                       AS totat_traffic_mb_fs_max
,CAST(totat_traffic_mb_s_max AS FLOAT64)                        AS totat_traffic_mb_s_max
,CAST(uplink_mb_sh4_max AS FLOAT64)                             AS uplink_mb_sh4_max
,CAST(downlink_mb_sh4_std AS FLOAT64)                           AS downlink_mb_sh4_std
,CAST(uplink_mb_s_avg AS FLOAT64)                               AS uplink_mb_s_avg
,CAST(traffic_4g_mb_sh4_std AS FLOAT64)                         AS traffic_4g_mb_sh4_std
,CAST(downlink_mb_s_max AS FLOAT64)                             AS downlink_mb_s_max
,CAST(traffic_4g_mb_fs_max AS FLOAT64)                          AS traffic_4g_mb_fs_max
,CAST(uplink_mb_sh4_avg AS FLOAT64)                             AS uplink_mb_sh4_avg
,CAST(totat_traffic_mb_t_max AS FLOAT64)                        AS totat_traffic_mb_t_max
,CAST(totat_traffic_mb_sh4_max AS FLOAT64)                      AS totat_traffic_mb_sh4_max
,CAST(downlink_mb_t_max AS FLOAT64)                             AS downlink_mb_t_max
,CAST(downlink_mb_s_std AS FLOAT64)                             AS downlink_mb_s_std
,CAST(totat_traffic_mb_fs_std AS FLOAT64)                       AS totat_traffic_mb_fs_std
,CAST(traffic_4g_mb_t_max AS FLOAT64)                           AS traffic_4g_mb_t_max
,CAST(totat_traffic_mb_sh4_std AS FLOAT64)                      AS totat_traffic_mb_sh4_std
,CAST(traffic_4g_mb_fs_std AS FLOAT64)                          AS traffic_4g_mb_fs_std
,CAST(downlink_mb_fs_std AS FLOAT64)                            AS downlink_mb_fs_std
,CAST(traffic_4g_mb_s_max AS FLOAT64)                           AS traffic_4g_mb_s_max
,CAST(downlink_mb_t_std AS FLOAT64)                             AS downlink_mb_t_std
,CAST(traffic_4g_mb_sh4_max AS FLOAT64)                         AS traffic_4g_mb_sh4_max
,CAST(downlink_mb_sh4_max AS FLOAT64)                           AS downlink_mb_sh4_max
,CAST(downlink_mb_t_avg AS FLOAT64)                             AS downlink_mb_t_avg
,CAST(downlink_mb_fs_avg AS FLOAT64)                            AS downlink_mb_fs_avg
,CAST(uplink_mb_t_avg AS FLOAT64)                               AS uplink_mb_t_avg
,CAST(traffic_4g_mb_s_avg AS FLOAT64)                           AS traffic_4g_mb_s_avg
,CAST(downlink_mb_s_avg AS FLOAT64)                             AS downlink_mb_s_avg
,CAST(traffic_4g_mb_sh4_avg AS FLOAT64)                         AS traffic_4g_mb_sh4_avg
,CAST(totat_traffic_mb_s_avg AS FLOAT64)                        AS totat_traffic_mb_s_avg
,CAST(totat_traffic_mb_t_avg AS FLOAT64)                        AS totat_traffic_mb_t_avg
,CAST(traffic_4g_mb_fs_min AS FLOAT64)                          AS traffic_4g_mb_fs_min
,CAST(totat_traffic_mb_s_std AS FLOAT64)                        AS totat_traffic_mb_s_std
,CAST(totat_traffic_mb_fs_avg AS FLOAT64)                       AS totat_traffic_mb_fs_avg
,CAST(downlink_mb_sh4_avg AS FLOAT64)                           AS downlink_mb_sh4_avg
,CAST(traffic_3g_mb_sh4_max AS FLOAT64)                         AS traffic_3g_mb_sh4_max
,CAST(traffic_4g_mb_s_min AS FLOAT64)                           AS traffic_4g_mb_s_min
,CAST(traffic_4g_mb_fs_avg AS FLOAT64)                          AS traffic_4g_mb_fs_avg
,CAST(totat_traffic_mb_sh4_avg AS FLOAT64)                      AS totat_traffic_mb_sh4_avg
,CAST(traffic_3g_mb_fs_std AS FLOAT64)                          AS traffic_3g_mb_fs_std
,CAST(traffic_4g_mb_t_std AS FLOAT64)                           AS traffic_4g_mb_t_std
,CAST(totat_traffic_mb_t_std AS FLOAT64)                        AS totat_traffic_mb_t_std
,CAST(traffic_3g_mb_fs_avg AS FLOAT64)                          AS traffic_3g_mb_fs_avg
,CAST(traffic_3g_mb_s_std AS FLOAT64)                           AS traffic_3g_mb_s_std
,CAST(traffic_3g_mb_fs_max AS FLOAT64)                          AS traffic_3g_mb_fs_max
,CAST(traffic_4g_mb_t_avg AS FLOAT64)                           AS traffic_4g_mb_t_avg
,CAST(traffic_3g_mb_s_avg AS FLOAT64)                           AS traffic_3g_mb_s_avg
,CAST(downlink_mb_fs_min AS FLOAT64)                            AS downlink_mb_fs_min
,CAST(totat_traffic_mb_fs_min AS FLOAT64)                       AS totat_traffic_mb_fs_min
,CAST(traffic_3g_mb_t_max AS FLOAT64)                           AS traffic_3g_mb_t_max
,CAST(uplink_mb_t_min AS FLOAT64)                               AS uplink_mb_t_min
,CAST(traffic_4g_mb_t_min AS FLOAT64)                           AS traffic_4g_mb_t_min
,CAST(totat_traffic_mb_s_min AS FLOAT64)                        AS totat_traffic_mb_s_min
,CAST(traffic_3g_mb_s_max AS FLOAT64)                           AS traffic_3g_mb_s_max
,CAST(downlink_mb_s_min AS FLOAT64)                             AS downlink_mb_s_min
,CAST(traffic_3g_mb_t_avg AS FLOAT64)                           AS traffic_3g_mb_t_avg
,CAST(traffic_3g_mb_sh4_std AS FLOAT64)                         AS traffic_3g_mb_sh4_std
,CAST(totat_traffic_mb_t_min AS FLOAT64)                        AS totat_traffic_mb_t_min
,CAST(uplink_mb_s_min AS FLOAT64)                               AS uplink_mb_s_min
,CAST(traffic_3g_mb_t_std AS FLOAT64)                           AS traffic_3g_mb_t_std 
,A.periodo
FROM (
    SELECT DISTINCT linea, periodo
    FROM `teco-prod-datalake-8f6a.vw_repo_convergente.vc_parque_movil_unificado_aa` where periodo = M_ABT 
    and subgrupo = 'Prepago') A
LEFT JOIN(
     SELECT linea,
            ofertas_all_u6_pagos_sum,
            ofertas_all_u5_pagos_sum,
            recargas_mdp_pagoelectronico_u6_pagos_cumsum,
            recargas_mdp_pagoelectronico_u5_pagos_cumsum,
            ofertas_all_u4_pagos_max,
            ofertas_all_u6_vigencia1_std,
            ofertas_all_u4_pagos_sum,
            ofertas_all_u3_pagos_avg,
            ofertas_all_u3_pagos_max,
            ofertas_all_u2_pagos_avg,
            recargas_rec_u1u2_pagos_ratio,
            ofertas_all_u6_vigencia1_avg,
            ofertas_all_u1_pagos_avg,
            recargas_rec_u1u3_pagos_diff,
            ofertas_all_u2_pagos_sum,
            recargas_rec_u1_arbu,
            recargas_rec_u1u6_pagos_diff,
            recargas_rec_u1u5_pagos_diff,
            recargas_rec_u3_pagos_sum,
            recargas_mdp_pagoelectronico_u1_pagos_sum,
            recargas_rec_u2_arbu,
            recargas_rec_u2_pagos_cumsum,
            recargas_mdp_pagoelectronico_u3_recargas_count,
            recargas_rec_u1u2_pagos_diff,
            recargas_rec_u5_arbu,
            recargas_mdp_pagoelectronico_u2_recargas_count,
            ofertas_all_u5_vigencia1_avg,
            ofertas_all_u4_vigencia1_std,
            recargas_rec_u4_arbu,
            recargas_mdp_pagoelectronico_u2_pagos_sum,
            recargas_rec_u6_pagos_sum,
            recargas_rec_u4_pagos_sum,
            recargas_mdp_pagoelectronico_u3_pagos_sum,
            recargas_rec_u5_pagos_cumsum,
            recargas_rec_u3_pagos_cumsum,
            recargas_rec_u4_pagos_cumsum,
            recargas_mdp_pagoelectronico_u4_pagos_sum,
            recargas_rec_u6_arbu,
            recargas_rec_u3_arbu,
            ofertas_all_u4_vigencia1_avg,
            recargas_rec_u6_pagos_cumsum,
            recargas_mdp_pagoelectronico_u4_recargas_count,
            recargas_mdp_pagoelectronico_u6_recargas_count,
            ofertas_all_u3_vigencia1_avg
    FROM `teco-prod-adam-85cc.data_lake_analytics.ft_prepago_m` where periodo >0 and periodo = M_PREPAGO) B
ON (CAST(A.linea AS INT64) = B.linea)
LEFT JOIN (
    SELECT  linea,
            uplink_mb_t_max,
            uplink_mb_sh4_std,
            uplink_mb_t_std,
            uplink_mb_s_std,
            downlink_mb_fs_max,
            uplink_mb_s_max,
            totat_traffic_mb_fs_max,
            totat_traffic_mb_s_max,
            uplink_mb_sh4_max,
            downlink_mb_sh4_std,
            uplink_mb_s_avg,
            traffic_4g_mb_sh4_std,
            downlink_mb_s_max,
            traffic_4g_mb_fs_max,
            uplink_mb_sh4_avg,
            totat_traffic_mb_t_max,
            totat_traffic_mb_sh4_max,
            downlink_mb_t_max,
            downlink_mb_s_std,
            totat_traffic_mb_fs_std,
            traffic_4g_mb_t_max,
            totat_traffic_mb_sh4_std,
            traffic_4g_mb_fs_std,
            downlink_mb_fs_std,
            traffic_4g_mb_s_max,
            downlink_mb_t_std,
            traffic_4g_mb_sh4_max,
            downlink_mb_sh4_max,
            downlink_mb_t_avg,
            downlink_mb_fs_avg,
            uplink_mb_t_avg,
            traffic_4g_mb_s_avg,
            downlink_mb_s_avg,
            traffic_4g_mb_sh4_avg,
            totat_traffic_mb_s_avg,
            totat_traffic_mb_t_avg,
            traffic_4g_mb_fs_min,
            totat_traffic_mb_s_std,
            totat_traffic_mb_fs_avg,
            downlink_mb_sh4_avg,
            traffic_3g_mb_sh4_max,
            traffic_4g_mb_s_min,
            traffic_4g_mb_fs_avg,
            totat_traffic_mb_sh4_avg,
            traffic_3g_mb_fs_std,
            traffic_4g_mb_t_std,
            totat_traffic_mb_t_std,
            traffic_3g_mb_fs_avg,
            traffic_3g_mb_s_std,
            traffic_3g_mb_fs_max,
            traffic_4g_mb_t_avg,
            traffic_3g_mb_s_avg,
            downlink_mb_fs_min,
            totat_traffic_mb_fs_min,
            traffic_3g_mb_t_max,
            uplink_mb_t_min,
            traffic_4g_mb_t_min,
            totat_traffic_mb_s_min,
            traffic_3g_mb_s_max,
            downlink_mb_s_min,
            traffic_3g_mb_t_avg,
            traffic_3g_mb_sh4_std,
            totat_traffic_mb_t_min,
            uplink_mb_s_min,
            traffic_3g_mb_t_std
    FROM `teco-prod-adam-85cc.data_lake_analytics.stg_traficodatos_m` where periodo >0 and periodo = M_TRAFICO) D
ON (A.linea  = SUBSTR(CAST(D.linea AS STRING), 3) and SUBSTR(CAST(D.linea AS STRING), 0,2) = '54')
;
---- Sanity check a los nulos los reemplazamos con 0 salvo las variables ratio (recargas_rec_u1u2_pagos_ratio) que van con 1

UPDATE `{{ PROJECT_ID }}.{{ DATASET }}.tmp_capro_imp`
SET 

ofertas_all_u6_pagos_sum= CASE WHEN IFNULL( ofertas_all_u6_pagos_sum, 0) = 0 THEN 0 ELSE ofertas_all_u6_pagos_sum END
,ofertas_all_u5_pagos_sum= CASE WHEN IFNULL( ofertas_all_u5_pagos_sum, 0) = 0 THEN 0 ELSE ofertas_all_u5_pagos_sum END
,recargas_mdp_pagoelectronico_u6_pagos_cumsum= CASE WHEN IFNULL( recargas_mdp_pagoelectronico_u6_pagos_cumsum, 0) = 0 THEN 0 ELSE recargas_mdp_pagoelectronico_u6_pagos_cumsum END
,recargas_mdp_pagoelectronico_u5_pagos_cumsum= CASE WHEN IFNULL( recargas_mdp_pagoelectronico_u5_pagos_cumsum, 0) = 0 THEN 0 ELSE recargas_mdp_pagoelectronico_u5_pagos_cumsum END
,ofertas_all_u4_pagos_max   = CASE WHEN IFNULL( ofertas_all_u4_pagos_max, 0) = 0 THEN 0 ELSE ofertas_all_u4_pagos_max END
,ofertas_all_u6_vigencia1_std= CASE WHEN IFNULL( ofertas_all_u6_vigencia1_std, 0) = 0 OR IS_NAN(ofertas_all_u6_vigencia1_std) THEN 0 ELSE ofertas_all_u6_vigencia1_std END


,ofertas_all_u4_pagos_sum= CASE WHEN IFNULL( ofertas_all_u4_pagos_sum, 0) = 0 THEN 0 ELSE ofertas_all_u4_pagos_sum END
,ofertas_all_u3_pagos_avg= CASE WHEN IFNULL( ofertas_all_u3_pagos_avg, 0) = 0 THEN 0 ELSE ofertas_all_u3_pagos_avg END
,ofertas_all_u3_pagos_max= CASE WHEN IFNULL( ofertas_all_u3_pagos_max, 0) = 0 THEN 0 ELSE ofertas_all_u3_pagos_max END
,uplink_mb_t_max= CASE WHEN IFNULL( uplink_mb_t_max, 0) = 0 THEN 0 ELSE uplink_mb_t_max END
,ofertas_all_u2_pagos_avg= CASE WHEN IFNULL( ofertas_all_u2_pagos_avg, 0) = 0 THEN 0 ELSE ofertas_all_u2_pagos_avg END
,recargas_rec_u1u2_pagos_ratio= CASE WHEN IFNULL( recargas_rec_u1u2_pagos_ratio, 0) = 0 OR IS_NAN(recargas_rec_u1u2_pagos_ratio)  THEN 1 ELSE recargas_rec_u1u2_pagos_ratio END
,ofertas_all_u6_vigencia1_avg= CASE WHEN IFNULL( ofertas_all_u6_vigencia1_avg, 0) = 0 THEN 0 ELSE ofertas_all_u6_vigencia1_avg END
,uplink_mb_sh4_std= CASE WHEN IFNULL( uplink_mb_sh4_std, 0) = 0 OR IS_NAN(uplink_mb_sh4_std) THEN 0 ELSE uplink_mb_sh4_std END
,ofertas_all_u1_pagos_avg= CASE WHEN IFNULL( ofertas_all_u1_pagos_avg, 0) = 0 THEN 0 ELSE ofertas_all_u1_pagos_avg END
,recargas_rec_u1u3_pagos_diff= CASE WHEN IFNULL( recargas_rec_u1u3_pagos_diff, 0) = 0 THEN 0 ELSE recargas_rec_u1u3_pagos_diff END
,ofertas_all_u2_pagos_sum= CASE WHEN IFNULL( ofertas_all_u2_pagos_sum, 0) = 0 THEN 0 ELSE ofertas_all_u2_pagos_sum END
,recargas_rec_u1_arbu= CASE WHEN IFNULL( recargas_rec_u1_arbu, 0) = 0 THEN 0 ELSE recargas_rec_u1_arbu END
,recargas_rec_u1u6_pagos_diff= CASE WHEN IFNULL( recargas_rec_u1u6_pagos_diff, 0) = 0 THEN 0 ELSE recargas_rec_u1u6_pagos_diff END
,uplink_mb_t_std= CASE WHEN IFNULL( uplink_mb_t_std, 0) = 0 OR IS_NAN(uplink_mb_t_std) THEN 0 ELSE uplink_mb_t_std END
,recargas_rec_u1u5_pagos_diff= CASE WHEN IFNULL( recargas_rec_u1u5_pagos_diff, 0) = 0 THEN 0 ELSE recargas_rec_u1u5_pagos_diff END
,uplink_mb_s_std= CASE WHEN IFNULL( uplink_mb_s_std, 0) = 0 OR IS_NAN(uplink_mb_s_std) THEN 0 ELSE uplink_mb_s_std END
,downlink_mb_fs_max= CASE WHEN IFNULL( downlink_mb_fs_max, 0) = 0 THEN 0 ELSE downlink_mb_fs_max END
,uplink_mb_s_max= CASE WHEN IFNULL( uplink_mb_s_max, 0) = 0 THEN 0 ELSE uplink_mb_s_max END
,recargas_rec_u3_pagos_sum= CASE WHEN IFNULL( recargas_rec_u3_pagos_sum, 0) = 0 THEN 0 ELSE recargas_rec_u3_pagos_sum END
,recargas_mdp_pagoelectronico_u1_pagos_sum= CASE WHEN IFNULL( recargas_mdp_pagoelectronico_u1_pagos_sum, 0) = 0 THEN 0 ELSE recargas_mdp_pagoelectronico_u1_pagos_sum END
,totat_traffic_mb_fs_max= CASE WHEN IFNULL( totat_traffic_mb_fs_max, 0) = 0 THEN 0 ELSE totat_traffic_mb_fs_max END
,recargas_rec_u2_arbu= CASE WHEN IFNULL( recargas_rec_u2_arbu, 0) = 0 THEN 0 ELSE recargas_rec_u2_arbu END
,totat_traffic_mb_s_max= CASE WHEN IFNULL( totat_traffic_mb_s_max, 0) = 0 THEN 0 ELSE totat_traffic_mb_s_max END
,uplink_mb_sh4_max= CASE WHEN IFNULL( uplink_mb_sh4_max, 0) = 0 THEN 0 ELSE uplink_mb_sh4_max END
,downlink_mb_sh4_std= CASE WHEN IFNULL( downlink_mb_sh4_std, 0) = 0 OR IS_NAN(downlink_mb_sh4_std) THEN 0 ELSE downlink_mb_sh4_std END
,recargas_rec_u2_pagos_cumsum= CASE WHEN IFNULL( recargas_rec_u2_pagos_cumsum, 0) = 0 THEN 0 ELSE recargas_rec_u2_pagos_cumsum END
,recargas_mdp_pagoelectronico_u3_recargas_count= CASE WHEN IFNULL( recargas_mdp_pagoelectronico_u3_recargas_count, 0) = 0 THEN 0 ELSE recargas_mdp_pagoelectronico_u3_recargas_count END
,uplink_mb_s_avg= CASE WHEN IFNULL( uplink_mb_s_avg, 0) = 0 THEN 0 ELSE uplink_mb_s_avg END
,traffic_4g_mb_sh4_std= CASE WHEN IFNULL( traffic_4g_mb_sh4_std, 0) = 0 OR IS_NAN(traffic_4g_mb_sh4_std) THEN 0 ELSE traffic_4g_mb_sh4_std END
,downlink_mb_s_max= CASE WHEN IFNULL( downlink_mb_s_max, 0) = 0 THEN 0 ELSE downlink_mb_s_max END
,recargas_rec_u1u2_pagos_diff= CASE WHEN IFNULL( recargas_rec_u1u2_pagos_diff, 0) = 0 THEN 0 ELSE recargas_rec_u1u2_pagos_diff END
,traffic_4g_mb_fs_max= CASE WHEN IFNULL( traffic_4g_mb_fs_max, 0) = 0 THEN 0 ELSE traffic_4g_mb_fs_max END
,recargas_rec_u5_arbu= CASE WHEN IFNULL( recargas_rec_u5_arbu, 0) = 0 THEN 0 ELSE recargas_rec_u5_arbu END
,uplink_mb_sh4_avg= CASE WHEN IFNULL( uplink_mb_sh4_avg, 0) = 0 THEN 0 ELSE uplink_mb_sh4_avg END
,totat_traffic_mb_t_max= CASE WHEN IFNULL( totat_traffic_mb_t_max, 0) = 0 THEN 0 ELSE totat_traffic_mb_t_max END
,totat_traffic_mb_sh4_max= CASE WHEN IFNULL( totat_traffic_mb_sh4_max, 0) = 0 THEN 0 ELSE totat_traffic_mb_sh4_max END
,downlink_mb_t_max= CASE WHEN IFNULL( downlink_mb_t_max, 0) = 0 THEN 0 ELSE downlink_mb_t_max END
,recargas_mdp_pagoelectronico_u2_recargas_count= CASE WHEN IFNULL( recargas_mdp_pagoelectronico_u2_recargas_count, 0) = 0 THEN 0 ELSE recargas_mdp_pagoelectronico_u2_recargas_count END
,ofertas_all_u5_vigencia1_avg= CASE WHEN IFNULL( ofertas_all_u5_vigencia1_avg, 0) = 0 THEN 0 ELSE ofertas_all_u5_vigencia1_avg END
,ofertas_all_u4_vigencia1_std= CASE WHEN IFNULL( ofertas_all_u4_vigencia1_std, 0) = 0 OR IS_NAN(ofertas_all_u4_vigencia1_std) THEN 0 ELSE ofertas_all_u4_vigencia1_std END
,recargas_rec_u4_arbu= CASE WHEN IFNULL( recargas_rec_u4_arbu, 0) = 0 THEN 0 ELSE recargas_rec_u4_arbu END
,recargas_mdp_pagoelectronico_u2_pagos_sum= CASE WHEN IFNULL( recargas_mdp_pagoelectronico_u2_pagos_sum, 0) = 0 THEN 0 ELSE recargas_mdp_pagoelectronico_u2_pagos_sum END
,recargas_rec_u6_pagos_sum= CASE WHEN IFNULL( recargas_rec_u6_pagos_sum, 0) = 0 THEN 0 ELSE recargas_rec_u6_pagos_sum END
,downlink_mb_s_std= CASE WHEN IFNULL( downlink_mb_s_std, 0) = 0 OR IS_NAN(downlink_mb_s_std) THEN 0 ELSE downlink_mb_s_std END
,recargas_rec_u4_pagos_sum= CASE WHEN IFNULL( recargas_rec_u4_pagos_sum, 0) = 0 THEN 0 ELSE recargas_rec_u4_pagos_sum END
,totat_traffic_mb_fs_std= CASE WHEN IFNULL( totat_traffic_mb_fs_std, 0) = 0 OR IS_NAN(totat_traffic_mb_fs_std)  THEN 0 ELSE totat_traffic_mb_fs_std END
,recargas_mdp_pagoelectronico_u3_pagos_sum= CASE WHEN IFNULL( recargas_mdp_pagoelectronico_u3_pagos_sum, 0) = 0 THEN 0 ELSE recargas_mdp_pagoelectronico_u3_pagos_sum END
,traffic_4g_mb_t_max= CASE WHEN IFNULL( traffic_4g_mb_t_max, 0) = 0 THEN 0 ELSE traffic_4g_mb_t_max END
,totat_traffic_mb_sh4_std= CASE WHEN IFNULL( totat_traffic_mb_sh4_std, 0) = 0 OR IS_NAN(totat_traffic_mb_sh4_std)  THEN 0 ELSE totat_traffic_mb_sh4_std END
,traffic_4g_mb_fs_std= CASE WHEN IFNULL( traffic_4g_mb_fs_std, 0) = 0 OR IS_NAN(traffic_4g_mb_fs_std)  THEN 0 ELSE traffic_4g_mb_fs_std END
,downlink_mb_fs_std= CASE WHEN IFNULL( downlink_mb_fs_std, 0) = 0 OR IS_NAN(downlink_mb_fs_std)  THEN 0 ELSE downlink_mb_fs_std END
,recargas_rec_u5_pagos_cumsum= CASE WHEN IFNULL( recargas_rec_u5_pagos_cumsum, 0) = 0 THEN 0 ELSE recargas_rec_u5_pagos_cumsum END
,recargas_rec_u3_pagos_cumsum= CASE WHEN IFNULL( recargas_rec_u3_pagos_cumsum, 0) = 0 THEN 0 ELSE recargas_rec_u3_pagos_cumsum END
,traffic_4g_mb_s_max= CASE WHEN IFNULL( traffic_4g_mb_s_max, 0) = 0 THEN 0 ELSE traffic_4g_mb_s_max END
,downlink_mb_t_std= CASE WHEN IFNULL( downlink_mb_t_std, 0) = 0 OR IS_NAN(downlink_mb_t_std)  THEN 0 ELSE downlink_mb_t_std END
,traffic_4g_mb_sh4_max= CASE WHEN IFNULL( traffic_4g_mb_sh4_max, 0) = 0 THEN 0 ELSE traffic_4g_mb_sh4_max END
,downlink_mb_sh4_max= CASE WHEN IFNULL( downlink_mb_sh4_max, 0) = 0 THEN 0 ELSE downlink_mb_sh4_max END
,downlink_mb_t_avg= CASE WHEN IFNULL( downlink_mb_t_avg, 0) = 0 THEN 0 ELSE downlink_mb_t_avg END
,downlink_mb_fs_avg= CASE WHEN IFNULL( downlink_mb_fs_avg, 0) = 0 THEN 0 ELSE downlink_mb_fs_avg END
,uplink_mb_t_avg= CASE WHEN IFNULL( uplink_mb_t_avg, 0) = 0 THEN 0 ELSE uplink_mb_t_avg END
,traffic_4g_mb_s_avg= CASE WHEN IFNULL( traffic_4g_mb_s_avg, 0) = 0 THEN 0 ELSE traffic_4g_mb_s_avg END
,downlink_mb_s_avg= CASE WHEN IFNULL( downlink_mb_s_avg, 0) = 0 THEN 0 ELSE downlink_mb_s_avg END
,recargas_rec_u4_pagos_cumsum= CASE WHEN IFNULL( recargas_rec_u4_pagos_cumsum, 0) = 0 THEN 0 ELSE recargas_rec_u4_pagos_cumsum END
,traffic_4g_mb_sh4_avg= CASE WHEN IFNULL( traffic_4g_mb_sh4_avg, 0) = 0 THEN 0 ELSE traffic_4g_mb_sh4_avg END
,totat_traffic_mb_s_avg= CASE WHEN IFNULL( totat_traffic_mb_s_avg, 0) = 0 THEN 0 ELSE totat_traffic_mb_s_avg END
,recargas_mdp_pagoelectronico_u4_pagos_sum= CASE WHEN IFNULL( recargas_mdp_pagoelectronico_u4_pagos_sum, 0) = 0 THEN 0 ELSE recargas_mdp_pagoelectronico_u4_pagos_sum END
,totat_traffic_mb_t_avg= CASE WHEN IFNULL( totat_traffic_mb_t_avg, 0) = 0 THEN 0 ELSE totat_traffic_mb_t_avg END
,traffic_4g_mb_fs_min= CASE WHEN IFNULL( traffic_4g_mb_fs_min, 0) = 0 THEN 0 ELSE traffic_4g_mb_fs_min END
,totat_traffic_mb_s_std= CASE WHEN IFNULL( totat_traffic_mb_s_std, 0) = 0 OR IS_NAN(totat_traffic_mb_s_std)  THEN 0 ELSE totat_traffic_mb_s_std END
,totat_traffic_mb_fs_avg= CASE WHEN IFNULL( totat_traffic_mb_fs_avg, 0) = 0 THEN 0 ELSE totat_traffic_mb_fs_avg END
,recargas_rec_u6_arbu= CASE WHEN IFNULL( recargas_rec_u6_arbu, 0) = 0 THEN 0 ELSE recargas_rec_u6_arbu END
,downlink_mb_sh4_avg= CASE WHEN IFNULL( downlink_mb_sh4_avg, 0) = 0 THEN 0 ELSE downlink_mb_sh4_avg END
,traffic_3g_mb_sh4_max= CASE WHEN IFNULL( traffic_3g_mb_sh4_max, 0) = 0 THEN 0 ELSE traffic_3g_mb_sh4_max END
,traffic_4g_mb_s_min= CASE WHEN IFNULL( traffic_4g_mb_s_min, 0) = 0 THEN 0 ELSE traffic_4g_mb_s_min END
,traffic_4g_mb_fs_avg= CASE WHEN IFNULL( traffic_4g_mb_fs_avg, 0) = 0 THEN 0 ELSE traffic_4g_mb_fs_avg END
,totat_traffic_mb_sh4_avg= CASE WHEN IFNULL( totat_traffic_mb_sh4_avg, 0) = 0 THEN 0 ELSE totat_traffic_mb_sh4_avg END
,recargas_rec_u3_arbu= CASE WHEN IFNULL( recargas_rec_u3_arbu, 0) = 0 THEN 0 ELSE recargas_rec_u3_arbu END
,ofertas_all_u4_vigencia1_avg= CASE WHEN IFNULL( ofertas_all_u4_vigencia1_avg, 0) = 0 THEN 0 ELSE ofertas_all_u4_vigencia1_avg END
,traffic_3g_mb_fs_std= CASE WHEN IFNULL( traffic_3g_mb_fs_std, 0) = 0 OR IS_NAN(traffic_3g_mb_fs_std)  THEN 0 ELSE traffic_3g_mb_fs_std END
,traffic_4g_mb_t_std= CASE WHEN IFNULL( traffic_4g_mb_t_std, 0) = 0 OR IS_NAN(traffic_4g_mb_t_std)     THEN 0 ELSE traffic_4g_mb_t_std END
,totat_traffic_mb_t_std= CASE WHEN IFNULL( totat_traffic_mb_t_std, 0) = 0 OR IS_NAN(totat_traffic_mb_t_std)  THEN 0 ELSE totat_traffic_mb_t_std END
,traffic_3g_mb_fs_avg= CASE WHEN IFNULL( traffic_3g_mb_fs_avg, 0) = 0 THEN 0 ELSE traffic_3g_mb_fs_avg END
,traffic_3g_mb_s_std= CASE WHEN IFNULL( traffic_3g_mb_s_std, 0) = 0 OR IS_NAN(traffic_3g_mb_s_std)  THEN 0 ELSE traffic_3g_mb_s_std END
,recargas_rec_u6_pagos_cumsum= CASE WHEN IFNULL( recargas_rec_u6_pagos_cumsum, 0) = 0 THEN 0 ELSE recargas_rec_u6_pagos_cumsum END
,traffic_3g_mb_fs_max= CASE WHEN IFNULL( traffic_3g_mb_fs_max, 0) = 0 THEN 0 ELSE traffic_3g_mb_fs_max END
,recargas_mdp_pagoelectronico_u4_recargas_count= CASE WHEN IFNULL( recargas_mdp_pagoelectronico_u4_recargas_count, 0) = 0 THEN 0 ELSE recargas_mdp_pagoelectronico_u4_recargas_count END
,traffic_4g_mb_t_avg= CASE WHEN IFNULL( traffic_4g_mb_t_avg, 0) = 0 THEN 0 ELSE traffic_4g_mb_t_avg END
,traffic_3g_mb_s_avg= CASE WHEN IFNULL( traffic_3g_mb_s_avg, 0) = 0 THEN 0 ELSE traffic_3g_mb_s_avg END
,downlink_mb_fs_min= CASE WHEN IFNULL( downlink_mb_fs_min, 0) = 0 THEN 0 ELSE downlink_mb_fs_min END
,totat_traffic_mb_fs_min= CASE WHEN IFNULL( totat_traffic_mb_fs_min, 0) = 0 THEN 0 ELSE totat_traffic_mb_fs_min END
,recargas_mdp_pagoelectronico_u6_recargas_count= CASE WHEN IFNULL( recargas_mdp_pagoelectronico_u6_recargas_count, 0) = 0 THEN 0 ELSE recargas_mdp_pagoelectronico_u6_recargas_count END
,traffic_3g_mb_t_max= CASE WHEN IFNULL( traffic_3g_mb_t_max, 0) = 0 THEN 0 ELSE traffic_3g_mb_t_max END
,uplink_mb_t_min= CASE WHEN IFNULL( uplink_mb_t_min, 0) = 0 THEN 0 ELSE uplink_mb_t_min END
,traffic_4g_mb_t_min= CASE WHEN IFNULL( traffic_4g_mb_t_min, 0) = 0 THEN 0 ELSE traffic_4g_mb_t_min END
,totat_traffic_mb_s_min= CASE WHEN IFNULL( totat_traffic_mb_s_min, 0) = 0 THEN 0 ELSE totat_traffic_mb_s_min END
,traffic_3g_mb_s_max= CASE WHEN IFNULL( traffic_3g_mb_s_max, 0) = 0 THEN 0 ELSE traffic_3g_mb_s_max END
,downlink_mb_s_min= CASE WHEN IFNULL( downlink_mb_s_min, 0) = 0 THEN 0 ELSE downlink_mb_s_min END
,traffic_3g_mb_t_avg= CASE WHEN IFNULL( traffic_3g_mb_t_avg, 0) = 0 THEN 0 ELSE traffic_3g_mb_t_avg END
,ofertas_all_u3_vigencia1_avg= CASE WHEN IFNULL( ofertas_all_u3_vigencia1_avg, 0) = 0 THEN 0 ELSE ofertas_all_u3_vigencia1_avg END
,traffic_3g_mb_sh4_std= CASE WHEN IFNULL( traffic_3g_mb_sh4_std, 0) = 0 OR IS_NAN(traffic_3g_mb_sh4_std) THEN 0 ELSE traffic_3g_mb_sh4_std END
,totat_traffic_mb_t_min= CASE WHEN IFNULL( totat_traffic_mb_t_min, 0) = 0 THEN 0 ELSE totat_traffic_mb_t_min END
,uplink_mb_s_min= CASE WHEN IFNULL( uplink_mb_s_min, 0) = 0 THEN 0 ELSE uplink_mb_s_min END
,traffic_3g_mb_t_std= CASE WHEN IFNULL( traffic_3g_mb_t_std, 0) = 0 OR IS_NAN(traffic_3g_mb_t_std)  THEN 0 ELSE traffic_3g_mb_t_std END
WHERE periodo = M_ABT;

--------------------------------------------------------------------
---- TRUNCAMIENTO DE OUTLIERS
--------------------------------------------------------------------

CREATE OR REPLACE TABLE `{{ PROJECT_ID }}.{{ DATASET }}.tmp_capro_trunc` AS
SELECT * FROM `{{ PROJECT_ID }}.{{ DATASET }}.tmp_capro_imp`
WHERE periodo = M_ABT;
-- ft_prepago_m
---- Capturamos percentiles G1
SET (P_99_v1,P_99_v2,P_99_v3,P_99_v4,P_99_v5,P_99_v6,P_99_v7,P_99_v8,P_99_v9,P_99_v10,P_99_v11,P_99_v12)
= (
SELECT AS STRUCT
PERCENTILE_CONT(ofertas_all_u6_pagos_sum,0.99 IGNORE NULLS) OVER() AS P_99_v1,
PERCENTILE_CONT(ofertas_all_u5_pagos_sum,0.99 IGNORE NULLS) OVER() AS P_99_v2,
PERCENTILE_CONT(recargas_mdp_pagoelectronico_u6_pagos_cumsum,0.99 IGNORE NULLS) OVER() AS P_99_v3,
PERCENTILE_CONT(recargas_mdp_pagoelectronico_u5_pagos_cumsum,0.99 IGNORE NULLS) OVER() AS P_99_v4,
PERCENTILE_CONT(ofertas_all_u4_pagos_max,0.99 IGNORE NULLS) OVER() AS P_99_v5,
PERCENTILE_CONT(ofertas_all_u6_vigencia1_std,0.99 IGNORE NULLS) OVER() AS P_99_v6,
PERCENTILE_CONT(ofertas_all_u4_pagos_sum,0.99 IGNORE NULLS) OVER() AS P_99_v7,
PERCENTILE_CONT(ofertas_all_u3_pagos_avg,0.99 IGNORE NULLS) OVER() AS P_99_v8,
PERCENTILE_CONT(ofertas_all_u3_pagos_max,0.99 IGNORE NULLS) OVER() AS P_99_v9,
PERCENTILE_CONT(ofertas_all_u2_pagos_avg,0.99 IGNORE NULLS) OVER() AS P_99_v10,
PERCENTILE_CONT(recargas_rec_u1u2_pagos_ratio,0.99 IGNORE NULLS) OVER() AS P_99_v11,
PERCENTILE_CONT(ofertas_all_u6_vigencia1_avg,0.99 IGNORE NULLS) OVER() AS P_99_v12
FROM `{{ PROJECT_ID }}.{{ DATASET }}.tmp_capro_trunc` 
WHERE periodo = M_ABT
LIMIT 1     
);  
---- Capturamos percentiles G2
SET (P_99_v13,P_99_v14,P_99_v15,P_99_v16,P_99_v17,P_99_v18,P_99_v19,P_99_v20,P_99_v21,P_99_v22,P_99_v23)
= (
SELECT AS STRUCT  
PERCENTILE_CONT(ofertas_all_u1_pagos_avg,0.99 IGNORE NULLS) OVER() AS P_99_v13,
PERCENTILE_CONT(recargas_rec_u1u3_pagos_diff,0.99 IGNORE NULLS) OVER() AS P_99_v14,
PERCENTILE_CONT(ofertas_all_u2_pagos_sum,0.99 IGNORE NULLS) OVER() AS P_99_v15,
PERCENTILE_CONT(recargas_rec_u1_arbu,0.99 IGNORE NULLS) OVER() AS P_99_v16,
PERCENTILE_CONT(recargas_rec_u1u6_pagos_diff,0.99 IGNORE NULLS) OVER() AS P_99_v17,
PERCENTILE_CONT(recargas_rec_u1u5_pagos_diff,0.99 IGNORE NULLS) OVER() AS P_99_v18,
PERCENTILE_CONT(recargas_rec_u3_pagos_sum,0.99 IGNORE NULLS) OVER() AS P_99_v19,
PERCENTILE_CONT(recargas_mdp_pagoelectronico_u1_pagos_sum,0.99 IGNORE NULLS) OVER() AS P_99_v20,
PERCENTILE_CONT(recargas_rec_u2_arbu,0.99 IGNORE NULLS) OVER() AS P_99_v21,
PERCENTILE_CONT(recargas_rec_u2_pagos_cumsum,0.99 IGNORE NULLS) OVER() AS P_99_v22,
PERCENTILE_CONT(recargas_mdp_pagoelectronico_u3_recargas_count,0.99 IGNORE NULLS) OVER() AS P_99_v23
FROM `{{ PROJECT_ID }}.{{ DATASET }}.tmp_capro_trunc` 
WHERE periodo = M_ABT
LIMIT 1     
);  
---- Capturamos percentiles G3
SET (P_99_v24,P_99_v25,P_99_v26,P_99_v27,P_99_v28,P_99_v29,P_99_v30,P_99_v31,P_99_v32,P_99_v33,P_99_v34)
= (
SELECT AS STRUCT
PERCENTILE_CONT(recargas_rec_u1u2_pagos_diff,0.99 IGNORE NULLS) OVER() AS P_99_v24,
PERCENTILE_CONT(recargas_rec_u5_arbu,0.99 IGNORE NULLS) OVER() AS P_99_v25,
PERCENTILE_CONT(recargas_mdp_pagoelectronico_u2_recargas_count,0.99 IGNORE NULLS) OVER() AS P_99_v26,
PERCENTILE_CONT(ofertas_all_u5_vigencia1_avg,0.99 IGNORE NULLS) OVER() AS P_99_v27,
PERCENTILE_CONT(ofertas_all_u4_vigencia1_std,0.99 IGNORE NULLS) OVER() AS P_99_v28,
PERCENTILE_CONT(recargas_rec_u4_arbu,0.99 IGNORE NULLS) OVER() AS P_99_v29,
PERCENTILE_CONT(recargas_mdp_pagoelectronico_u2_pagos_sum,0.99 IGNORE NULLS) OVER() AS P_99_v30,
PERCENTILE_CONT(recargas_rec_u6_pagos_sum,0.99 IGNORE NULLS) OVER() AS P_99_v31,
PERCENTILE_CONT(recargas_rec_u4_pagos_sum,0.99 IGNORE NULLS) OVER() AS P_99_v32,
PERCENTILE_CONT(recargas_mdp_pagoelectronico_u3_pagos_sum,0.99 IGNORE NULLS) OVER() AS P_99_v33,
PERCENTILE_CONT(recargas_rec_u5_pagos_cumsum,0.99 IGNORE NULLS) OVER() AS P_99_v34
FROM `{{ PROJECT_ID }}.{{ DATASET }}.tmp_capro_trunc` 
WHERE periodo = M_ABT
LIMIT 1        
);
---- Capturamos percentiles G4
SET (P_99_v35,P_99_v36,P_99_v37,P_99_v38,P_99_v39,P_99_v40,P_99_v41,P_99_v42,P_99_v43,P_99_v44)
= (
SELECT AS STRUCT
PERCENTILE_CONT(recargas_rec_u3_pagos_cumsum,0.99 IGNORE NULLS) OVER() AS P_99_v35,
PERCENTILE_CONT(recargas_rec_u4_pagos_cumsum,0.99 IGNORE NULLS) OVER() AS P_99_v36,
PERCENTILE_CONT(recargas_mdp_pagoelectronico_u4_pagos_sum,0.99 IGNORE NULLS) OVER() AS P_99_v37,
PERCENTILE_CONT(recargas_rec_u6_arbu,0.99 IGNORE NULLS) OVER() AS P_99_v38,
PERCENTILE_CONT(recargas_rec_u3_arbu,0.99 IGNORE NULLS) OVER() AS P_99_v39,
PERCENTILE_CONT(ofertas_all_u4_vigencia1_avg,0.99 IGNORE NULLS) OVER() AS P_99_v40,
PERCENTILE_CONT(recargas_rec_u6_pagos_cumsum,0.99 IGNORE NULLS) OVER() AS P_99_v41,
PERCENTILE_CONT(recargas_mdp_pagoelectronico_u4_recargas_count,0.99 IGNORE NULLS) OVER() AS P_99_v42,
PERCENTILE_CONT(recargas_mdp_pagoelectronico_u6_recargas_count,0.99 IGNORE NULLS) OVER() AS P_99_v43,
PERCENTILE_CONT(ofertas_all_u3_vigencia1_avg                   ,0.99 IGNORE NULLS) OVER() AS P_99_v44
FROM `{{ PROJECT_ID }}.{{ DATASET }}.tmp_capro_trunc` 
WHERE periodo = M_ABT
LIMIT 1        
);      
-- stg_traficodatos_m
---- Capturamos percentiles G1
SET (P_99_v1,P_99_v2,P_99_v3,P_99_v4,P_99_v5,P_99_v6,P_99_v7,P_99_v8,P_99_v9,P_99_v10,P_99_v11,P_99_v12)
= (
SELECT AS STRUCT   
PERCENTILE_CONT(uplink_mb_t_max,0.99 IGNORE NULLS) OVER() AS P_99_v1,
PERCENTILE_CONT(uplink_mb_sh4_std,0.99 IGNORE NULLS) OVER() AS P_99_v2,
PERCENTILE_CONT(uplink_mb_t_std,0.99 IGNORE NULLS) OVER() AS P_99_v3,
PERCENTILE_CONT(uplink_mb_s_std,0.99 IGNORE NULLS) OVER() AS P_99_v4,
PERCENTILE_CONT(downlink_mb_fs_max,0.99 IGNORE NULLS) OVER() AS P_99_v5,
PERCENTILE_CONT(uplink_mb_s_max,0.99 IGNORE NULLS) OVER() AS P_99_v6,
PERCENTILE_CONT(totat_traffic_mb_fs_max,0.99 IGNORE NULLS) OVER() AS P_99_v7,
PERCENTILE_CONT(totat_traffic_mb_s_max,0.99 IGNORE NULLS) OVER() AS P_99_v8,
PERCENTILE_CONT(uplink_mb_sh4_max,0.99 IGNORE NULLS) OVER() AS P_99_v9,
PERCENTILE_CONT(downlink_mb_sh4_std,0.99 IGNORE NULLS) OVER() AS P_99_v10,
PERCENTILE_CONT(uplink_mb_s_avg,0.99 IGNORE NULLS) OVER() AS P_99_v11,
PERCENTILE_CONT(traffic_4g_mb_sh4_std,0.99 IGNORE NULLS) OVER() AS P_99_v12
FROM `{{ PROJECT_ID }}.{{ DATASET }}.tmp_capro_trunc` 
WHERE periodo = M_ABT
LIMIT 1        
);    
---- Capturamos percentiles G2
SET (P_99_v13,P_99_v14,P_99_v15,P_99_v16,P_99_v17,P_99_v18,P_99_v19,P_99_v20,P_99_v21,P_99_v22,P_99_v23)
= (
SELECT AS STRUCT  
PERCENTILE_CONT(downlink_mb_s_max       ,0.99 IGNORE NULLS) OVER() AS P_99_v13,
PERCENTILE_CONT(traffic_4g_mb_fs_max,0.99 IGNORE NULLS) OVER() AS P_99_v14,
PERCENTILE_CONT(uplink_mb_sh4_avg,0.99 IGNORE NULLS) OVER() AS P_99_v15,
PERCENTILE_CONT(totat_traffic_mb_t_max,0.99 IGNORE NULLS) OVER() AS P_99_v16,
PERCENTILE_CONT(totat_traffic_mb_sh4_max,0.99 IGNORE NULLS) OVER() AS P_99_v17,
PERCENTILE_CONT(downlink_mb_t_max,0.99 IGNORE NULLS) OVER() AS P_99_v18,
PERCENTILE_CONT(downlink_mb_s_std,0.99 IGNORE NULLS) OVER() AS P_99_v19,
PERCENTILE_CONT(totat_traffic_mb_fs_std,0.99 IGNORE NULLS) OVER() AS P_99_v20,
PERCENTILE_CONT(traffic_4g_mb_t_max,0.99 IGNORE NULLS) OVER() AS P_99_v21,
PERCENTILE_CONT(totat_traffic_mb_sh4_std,0.99 IGNORE NULLS) OVER() AS P_99_v22,
PERCENTILE_CONT(traffic_4g_mb_fs_std,0.99 IGNORE NULLS) OVER() AS P_99_v23
FROM `{{ PROJECT_ID }}.{{ DATASET }}.tmp_capro_trunc` 
WHERE periodo = M_ABT
LIMIT 1        
);       
---- Capturamos percentiles G3
SET (P_99_v24,P_99_v25,P_99_v26,P_99_v27,P_99_v28,P_99_v29,P_99_v30,P_99_v31,P_99_v32,P_99_v33,P_99_v34)
= (
SELECT AS STRUCT 
PERCENTILE_CONT(downlink_mb_fs_std,0.99 IGNORE NULLS) OVER() AS P_99_v24,
PERCENTILE_CONT(traffic_4g_mb_s_max,0.99 IGNORE NULLS) OVER() AS P_99_v25,
PERCENTILE_CONT(downlink_mb_t_std,0.99 IGNORE NULLS) OVER() AS P_99_v26,
PERCENTILE_CONT(traffic_4g_mb_sh4_max,0.99 IGNORE NULLS) OVER() AS P_99_v27,
PERCENTILE_CONT(downlink_mb_sh4_max,0.99 IGNORE NULLS) OVER() AS P_99_v28,
PERCENTILE_CONT(downlink_mb_t_avg,0.99 IGNORE NULLS) OVER() AS P_99_v29,
PERCENTILE_CONT(downlink_mb_fs_avg,0.99 IGNORE NULLS) OVER() AS P_99_v30,
PERCENTILE_CONT(uplink_mb_t_avg,0.99 IGNORE NULLS) OVER() AS P_99_v31,
PERCENTILE_CONT(traffic_4g_mb_s_avg,0.99 IGNORE NULLS) OVER() AS P_99_v32,
PERCENTILE_CONT(downlink_mb_s_avg,0.99 IGNORE NULLS) OVER() AS P_99_v33,
PERCENTILE_CONT(traffic_4g_mb_sh4_avg,0.99 IGNORE NULLS) OVER() AS P_99_v34
FROM `{{ PROJECT_ID }}.{{ DATASET }}.tmp_capro_trunc` 
WHERE periodo = M_ABT
LIMIT 1        
);     
---- Capturamos percentiles G4
SET (P_99_v35,P_99_v36,P_99_v37,P_99_v38,P_99_v39,P_99_v40,P_99_v41,P_99_v42,P_99_v43,P_99_v44, P_99_v45)
= (
SELECT AS STRUCT  
PERCENTILE_CONT(totat_traffic_mb_s_avg,0.99 IGNORE NULLS) OVER() AS P_99_v35,
PERCENTILE_CONT(totat_traffic_mb_t_avg,0.99 IGNORE NULLS) OVER() AS P_99_v36,
PERCENTILE_CONT(traffic_4g_mb_fs_min,0.99 IGNORE NULLS) OVER() AS P_99_v37,
PERCENTILE_CONT(totat_traffic_mb_s_std,0.99 IGNORE NULLS) OVER() AS P_99_v38,
PERCENTILE_CONT(totat_traffic_mb_fs_avg,0.99 IGNORE NULLS) OVER() AS P_99_v39,
PERCENTILE_CONT(downlink_mb_sh4_avg,0.99 IGNORE NULLS) OVER() AS P_99_v40,
PERCENTILE_CONT(traffic_3g_mb_sh4_max,0.99 IGNORE NULLS) OVER() AS P_99_v41,
PERCENTILE_CONT(traffic_4g_mb_s_min,0.99 IGNORE NULLS) OVER() AS P_99_v42,
PERCENTILE_CONT(traffic_4g_mb_fs_avg,0.99 IGNORE NULLS) OVER() AS P_99_v43,
PERCENTILE_CONT(totat_traffic_mb_sh4_avg,0.99 IGNORE NULLS) OVER() AS P_99_v44,
PERCENTILE_CONT(traffic_3g_mb_fs_std,0.99 IGNORE NULLS) OVER() AS P_99_v45
FROM `{{ PROJECT_ID }}.{{ DATASET }}.tmp_capro_trunc` 
WHERE periodo = M_ABT
LIMIT 1        
);  
---- Capturamos percentiles G5
SET (P_99_v46,P_99_v47,P_99_v48,P_99_v49,P_99_v50,P_99_v51,P_99_v52,P_99_v53,P_99_v54,P_99_v55,P_99_v56)
= (
SELECT AS STRUCT
PERCENTILE_CONT(traffic_4g_mb_t_std,0.99 IGNORE NULLS) OVER() AS P_99_v46,
PERCENTILE_CONT(totat_traffic_mb_t_std,0.99 IGNORE NULLS) OVER() AS P_99_v47,
PERCENTILE_CONT(traffic_3g_mb_fs_avg,0.99 IGNORE NULLS) OVER() AS P_99_v48,
PERCENTILE_CONT(traffic_3g_mb_s_std,0.99 IGNORE NULLS) OVER() AS P_99_v49,
PERCENTILE_CONT(traffic_3g_mb_fs_max,0.99 IGNORE NULLS) OVER() AS P_99_v50,
PERCENTILE_CONT(traffic_4g_mb_t_avg,0.99 IGNORE NULLS) OVER() AS P_99_v51,
PERCENTILE_CONT(traffic_3g_mb_s_avg,0.99 IGNORE NULLS) OVER() AS P_99_v52,
PERCENTILE_CONT(downlink_mb_fs_min,0.99 IGNORE NULLS) OVER() AS P_99_v53,
PERCENTILE_CONT(totat_traffic_mb_fs_min,0.99 IGNORE NULLS) OVER() AS P_99_v54,
PERCENTILE_CONT(traffic_3g_mb_t_max,0.99 IGNORE NULLS) OVER() AS P_99_v55,
PERCENTILE_CONT(uplink_mb_t_min,0.99 IGNORE NULLS) OVER() AS P_99_v56
FROM `{{ PROJECT_ID }}.{{ DATASET }}.tmp_capro_trunc` 
WHERE periodo = M_ABT
LIMIT 1        
);  
---- Capturamos percentiles G6
SET(P_99_v57,P_99_v58,P_99_v59,P_99_v60,P_99_v61,P_99_v62,P_99_v63,P_99_v64,P_99_v65)
=(
SELECT AS STRUCT     
PERCENTILE_CONT(traffic_4g_mb_t_min,0.99 IGNORE NULLS) OVER() AS P_99_v57,
PERCENTILE_CONT(totat_traffic_mb_s_min,0.99 IGNORE NULLS) OVER() AS P_99_v58,
PERCENTILE_CONT(traffic_3g_mb_s_max,0.99 IGNORE NULLS) OVER() AS P_99_v59,
PERCENTILE_CONT(downlink_mb_s_min,0.99 IGNORE NULLS) OVER() AS P_99_v60,
PERCENTILE_CONT(traffic_3g_mb_t_avg,0.99 IGNORE NULLS) OVER() AS P_99_v61,
PERCENTILE_CONT(traffic_3g_mb_sh4_std,0.99 IGNORE NULLS) OVER() AS P_99_v62,
PERCENTILE_CONT(totat_traffic_mb_t_min,0.99 IGNORE NULLS) OVER() AS P_99_v63,
PERCENTILE_CONT(uplink_mb_s_min,0.99 IGNORE NULLS) OVER() AS P_99_v64,
PERCENTILE_CONT(traffic_3g_mb_t_std     ,0.99 IGNORE NULLS) OVER() AS P_99_v65
FROM `{{ PROJECT_ID }}.{{ DATASET }}.tmp_capro_trunc` 
WHERE periodo = M_ABT
LIMIT 1        
);  

---- Truncamiento de variables
UPDATE `{{ PROJECT_ID }}.{{ DATASET }}.tmp_capro_trunc`
SET 
-- ft_prepago_m
ofertas_all_u6_pagos_sum = CASE WHEN ofertas_all_u6_pagos_sum > P_99_V1 THEN P_99_V1 ELSE ofertas_all_u6_pagos_sum END,
ofertas_all_u5_pagos_sum = CASE WHEN ofertas_all_u5_pagos_sum > P_99_V2 THEN P_99_V2 ELSE ofertas_all_u5_pagos_sum END,
recargas_mdp_pagoelectronico_u6_pagos_cumsum = CASE WHEN recargas_mdp_pagoelectronico_u6_pagos_cumsum > P_99_V3 THEN P_99_V3 ELSE recargas_mdp_pagoelectronico_u6_pagos_cumsum END,
recargas_mdp_pagoelectronico_u5_pagos_cumsum = CASE WHEN recargas_mdp_pagoelectronico_u5_pagos_cumsum > P_99_V4 THEN P_99_V4 ELSE recargas_mdp_pagoelectronico_u5_pagos_cumsum END,
ofertas_all_u4_pagos_max = CASE WHEN ofertas_all_u4_pagos_max > P_99_V5 THEN P_99_V5 ELSE ofertas_all_u4_pagos_max END,
ofertas_all_u6_vigencia1_std = CASE WHEN ofertas_all_u6_vigencia1_std > P_99_V6 THEN P_99_V6 ELSE ofertas_all_u6_vigencia1_std END,
ofertas_all_u4_pagos_sum = CASE WHEN ofertas_all_u4_pagos_sum > P_99_V7 THEN P_99_V7 ELSE ofertas_all_u4_pagos_sum END,
ofertas_all_u3_pagos_avg = CASE WHEN ofertas_all_u3_pagos_avg > P_99_V8 THEN P_99_V8 ELSE ofertas_all_u3_pagos_avg END,
ofertas_all_u3_pagos_max = CASE WHEN ofertas_all_u3_pagos_max > P_99_V9 THEN P_99_V9 ELSE ofertas_all_u3_pagos_max END,
ofertas_all_u2_pagos_avg = CASE WHEN ofertas_all_u2_pagos_avg > P_99_V10 THEN P_99_V10 ELSE ofertas_all_u2_pagos_avg END,
recargas_rec_u1u2_pagos_ratio = CASE WHEN recargas_rec_u1u2_pagos_ratio > P_99_V11 THEN P_99_V11 ELSE recargas_rec_u1u2_pagos_ratio END,
ofertas_all_u6_vigencia1_avg = CASE WHEN ofertas_all_u6_vigencia1_avg > P_99_V12 THEN P_99_V12 ELSE ofertas_all_u6_vigencia1_avg END,
ofertas_all_u1_pagos_avg = CASE WHEN ofertas_all_u1_pagos_avg > P_99_V13 THEN P_99_V13 ELSE ofertas_all_u1_pagos_avg END,
recargas_rec_u1u3_pagos_diff = CASE WHEN recargas_rec_u1u3_pagos_diff > P_99_V14 THEN P_99_V14 ELSE recargas_rec_u1u3_pagos_diff END,
ofertas_all_u2_pagos_sum = CASE WHEN ofertas_all_u2_pagos_sum > P_99_V15 THEN P_99_V15 ELSE ofertas_all_u2_pagos_sum END,
recargas_rec_u1_arbu = CASE WHEN recargas_rec_u1_arbu > P_99_V16 THEN P_99_V16 ELSE recargas_rec_u1_arbu END,
recargas_rec_u1u6_pagos_diff = CASE WHEN recargas_rec_u1u6_pagos_diff > P_99_V17 THEN P_99_V17 ELSE recargas_rec_u1u6_pagos_diff END,
recargas_rec_u1u5_pagos_diff = CASE WHEN recargas_rec_u1u5_pagos_diff > P_99_V18 THEN P_99_V18 ELSE recargas_rec_u1u5_pagos_diff END,
recargas_rec_u3_pagos_sum = CASE WHEN recargas_rec_u3_pagos_sum > P_99_V19 THEN P_99_V19 ELSE recargas_rec_u3_pagos_sum END,
recargas_mdp_pagoelectronico_u1_pagos_sum = CASE WHEN recargas_mdp_pagoelectronico_u1_pagos_sum > P_99_V20 THEN P_99_V20 ELSE recargas_mdp_pagoelectronico_u1_pagos_sum END,
recargas_rec_u2_arbu = CASE WHEN recargas_rec_u2_arbu > P_99_V21 THEN P_99_V21 ELSE recargas_rec_u2_arbu END,
recargas_rec_u2_pagos_cumsum = CASE WHEN recargas_rec_u2_pagos_cumsum > P_99_V22 THEN P_99_V22 ELSE recargas_rec_u2_pagos_cumsum END,
recargas_mdp_pagoelectronico_u3_recargas_count = CASE WHEN recargas_mdp_pagoelectronico_u3_recargas_count > P_99_V23 THEN P_99_V23 ELSE recargas_mdp_pagoelectronico_u3_recargas_count END,
recargas_rec_u1u2_pagos_diff = CASE WHEN recargas_rec_u1u2_pagos_diff > P_99_V24 THEN P_99_V24 ELSE recargas_rec_u1u2_pagos_diff END,
recargas_rec_u5_arbu = CASE WHEN recargas_rec_u5_arbu > P_99_V25 THEN P_99_V25 ELSE recargas_rec_u5_arbu END,
recargas_mdp_pagoelectronico_u2_recargas_count = CASE WHEN recargas_mdp_pagoelectronico_u2_recargas_count > P_99_V26 THEN P_99_V26 ELSE recargas_mdp_pagoelectronico_u2_recargas_count END,
ofertas_all_u5_vigencia1_avg = CASE WHEN ofertas_all_u5_vigencia1_avg > P_99_V27 THEN P_99_V27 ELSE ofertas_all_u5_vigencia1_avg END,
ofertas_all_u4_vigencia1_std = CASE WHEN ofertas_all_u4_vigencia1_std > P_99_V28 THEN P_99_V28 ELSE ofertas_all_u4_vigencia1_std END,
recargas_rec_u4_arbu = CASE WHEN recargas_rec_u4_arbu > P_99_V29 THEN P_99_V29 ELSE recargas_rec_u4_arbu END,
recargas_mdp_pagoelectronico_u2_pagos_sum = CASE WHEN recargas_mdp_pagoelectronico_u2_pagos_sum > P_99_V30 THEN P_99_V30 ELSE recargas_mdp_pagoelectronico_u2_pagos_sum END,
recargas_rec_u6_pagos_sum = CASE WHEN recargas_rec_u6_pagos_sum > P_99_V31 THEN P_99_V31 ELSE recargas_rec_u6_pagos_sum END,
recargas_rec_u4_pagos_sum = CASE WHEN recargas_rec_u4_pagos_sum > P_99_V32 THEN P_99_V32 ELSE recargas_rec_u4_pagos_sum END,
recargas_mdp_pagoelectronico_u3_pagos_sum = CASE WHEN recargas_mdp_pagoelectronico_u3_pagos_sum > P_99_V33 THEN P_99_V33 ELSE recargas_mdp_pagoelectronico_u3_pagos_sum END,
recargas_rec_u5_pagos_cumsum = CASE WHEN recargas_rec_u5_pagos_cumsum > P_99_V34 THEN P_99_V34 ELSE recargas_rec_u5_pagos_cumsum END,
recargas_rec_u3_pagos_cumsum = CASE WHEN recargas_rec_u3_pagos_cumsum > P_99_V35 THEN P_99_V35 ELSE recargas_rec_u3_pagos_cumsum END,
recargas_rec_u4_pagos_cumsum = CASE WHEN recargas_rec_u4_pagos_cumsum > P_99_V36 THEN P_99_V36 ELSE recargas_rec_u4_pagos_cumsum END,
recargas_mdp_pagoelectronico_u4_pagos_sum = CASE WHEN recargas_mdp_pagoelectronico_u4_pagos_sum > P_99_V37 THEN P_99_V37 ELSE recargas_mdp_pagoelectronico_u4_pagos_sum END,
recargas_rec_u6_arbu = CASE WHEN recargas_rec_u6_arbu > P_99_V38 THEN P_99_V38 ELSE recargas_rec_u6_arbu END,
recargas_rec_u3_arbu = CASE WHEN recargas_rec_u3_arbu > P_99_V39 THEN P_99_V39 ELSE recargas_rec_u3_arbu END,
ofertas_all_u4_vigencia1_avg = CASE WHEN ofertas_all_u4_vigencia1_avg > P_99_V40 THEN P_99_V40 ELSE ofertas_all_u4_vigencia1_avg END,
recargas_rec_u6_pagos_cumsum = CASE WHEN recargas_rec_u6_pagos_cumsum > P_99_V41 THEN P_99_V41 ELSE recargas_rec_u6_pagos_cumsum END,
recargas_mdp_pagoelectronico_u4_recargas_count = CASE WHEN recargas_mdp_pagoelectronico_u4_recargas_count > P_99_V42 THEN P_99_V42 ELSE recargas_mdp_pagoelectronico_u4_recargas_count END,
recargas_mdp_pagoelectronico_u6_recargas_count = CASE WHEN recargas_mdp_pagoelectronico_u6_recargas_count > P_99_V43 THEN P_99_V43 ELSE recargas_mdp_pagoelectronico_u6_recargas_count END,
ofertas_all_u3_vigencia1_avg                    = CASE WHEN ofertas_all_u3_vigencia1_avg                    > P_99_V44 THEN P_99_V44 ELSE ofertas_all_u3_vigencia1_avg                    END,
-- stg_traficodatos_m
uplink_mb_t_max = CASE WHEN uplink_mb_t_max > P_99_V1 THEN P_99_V1 ELSE uplink_mb_t_max END,
uplink_mb_sh4_std = CASE WHEN uplink_mb_sh4_std > P_99_V2 THEN P_99_V2 ELSE uplink_mb_sh4_std END,
uplink_mb_t_std = CASE WHEN uplink_mb_t_std > P_99_V3 THEN P_99_V3 ELSE uplink_mb_t_std END,
uplink_mb_s_std = CASE WHEN uplink_mb_s_std > P_99_V4 THEN P_99_V4 ELSE uplink_mb_s_std END,
downlink_mb_fs_max = CASE WHEN downlink_mb_fs_max > P_99_V5 THEN P_99_V5 ELSE downlink_mb_fs_max END,
uplink_mb_s_max = CASE WHEN uplink_mb_s_max > P_99_V6 THEN P_99_V6 ELSE uplink_mb_s_max END,
totat_traffic_mb_fs_max = CASE WHEN totat_traffic_mb_fs_max > P_99_V7 THEN P_99_V7 ELSE totat_traffic_mb_fs_max END,
totat_traffic_mb_s_max = CASE WHEN totat_traffic_mb_s_max > P_99_V8 THEN P_99_V8 ELSE totat_traffic_mb_s_max END,
uplink_mb_sh4_max = CASE WHEN uplink_mb_sh4_max > P_99_V9 THEN P_99_V9 ELSE uplink_mb_sh4_max END,
downlink_mb_sh4_std = CASE WHEN downlink_mb_sh4_std > P_99_V10 THEN P_99_V10 ELSE downlink_mb_sh4_std END,
uplink_mb_s_avg = CASE WHEN uplink_mb_s_avg > P_99_V11 THEN P_99_V11 ELSE uplink_mb_s_avg END,
traffic_4g_mb_sh4_std = CASE WHEN traffic_4g_mb_sh4_std > P_99_V12 THEN P_99_V12 ELSE traffic_4g_mb_sh4_std END,
downlink_mb_s_max        = CASE WHEN downlink_mb_s_max        > P_99_V13 THEN P_99_V13 ELSE downlink_mb_s_max        END,
traffic_4g_mb_fs_max = CASE WHEN traffic_4g_mb_fs_max > P_99_V14 THEN P_99_V14 ELSE traffic_4g_mb_fs_max END,
uplink_mb_sh4_avg = CASE WHEN uplink_mb_sh4_avg > P_99_V15 THEN P_99_V15 ELSE uplink_mb_sh4_avg END,
totat_traffic_mb_t_max = CASE WHEN totat_traffic_mb_t_max > P_99_V16 THEN P_99_V16 ELSE totat_traffic_mb_t_max END,
totat_traffic_mb_sh4_max = CASE WHEN totat_traffic_mb_sh4_max > P_99_V17 THEN P_99_V17 ELSE totat_traffic_mb_sh4_max END,
downlink_mb_t_max = CASE WHEN downlink_mb_t_max > P_99_V18 THEN P_99_V18 ELSE downlink_mb_t_max END,
downlink_mb_s_std = CASE WHEN downlink_mb_s_std > P_99_V19 THEN P_99_V19 ELSE downlink_mb_s_std END,
totat_traffic_mb_fs_std = CASE WHEN totat_traffic_mb_fs_std > P_99_V20 THEN P_99_V20 ELSE totat_traffic_mb_fs_std END,
traffic_4g_mb_t_max = CASE WHEN traffic_4g_mb_t_max > P_99_V21 THEN P_99_V21 ELSE traffic_4g_mb_t_max END,
totat_traffic_mb_sh4_std = CASE WHEN totat_traffic_mb_sh4_std > P_99_V22 THEN P_99_V22 ELSE totat_traffic_mb_sh4_std END,
traffic_4g_mb_fs_std = CASE WHEN traffic_4g_mb_fs_std > P_99_V23 THEN P_99_V23 ELSE traffic_4g_mb_fs_std END,
downlink_mb_fs_std = CASE WHEN downlink_mb_fs_std > P_99_V24 THEN P_99_V24 ELSE downlink_mb_fs_std END,
traffic_4g_mb_s_max = CASE WHEN traffic_4g_mb_s_max > P_99_V25 THEN P_99_V25 ELSE traffic_4g_mb_s_max END,
downlink_mb_t_std = CASE WHEN downlink_mb_t_std > P_99_V26 THEN P_99_V26 ELSE downlink_mb_t_std END,
traffic_4g_mb_sh4_max = CASE WHEN traffic_4g_mb_sh4_max > P_99_V27 THEN P_99_V27 ELSE traffic_4g_mb_sh4_max END,
downlink_mb_sh4_max = CASE WHEN downlink_mb_sh4_max > P_99_V28 THEN P_99_V28 ELSE downlink_mb_sh4_max END,
downlink_mb_t_avg = CASE WHEN downlink_mb_t_avg > P_99_V29 THEN P_99_V29 ELSE downlink_mb_t_avg END,
downlink_mb_fs_avg = CASE WHEN downlink_mb_fs_avg > P_99_V30 THEN P_99_V30 ELSE downlink_mb_fs_avg END,
uplink_mb_t_avg = CASE WHEN uplink_mb_t_avg > P_99_V31 THEN P_99_V31 ELSE uplink_mb_t_avg END,
traffic_4g_mb_s_avg = CASE WHEN traffic_4g_mb_s_avg > P_99_V32 THEN P_99_V32 ELSE traffic_4g_mb_s_avg END,
downlink_mb_s_avg = CASE WHEN downlink_mb_s_avg > P_99_V33 THEN P_99_V33 ELSE downlink_mb_s_avg END,
traffic_4g_mb_sh4_avg = CASE WHEN traffic_4g_mb_sh4_avg > P_99_V34 THEN P_99_V34 ELSE traffic_4g_mb_sh4_avg END,
totat_traffic_mb_s_avg = CASE WHEN totat_traffic_mb_s_avg > P_99_V35 THEN P_99_V35 ELSE totat_traffic_mb_s_avg END,
totat_traffic_mb_t_avg = CASE WHEN totat_traffic_mb_t_avg > P_99_V36 THEN P_99_V36 ELSE totat_traffic_mb_t_avg END,
traffic_4g_mb_fs_min = CASE WHEN traffic_4g_mb_fs_min > P_99_V37 THEN P_99_V37 ELSE traffic_4g_mb_fs_min END,
totat_traffic_mb_s_std = CASE WHEN totat_traffic_mb_s_std > P_99_V38 THEN P_99_V38 ELSE totat_traffic_mb_s_std END,
totat_traffic_mb_fs_avg = CASE WHEN totat_traffic_mb_fs_avg > P_99_V39 THEN P_99_V39 ELSE totat_traffic_mb_fs_avg END,
downlink_mb_sh4_avg = CASE WHEN downlink_mb_sh4_avg > P_99_V40 THEN P_99_V40 ELSE downlink_mb_sh4_avg END,
traffic_3g_mb_sh4_max = CASE WHEN traffic_3g_mb_sh4_max > P_99_V41 THEN P_99_V41 ELSE traffic_3g_mb_sh4_max END,
traffic_4g_mb_s_min = CASE WHEN traffic_4g_mb_s_min > P_99_V42 THEN P_99_V42 ELSE traffic_4g_mb_s_min END,
traffic_4g_mb_fs_avg = CASE WHEN traffic_4g_mb_fs_avg > P_99_V43 THEN P_99_V43 ELSE traffic_4g_mb_fs_avg END,
totat_traffic_mb_sh4_avg = CASE WHEN totat_traffic_mb_sh4_avg > P_99_V44 THEN P_99_V44 ELSE totat_traffic_mb_sh4_avg END,
traffic_3g_mb_fs_std = CASE WHEN traffic_3g_mb_fs_std > P_99_V45 THEN P_99_V45 ELSE traffic_3g_mb_fs_std END,
traffic_4g_mb_t_std = CASE WHEN traffic_4g_mb_t_std > P_99_V46 THEN P_99_V46 ELSE traffic_4g_mb_t_std END,
totat_traffic_mb_t_std = CASE WHEN totat_traffic_mb_t_std > P_99_V47 THEN P_99_V47 ELSE totat_traffic_mb_t_std END,
traffic_3g_mb_fs_avg = CASE WHEN traffic_3g_mb_fs_avg > P_99_V48 THEN P_99_V48 ELSE traffic_3g_mb_fs_avg END,
traffic_3g_mb_s_std = CASE WHEN traffic_3g_mb_s_std > P_99_V49 THEN P_99_V49 ELSE traffic_3g_mb_s_std END,
traffic_3g_mb_fs_max = CASE WHEN traffic_3g_mb_fs_max > P_99_V50 THEN P_99_V50 ELSE traffic_3g_mb_fs_max END,
traffic_4g_mb_t_avg = CASE WHEN traffic_4g_mb_t_avg > P_99_V51 THEN P_99_V51 ELSE traffic_4g_mb_t_avg END,
traffic_3g_mb_s_avg = CASE WHEN traffic_3g_mb_s_avg > P_99_V52 THEN P_99_V52 ELSE traffic_3g_mb_s_avg END,
downlink_mb_fs_min = CASE WHEN downlink_mb_fs_min > P_99_V53 THEN P_99_V53 ELSE downlink_mb_fs_min END,
totat_traffic_mb_fs_min = CASE WHEN totat_traffic_mb_fs_min > P_99_V54 THEN P_99_V54 ELSE totat_traffic_mb_fs_min END,
traffic_3g_mb_t_max = CASE WHEN traffic_3g_mb_t_max > P_99_V55 THEN P_99_V55 ELSE traffic_3g_mb_t_max END,
uplink_mb_t_min = CASE WHEN uplink_mb_t_min > P_99_V56 THEN P_99_V56 ELSE uplink_mb_t_min END,
traffic_4g_mb_t_min = CASE WHEN traffic_4g_mb_t_min > P_99_V57 THEN P_99_V57 ELSE traffic_4g_mb_t_min END,
totat_traffic_mb_s_min = CASE WHEN totat_traffic_mb_s_min > P_99_V58 THEN P_99_V58 ELSE totat_traffic_mb_s_min END,
traffic_3g_mb_s_max = CASE WHEN traffic_3g_mb_s_max > P_99_V59 THEN P_99_V59 ELSE traffic_3g_mb_s_max END,
downlink_mb_s_min = CASE WHEN downlink_mb_s_min > P_99_V60 THEN P_99_V60 ELSE downlink_mb_s_min END,
traffic_3g_mb_t_avg = CASE WHEN traffic_3g_mb_t_avg > P_99_V61 THEN P_99_V61 ELSE traffic_3g_mb_t_avg END,
traffic_3g_mb_sh4_std = CASE WHEN traffic_3g_mb_sh4_std > P_99_V62 THEN P_99_V62 ELSE traffic_3g_mb_sh4_std END,
totat_traffic_mb_t_min = CASE WHEN totat_traffic_mb_t_min > P_99_V63 THEN P_99_V63 ELSE totat_traffic_mb_t_min END,
uplink_mb_s_min = CASE WHEN uplink_mb_s_min > P_99_V64 THEN P_99_V64 ELSE uplink_mb_s_min END,
traffic_3g_mb_t_std      = CASE WHEN traffic_3g_mb_t_std      > P_99_V65 THEN P_99_V65 ELSE traffic_3g_mb_t_std      END
WHERE 1=1 AND periodo = M_ABT;


------------------------------------------------------------------------------------------------------------
-- NORMALIZACIÓN 
------------------------------------------------------------------------------------------------------------

-- Eliminamos los datos antes de insertarlos
DELETE FROM `{{ PROJECT_ID }}.{{ DATASET }}.abt_capro_m`
WHERE periodo = M_ABT;

-- Insertamos datos normalizados
INSERT INTO `{{ PROJECT_ID }}.{{ DATASET }}.abt_capro_m`
SELECT
        linea,
        --ft_prepago_m
        ML.MIN_MAX_SCALER(ofertas_all_u6_pagos_sum) OVER() AS ofertas_all_u6_pagos_sum,
        ML.MIN_MAX_SCALER(ofertas_all_u5_pagos_sum) OVER() AS ofertas_all_u5_pagos_sum,
        ML.MIN_MAX_SCALER(recargas_mdp_pagoelectronico_u6_pagos_cumsum) OVER() AS recargas_mdp_pagoelectronico_u6_pagos_cumsum,
        ML.MIN_MAX_SCALER(recargas_mdp_pagoelectronico_u5_pagos_cumsum) OVER() AS recargas_mdp_pagoelectronico_u5_pagos_cumsum,
        ML.MIN_MAX_SCALER(ofertas_all_u4_pagos_max) OVER() AS ofertas_all_u4_pagos_max,
        ML.MIN_MAX_SCALER(ofertas_all_u6_vigencia1_std) OVER() AS ofertas_all_u6_vigencia1_std,
        ML.MIN_MAX_SCALER(ofertas_all_u4_pagos_sum) OVER() AS ofertas_all_u4_pagos_sum,
        ML.MIN_MAX_SCALER(ofertas_all_u3_pagos_avg) OVER() AS ofertas_all_u3_pagos_avg,
        ML.MIN_MAX_SCALER(ofertas_all_u3_pagos_max) OVER() AS ofertas_all_u3_pagos_max,
        ML.MIN_MAX_SCALER(ofertas_all_u2_pagos_avg) OVER() AS ofertas_all_u2_pagos_avg,
        ML.MIN_MAX_SCALER(recargas_rec_u1u2_pagos_ratio) OVER() AS recargas_rec_u1u2_pagos_ratio,
        ML.MIN_MAX_SCALER(ofertas_all_u6_vigencia1_avg) OVER() AS ofertas_all_u6_vigencia1_avg,
        ML.MIN_MAX_SCALER(ofertas_all_u1_pagos_avg) OVER() AS ofertas_all_u1_pagos_avg,
        ML.MIN_MAX_SCALER(recargas_rec_u1u3_pagos_diff) OVER() AS recargas_rec_u1u3_pagos_diff,
        ML.MIN_MAX_SCALER(ofertas_all_u2_pagos_sum) OVER() AS ofertas_all_u2_pagos_sum,
        ML.MIN_MAX_SCALER(recargas_rec_u1_arbu) OVER() AS recargas_rec_u1_arbu,
        ML.MIN_MAX_SCALER(recargas_rec_u1u6_pagos_diff) OVER() AS recargas_rec_u1u6_pagos_diff,
        ML.MIN_MAX_SCALER(recargas_rec_u1u5_pagos_diff) OVER() AS recargas_rec_u1u5_pagos_diff,
        ML.MIN_MAX_SCALER(recargas_rec_u3_pagos_sum) OVER() AS recargas_rec_u3_pagos_sum,
        ML.MIN_MAX_SCALER(recargas_mdp_pagoelectronico_u1_pagos_sum) OVER() AS recargas_mdp_pagoelectronico_u1_pagos_sum,
        ML.MIN_MAX_SCALER(recargas_rec_u2_arbu) OVER() AS recargas_rec_u2_arbu,
        ML.MIN_MAX_SCALER(recargas_rec_u2_pagos_cumsum) OVER() AS recargas_rec_u2_pagos_cumsum,
        ML.MIN_MAX_SCALER(recargas_mdp_pagoelectronico_u3_recargas_count) OVER() AS recargas_mdp_pagoelectronico_u3_recargas_count,
        ML.MIN_MAX_SCALER(recargas_rec_u1u2_pagos_diff) OVER() AS recargas_rec_u1u2_pagos_diff,
        ML.MIN_MAX_SCALER(recargas_rec_u5_arbu) OVER() AS recargas_rec_u5_arbu,
        ML.MIN_MAX_SCALER(recargas_mdp_pagoelectronico_u2_recargas_count) OVER() AS recargas_mdp_pagoelectronico_u2_recargas_count,
        ML.MIN_MAX_SCALER(ofertas_all_u5_vigencia1_avg) OVER() AS ofertas_all_u5_vigencia1_avg,
        ML.MIN_MAX_SCALER(ofertas_all_u4_vigencia1_std) OVER() AS ofertas_all_u4_vigencia1_std,
        ML.MIN_MAX_SCALER(recargas_rec_u4_arbu) OVER() AS recargas_rec_u4_arbu,
        ML.MIN_MAX_SCALER(recargas_mdp_pagoelectronico_u2_pagos_sum) OVER() AS recargas_mdp_pagoelectronico_u2_pagos_sum,
        ML.MIN_MAX_SCALER(recargas_rec_u6_pagos_sum) OVER() AS recargas_rec_u6_pagos_sum,
        ML.MIN_MAX_SCALER(recargas_rec_u4_pagos_sum) OVER() AS recargas_rec_u4_pagos_sum,
        ML.MIN_MAX_SCALER(recargas_mdp_pagoelectronico_u3_pagos_sum) OVER() AS recargas_mdp_pagoelectronico_u3_pagos_sum,
        ML.MIN_MAX_SCALER(recargas_rec_u5_pagos_cumsum) OVER() AS recargas_rec_u5_pagos_cumsum,
        ML.MIN_MAX_SCALER(recargas_rec_u3_pagos_cumsum) OVER() AS recargas_rec_u3_pagos_cumsum,
        ML.MIN_MAX_SCALER(recargas_rec_u4_pagos_cumsum) OVER() AS recargas_rec_u4_pagos_cumsum,
        ML.MIN_MAX_SCALER(recargas_mdp_pagoelectronico_u4_pagos_sum) OVER() AS recargas_mdp_pagoelectronico_u4_pagos_sum,
        ML.MIN_MAX_SCALER(recargas_rec_u6_arbu) OVER() AS recargas_rec_u6_arbu,
        ML.MIN_MAX_SCALER(recargas_rec_u3_arbu) OVER() AS recargas_rec_u3_arbu,
        ML.MIN_MAX_SCALER(ofertas_all_u4_vigencia1_avg) OVER() AS ofertas_all_u4_vigencia1_avg,
        ML.MIN_MAX_SCALER(recargas_rec_u6_pagos_cumsum) OVER() AS recargas_rec_u6_pagos_cumsum,
        ML.MIN_MAX_SCALER(recargas_mdp_pagoelectronico_u4_recargas_count) OVER() AS recargas_mdp_pagoelectronico_u4_recargas_count,
        ML.MIN_MAX_SCALER(recargas_mdp_pagoelectronico_u6_recargas_count) OVER() AS recargas_mdp_pagoelectronico_u6_recargas_count,
        ML.MIN_MAX_SCALER(ofertas_all_u3_vigencia1_avg                   ) OVER() AS ofertas_all_u3_vigencia1_avg,
        -- stg_traficodatos_m
        ML.MIN_MAX_SCALER(uplink_mb_t_max) OVER() AS uplink_mb_t_max,
        ML.MIN_MAX_SCALER(uplink_mb_sh4_std) OVER() AS uplink_mb_sh4_std,
        ML.MIN_MAX_SCALER(uplink_mb_t_std) OVER() AS uplink_mb_t_std,
        ML.MIN_MAX_SCALER(uplink_mb_s_std) OVER() AS uplink_mb_s_std,
        ML.MIN_MAX_SCALER(downlink_mb_fs_max) OVER() AS downlink_mb_fs_max,
        ML.MIN_MAX_SCALER(uplink_mb_s_max) OVER() AS uplink_mb_s_max,
        ML.MIN_MAX_SCALER(totat_traffic_mb_fs_max) OVER() AS totat_traffic_mb_fs_max,
        ML.MIN_MAX_SCALER(totat_traffic_mb_s_max) OVER() AS totat_traffic_mb_s_max,
        ML.MIN_MAX_SCALER(uplink_mb_sh4_max) OVER() AS uplink_mb_sh4_max,
        ML.MIN_MAX_SCALER(downlink_mb_sh4_std) OVER() AS downlink_mb_sh4_std,
        ML.MIN_MAX_SCALER(uplink_mb_s_avg) OVER() AS uplink_mb_s_avg,
        ML.MIN_MAX_SCALER(traffic_4g_mb_sh4_std) OVER() AS traffic_4g_mb_sh4_std,
        ML.MIN_MAX_SCALER(downlink_mb_s_max       ) OVER() AS downlink_mb_s_max       ,
        ML.MIN_MAX_SCALER(traffic_4g_mb_fs_max) OVER() AS traffic_4g_mb_fs_max,
        ML.MIN_MAX_SCALER(uplink_mb_sh4_avg) OVER() AS uplink_mb_sh4_avg,
        ML.MIN_MAX_SCALER(totat_traffic_mb_t_max) OVER() AS totat_traffic_mb_t_max,
        ML.MIN_MAX_SCALER(totat_traffic_mb_sh4_max) OVER() AS totat_traffic_mb_sh4_max,
        ML.MIN_MAX_SCALER(downlink_mb_t_max) OVER() AS downlink_mb_t_max,
        ML.MIN_MAX_SCALER(downlink_mb_s_std) OVER() AS downlink_mb_s_std,
        ML.MIN_MAX_SCALER(totat_traffic_mb_fs_std) OVER() AS totat_traffic_mb_fs_std,
        ML.MIN_MAX_SCALER(traffic_4g_mb_t_max) OVER() AS traffic_4g_mb_t_max,
        ML.MIN_MAX_SCALER(totat_traffic_mb_sh4_std) OVER() AS totat_traffic_mb_sh4_std,
        ML.MIN_MAX_SCALER(traffic_4g_mb_fs_std) OVER() AS traffic_4g_mb_fs_std,
        ML.MIN_MAX_SCALER(downlink_mb_fs_std) OVER() AS downlink_mb_fs_std,
        ML.MIN_MAX_SCALER(traffic_4g_mb_s_max) OVER() AS traffic_4g_mb_s_max,
        ML.MIN_MAX_SCALER(downlink_mb_t_std) OVER() AS downlink_mb_t_std,
        ML.MIN_MAX_SCALER(traffic_4g_mb_sh4_max) OVER() AS traffic_4g_mb_sh4_max,
        ML.MIN_MAX_SCALER(downlink_mb_sh4_max) OVER() AS downlink_mb_sh4_max,
        ML.MIN_MAX_SCALER(downlink_mb_t_avg) OVER() AS downlink_mb_t_avg,
        ML.MIN_MAX_SCALER(downlink_mb_fs_avg) OVER() AS downlink_mb_fs_avg,
        ML.MIN_MAX_SCALER(uplink_mb_t_avg) OVER() AS uplink_mb_t_avg,
        ML.MIN_MAX_SCALER(traffic_4g_mb_s_avg) OVER() AS traffic_4g_mb_s_avg,
        ML.MIN_MAX_SCALER(downlink_mb_s_avg) OVER() AS downlink_mb_s_avg,
        ML.MIN_MAX_SCALER(traffic_4g_mb_sh4_avg) OVER() AS traffic_4g_mb_sh4_avg,
        ML.MIN_MAX_SCALER(totat_traffic_mb_s_avg) OVER() AS totat_traffic_mb_s_avg,
        ML.MIN_MAX_SCALER(totat_traffic_mb_t_avg) OVER() AS totat_traffic_mb_t_avg,
        ML.MIN_MAX_SCALER(traffic_4g_mb_fs_min) OVER() AS traffic_4g_mb_fs_min,
        ML.MIN_MAX_SCALER(totat_traffic_mb_s_std) OVER() AS totat_traffic_mb_s_std,
        ML.MIN_MAX_SCALER(totat_traffic_mb_fs_avg) OVER() AS totat_traffic_mb_fs_avg,
        ML.MIN_MAX_SCALER(downlink_mb_sh4_avg) OVER() AS downlink_mb_sh4_avg,
        ML.MIN_MAX_SCALER(traffic_3g_mb_sh4_max) OVER() AS traffic_3g_mb_sh4_max,
        ML.MIN_MAX_SCALER(traffic_4g_mb_s_min) OVER() AS traffic_4g_mb_s_min,
        ML.MIN_MAX_SCALER(traffic_4g_mb_fs_avg) OVER() AS traffic_4g_mb_fs_avg,
        ML.MIN_MAX_SCALER(totat_traffic_mb_sh4_avg) OVER() AS totat_traffic_mb_sh4_avg,
        ML.MIN_MAX_SCALER(traffic_3g_mb_fs_std) OVER() AS traffic_3g_mb_fs_std,
        ML.MIN_MAX_SCALER(traffic_4g_mb_t_std) OVER() AS traffic_4g_mb_t_std,
        ML.MIN_MAX_SCALER(totat_traffic_mb_t_std) OVER() AS totat_traffic_mb_t_std,
        ML.MIN_MAX_SCALER(traffic_3g_mb_fs_avg) OVER() AS traffic_3g_mb_fs_avg,
        ML.MIN_MAX_SCALER(traffic_3g_mb_s_std) OVER() AS traffic_3g_mb_s_std,
        ML.MIN_MAX_SCALER(traffic_3g_mb_fs_max) OVER() AS traffic_3g_mb_fs_max,
        ML.MIN_MAX_SCALER(traffic_4g_mb_t_avg) OVER() AS traffic_4g_mb_t_avg,
        ML.MIN_MAX_SCALER(traffic_3g_mb_s_avg) OVER() AS traffic_3g_mb_s_avg,
        ML.MIN_MAX_SCALER(downlink_mb_fs_min) OVER() AS downlink_mb_fs_min,
        ML.MIN_MAX_SCALER(totat_traffic_mb_fs_min) OVER() AS totat_traffic_mb_fs_min,
        ML.MIN_MAX_SCALER(traffic_3g_mb_t_max) OVER() AS traffic_3g_mb_t_max,
        ML.MIN_MAX_SCALER(uplink_mb_t_min) OVER() AS uplink_mb_t_min,
        ML.MIN_MAX_SCALER(traffic_4g_mb_t_min) OVER() AS traffic_4g_mb_t_min,
        ML.MIN_MAX_SCALER(totat_traffic_mb_s_min) OVER() AS totat_traffic_mb_s_min,
        ML.MIN_MAX_SCALER(traffic_3g_mb_s_max) OVER() AS traffic_3g_mb_s_max,
        ML.MIN_MAX_SCALER(downlink_mb_s_min) OVER() AS downlink_mb_s_min,
        ML.MIN_MAX_SCALER(traffic_3g_mb_t_avg) OVER() AS traffic_3g_mb_t_avg,
        ML.MIN_MAX_SCALER(traffic_3g_mb_sh4_std) OVER() AS traffic_3g_mb_sh4_std,
        ML.MIN_MAX_SCALER(totat_traffic_mb_t_min) OVER() AS totat_traffic_mb_t_min,
        ML.MIN_MAX_SCALER(uplink_mb_s_min) OVER() AS uplink_mb_s_min,
        ML.MIN_MAX_SCALER(traffic_3g_mb_t_std     ) OVER() AS traffic_3g_mb_t_std,
        -- periodo
        periodo,
        -- prediccion añadimos un valor por defecto
        0 score,
        0 bin           
FROM `{{ PROJECT_ID }}.{{ DATASET }}.tmp_capro_trunc` A 
WHERE A.periodo = M_ABT;

-- Dropeamos las tablas temporales
DROP TABLE `{{ PROJECT_ID }}.{{ DATASET }}.tmp_capro_imp` ;
DROP TABLE `{{ PROJECT_ID }}.{{ DATASET }}.tmp_capro_trunc` ;
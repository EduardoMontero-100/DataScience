----------------------------------------------------
-- VARIABLES
----------------------------------------------------
DECLARE p_0, p_1, p_2, p_3, p_4, p_5, p_6, p_7, p_8, p_9, p_10, p_11, p_12, p_13, p_14, p_15, p_16, p_17, p_18, p_19 FLOAT64;
DECLARE filter_date DATE;
DECLARE M_ABT INT64;
          
SET filter_date = '{{ next_execution_date | ds }}';  -- borrar ya que es la fecha de ejecución del DAG
SET M_ABT     = CAST(FORMAT_DATE('%Y%m',DATE_ADD(filter_date, INTERVAL -1 MONTH)) AS INT64); -- Mes usado en abt_capro 

-- Levantamos los puntos de corte del baseline de capro: 202207
CREATE OR REPLACE TABLE `{{ PROJECT_ID }}.{{ DATASET }}.tmp_bines_baseline` AS
SELECT ROUND(max_p_target, 6) max_p_target, bin
FROM `{{ PROJECT_ID }}.{{ DATASET }}.indicadores_psi_bl`  
WHERE modelo = 'Capro' and fecha = (SELECT MAX(fecha) FROM `{{ PROJECT_ID }}.{{ DATASET }}.indicadores_psi_bl`  
                                    WHERE modelo = 'Capro')
ORDER BY bin;


SET (p_0, p_1, p_2, p_3, p_4, p_5, p_6, p_7, p_8, p_9, p_10, p_11, p_12, p_13, p_14, p_15, p_16, p_17, p_18, p_19)
=(
SELECT AS STRUCT * FROM `{{ PROJECT_ID }}.{{ DATASET }}.tmp_bines_baseline`
PIVOT(MAX(max_p_target) FOR bin IN (0,1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19))
)
;
-- Borramos la abt para contemplar reprocesos
DELETE FROM `{{ PROJECT_ID }}.{{ DATASET }}.abt_capro_m` WHERE periodo = M_ABT;

-- Insertamos la abt tanto los features como el score y el bin
INSERT INTO `{{ PROJECT_ID }}.{{ DATASET }}.abt_capro_m`
SELECT 
    linea,
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
    ofertas_all_u3_vigencia1_avg,
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
    traffic_3g_mb_t_std,
    periodo,
    score,
    CASE 
        WHEN ROUND(score, 6) <= coalesce(p_0, 0) THEN 0 
        WHEN ROUND(score, 6) >  coalesce(p_0, 0)  AND ROUND(score, 6) <= coalesce(p_1, 0) THEN 1
        WHEN ROUND(score, 6) >  coalesce(p_1, 0)  AND ROUND(score, 6) <= coalesce(p_2, 0) THEN 2
        WHEN ROUND(score, 6) >  coalesce(p_2, 0)  AND ROUND(score, 6) <= coalesce(p_3, 0) THEN 3
        WHEN ROUND(score, 6) >  coalesce(p_3, 0)  AND ROUND(score, 6) <= coalesce(p_4, 0) THEN 4
        WHEN ROUND(score, 6) >  coalesce(p_4, 0)  AND ROUND(score, 6) <= coalesce(p_5, 0) THEN 5
        WHEN ROUND(score, 6) >  coalesce(p_5, 0)  AND ROUND(score, 6) <= coalesce(p_6, 0) THEN 6
        WHEN ROUND(score, 6) >  coalesce(p_6, 0)  AND ROUND(score, 6) <= coalesce(p_7, 0) THEN 7
        WHEN ROUND(score, 6) >  coalesce(p_7, 0)  AND ROUND(score, 6) <= coalesce(p_8, 0) THEN 8
        WHEN ROUND(score, 6) >  coalesce(p_8, 0)  AND ROUND(score, 6) <= coalesce(p_9, 0) THEN 9
        WHEN ROUND(score, 6) >  coalesce(p_9, 0)  AND ROUND(score, 6) <= coalesce(p_10, 0) THEN 10
        WHEN ROUND(score, 6) >  coalesce(p_10, 0) AND ROUND(score, 6) <= coalesce(p_11, 0) THEN 11
        WHEN ROUND(score, 6) >  coalesce(p_11, 0) AND ROUND(score, 6) <= coalesce(p_12, 0) THEN 12
        WHEN ROUND(score, 6) >  coalesce(p_12, 0) AND ROUND(score, 6) <= coalesce(p_13, 0) THEN 13
        WHEN ROUND(score, 6) >  coalesce(p_13, 0) AND ROUND(score, 6) <= coalesce(p_14, 0) THEN 14
        WHEN ROUND(score, 6) >  coalesce(p_14, 0) AND ROUND(score, 6) <= coalesce(p_15, 0) THEN 15
        WHEN ROUND(score, 6) >  coalesce(p_15, 0) AND ROUND(score, 6) <= coalesce(p_16, 0) THEN 16
        WHEN ROUND(score, 6) >  coalesce(p_16, 0) AND ROUND(score, 6) <= coalesce(p_17, 0) THEN 17
        WHEN ROUND(score, 6) >  coalesce(p_17, 0) AND ROUND(score, 6) <= coalesce(p_18, 0) THEN 18
        WHEN ROUND(score, 6) >  coalesce(p_18, 0) THEN 19 
        END AS bin
FROM `{{ PROJECT_ID }}.{{ DATASET }}.tmp_capro_batch_preds`;

-- Dropeamos las tablas temporales
DROP TABLE `{{ PROJECT_ID }}.{{ DATASET }}.tmp_capro_batch_preds`;
DROP TABLE `{{ PROJECT_ID }}.{{ DATASET }}.tmp_bines_baseline`;

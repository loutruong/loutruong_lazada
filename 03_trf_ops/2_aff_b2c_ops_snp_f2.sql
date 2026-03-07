-- MaxCompute SQL 
-- ********************************************************************--
-- author:Truong, Van Thanh
-- create time:2024-06-01 16:55:51
-- ********************************************************************--
--@@ Input = lazada_analyst.loutruong_aff_b2c_ops_target_breakdown
--@@ Input = lazada_analyst.loutruong_aff_mem_id_offline_info
--@@ Input = lazada_analyst_dev.tmp_loutruong_aff_perf_hh_1
--@@ Input = lazada_analyst_dev.loutruong_aff_perf_plm_ops_di
--@@ Input = lazada_cdm.dim_lzd_date
WITH t_date AS (
    SELECT ds_day AS ds,
CASE
            WHEN ds_day = 20240301 THEN 'Teasing'
            WHEN ds_day = 20240302 THEN 'Teasing'
            WHEN ds_day = 20240303 THEN 'Mega1'
            WHEN ds_day = 20240304 THEN 'Mega'
            WHEN ds_day = 20240305 THEN 'Mega'
            WHEN ds_day = 20240306 THEN 'BAU'
            WHEN ds_day = 20240307 THEN 'BAU'
            WHEN ds_day = 20240308 THEN 'BAU'
            WHEN ds_day = 20240309 THEN 'BAU'
            WHEN ds_day = 20240310 THEN 'BAU'
            WHEN ds_day = 20240311 THEN 'BAU'
            WHEN ds_day = 20240312 THEN 'BAU'
            WHEN ds_day = 20240313 THEN 'BAU'
            WHEN ds_day = 20240314 THEN 'BAU'
            WHEN ds_day = 20240315 THEN 'A++'
            WHEN ds_day = 20240316 THEN 'A+'
            WHEN ds_day = 20240317 THEN 'A+'
            WHEN ds_day = 20240318 THEN 'BAU'
            WHEN ds_day = 20240319 THEN 'BAU'
            WHEN ds_day = 20240320 THEN 'BAU'
            WHEN ds_day = 20240321 THEN 'BAU'
            WHEN ds_day = 20240322 THEN 'Teasing'
            WHEN ds_day = 20240323 THEN 'Teasing'
            WHEN ds_day = 20240324 THEN 'Mega-D-1'
            WHEN ds_day = 20240325 THEN 'Mega1'
            WHEN ds_day = 20240326 THEN 'Mega'
            WHEN ds_day = 20240327 THEN 'Mega'
            WHEN ds_day = 20240328 THEN 'Mega'
            WHEN ds_day = 20240329 THEN 'Mega'
            WHEN ds_day = 20240330 THEN 'BAU'
            WHEN ds_day = 20240331 THEN 'BAU'
        END AS period
    FROM lazada_cdm.dim_lzd_date
    WHERE 1 = 1
        AND TO_CHAR(TO_DATE(ds_day, 'yyyymmdd'), 'yyyymm') IN (202403)
    ORDER BY ds_day ASC
),
t_member_id_master_raw AS (
    SELECT t1.period AS period,
        t1.member_id AS member_id,
        t1.member_name AS member_name,
        t1.team_manage AS team_manage,
        t1.plm_tier AS plm_tier,
        t1.pic AS pic,
        t1.gmv_baseline AS gmv_baseline,
        t1.order_baseline AS order_baseline,
        t2.duration AS duration,
        t2.gmv AS gmv_target_team,
        t2.order AS order_target_team
    FROM (
            SELECT t2.period AS period,
                t1.member_id AS member_id,
                t3.member_name AS member_name,
                t3.team_manage AS team_manage,
                t3.member_plm_local_lv2 AS plm_tier,
                t3.member_pic_local AS pic,
                SUM(t1.gmv_usd_lt_1d_p) AS gmv_baseline,
                SUM(t1.order_cnt_lt_1d_p) AS order_baseline
            FROM (
                    SELECT ds,
                        member_id,
                        gmv_usd_lt_1d_p,
                        order_cnt_lt_1d_p
                    FROM lazada_analyst_dev.loutruong_aff_perf_plm_ops_di
                    WHERE 1 = 1
                        AND TO_CHAR(TO_DATE(ds, 'yyyymmdd'), 'yyyymm') IN (202403)
                ) AS t1
                INNER JOIN (
                    SELECT ds,
                        period
                    FROM t_date
                ) AS t2 ON t1.ds = t2.ds
                INNER JOIN (
                    SELECT member_id,
                        member_name,
                        team_manage,
                        member_plm_local_lv2,
                        member_pic_local
                    FROM lazada_analyst.loutruong_aff_mem_id_offline_info
                    WHERE 1 = 1
                        AND month_apply = (
                            SELECT MAX(month_apply)
                            FROM lazada_analyst.loutruong_aff_mem_id_offline_info
                        )
                ) AS t3 ON t1.member_id = t3.member_id
            GROUP BY t1.member_id,
                t3.member_name,
                t3.team_manage,
                t3.member_plm_local_lv2,
                t3.member_pic_local,
                t2.period
        ) AS t1
        INNER JOIN (
            SELECT period,
                team AS team_manage,
                COUNT(DISTINCT ds) AS duration,
                SUM(gmv) AS gmv,
                SUM(order) AS order
            FROM lazada_analyst.loutruong_aff_b2c_ops_target_breakdown
            GROUP BY period,
                team
        ) AS t2 ON t1.period = t2.period
        AND t1.team_manage = t2.team_manage
)
SELECT t1.ds AS ds_target,
    t1.period AS period_target,
    t1.member_id,
    t1.member_name,
    t1.team,
    t1.plm_tier,
    t1.pc25_tier,
    t1.pic,
    t1.gmv_target,
    t1.order_target,
    t2.ds AS ds_baseline,
    COALESCE(t2.gmv_usd_lt_1d_p, 0) AS gmv_baseline,
    COALESCE(t2.order_cnt_lt_1d_p, 0) AS order_baseline
FROM (
        SELECT t2.ds AS ds,
            t2.period AS period,
            t1.member_id AS member_id,
            t1.member_name AS member_name,
            t1.team AS team,
            t1.plm_tier AS plm_tier,
            t1.pc25_tier AS pc25_tier,
            t1.pic AS pic,
            t3.gmv_target AS gmv_target,
            t3.order_target AS order_target
        FROM (
                SELECT member_id,
                    member_name,
                    team_manage AS team,
                    member_plm_local_lv2 AS plm_tier,
                    base_cms_tier,
                    pc25_tier,
                    member_pic_local AS pic,
                    1 AS map_key
                FROM lazada_analyst.loutruong_aff_mem_id_offline_info
                WHERE 1 = 1
                    AND month_apply IN (
                        SELECT MAX(month_apply)
                        FROM lazada_analyst.loutruong_aff_mem_id_offline_info
                    )
            ) AS t1
            INNER JOIN (
                SELECT DISTINCT ds,
                    period,
                    1 AS map_key
                FROM lazada_analyst.loutruong_aff_b2c_ops_target_breakdown
            ) AS t2 ON t1.map_key = t2.map_key
            INNER JOIN (
                SELECT t1.period AS period,
                    t1.member_id AS member_id,
                    t1.member_name AS member_name,
                    t1.team_manage AS team_manage,
                    t1.plm_tier AS plm_tier,
                    t1.pic AS pic,
                    t1.gmv_baseline AS gmv_baseline,
                    t2.gmv_baseline AS gmv_baseline_team,
                    t1.order_baseline AS order_baseline,
                    t2.order_baseline AS order_baseline_team,
                    t1.duration AS duration,
                    t1.gmv_target_team AS gmv_target_team,
CASE
                        WHEN t1.team_manage IN ('B2C-Acquisition') THEN t1.gmv_target_team / t2.mem_cnt / t1.duration
                        ELSE t1.gmv_baseline / t2.gmv_baseline * t1.gmv_target_team / t1.duration
                    END AS gmv_target,
                    t1.order_target_team AS order_target_team,
CASE
                        WHEN t1.team_manage IN ('B2C-Acquisition') THEN t1.order_target_team / t2.mem_cnt / t1.duration
                        ELSE t1.order_baseline / t2.order_baseline * t1.order_target_team / t1.duration
                    END AS order_target,
                    t2.mem_cnt
                FROM (
                        SELECT *
                        FROM t_member_id_master_raw
                    ) AS t1
                    INNER JOIN (
                        SELECT period,
                            team_manage,
                            SUM(gmv_baseline) AS gmv_baseline,
                            SUM(order_baseline) AS order_baseline,
                            COUNT(DISTINCT member_id) AS mem_cnt
                        FROM t_member_id_master_raw
                        GROUP BY period,
                            team_manage
                    ) AS t2 ON t1.period = t2.period
                    AND t1.team_manage = t2.team_manage
                ORDER BY t1.period DESC,
                    t1.team_manage DESC,
                    t1.plm_tier ASC,
                    t1.pic ASC,
                    t1.gmv_baseline DESC,
                    t1.order_baseline DESC
            ) AS t3 ON t2.period = t3.period
            AND t1.member_id = t3.member_id
    ) AS t1
    LEFT JOIN (
        SELECT ds,
            member_id,
            gmv_usd_lt_1d_p,
            order_cnt_lt_1d_p
        FROM lazada_analyst_dev.loutruong_aff_perf_plm_ops_di
        WHERE 1 = 1
            AND ds BETWEEN 20240301 AND 20240330
    ) AS t2 ON t1.ds = TO_CHAR(
        DATEADD(TO_DATE(t2.ds, 'yyyymmdd'), + 3, 'mm'),
        'yyyymmdd'
    )
    AND t1.member_id = t2.member_id
ORDER BY t1.ds DESC,
    t1.period ASC,
    t1.team DESC,
    t1.plm_tier ASC,
    t1.pc25_tier ASC,
    pic ASC,
    t1.gmv_target DESC;
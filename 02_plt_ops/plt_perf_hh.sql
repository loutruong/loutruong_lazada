-- MaxCompute SQL 
-- ********************************************************************--
-- author:Truong, Van Thanh
-- create time:2024-01-05 11:27:28
-- ********************************************************************--
--@@ Input = lazada_cdm.dws_lzd_mkt_app_ad_mp_di
--@@ Input = lazada_cdm.dwd_lzd_mkt_app_traffic_pv_di
--@@ Input = lazada_cdm.dwd_lzd_mkt_a2c_uam_core_di
--@@ Input = lazada_cdm.dws_lzd_mkt_trn_uam_di
--@@ Output = lazada_analyst_dev.tmp_loutruong_plt_perf_hh_1
-- DROP TABLE IF EXISTS lazada_analyst_dev.tmp_loutruong_plt_perf_hh_1
-- ;
-- CREATE TABLE IF NOT EXISTS lazada_analyst_dev.tmp_loutruong_plt_perf_hh_1
-- (
--     app_visit_cnt_ft_1d_np       DOUBLE COMMENT 'Performance base on attr_model = ft_1d_np'
--     ,app_dau_cnt_ft_1d_np         DOUBLE COMMENT 'Performance base on attr_model = ft_1d_np'
--     ,app_pdp_pv_ft_1d_np          DOUBLE COMMENT 'Performance base on attr_model = ft_1d_np'
--     ,app_pdp_uv_cnt_ft_1d_np      DOUBLE COMMENT 'Performance base on attr_model = ft_1d_np'
--     ,a2c_lt_1d_p                  DOUBLE COMMENT 'Performance base on attr_model = lt_1d_p'
--     ,a2c_uv_cnt_lt_1d_p           DOUBLE COMMENT 'Performance base on attr_model = lt_1d_p'
--     ,buyer_cnt_lt_1d_p            DOUBLE COMMENT 'Performance base on attr_model = lt_1d_p'
--     ,new_buyer_cnt_lt_1d_p        DOUBLE COMMENT 'Performance base on attr_model = lt_1d_p'
--     ,reacquired_buyer_cnt_lt_1d_p DOUBLE COMMENT 'Performance base on attr_model = lt_1d_p'
--     ,order_cnt_lt_1d_p            DOUBLE COMMENT 'Performance base on attr_model = lt_1d_p'
--     ,gmv_vnd_lt_1d_p              DOUBLE COMMENT 'Performance base on attr_model = lt_1d_p'
--     ,gmv_usd_lt_1d_p              DOUBLE COMMENT 'Performance base on attr_model = lt_1d_p'
-- )
-- PARTITIONED BY 
-- (
--     ds                            STRING COMMENT 'visit_time for trf, add_date for a2c, order_create_date for trn'
--     ,hh                           BIGINT COMMENT 'Min 00 Max 23'
-- )
-- LIFECYCLE 3600
-- ;
WITH t_trf_app_ads AS (
    SELECT ds,
        hh,
        SUM(SUM(app_visit_cnt)) OVER (
            PARTITION BY ds
            ORDER BY hh ASC
        ) AS app_visit_cnt,
        SUM(SUM(app_dau_cnt)) OVER (
            PARTITION BY ds
            ORDER BY hh ASC
        ) AS app_dau_cnt,
        SUM(SUM(app_pdp_pv)) OVER (
            PARTITION BY ds
            ORDER BY hh ASC
        ) AS app_pdp_pv,
        SUM(SUM(app_pdp_uv_cnt)) OVER (
            PARTITION BY ds
            ORDER BY hh ASC
        ) AS app_pdp_uv_cnt
    FROM (
            SELECT t1.ds AS ds,
                t2.hh AS hh,
                COUNT(
                    DISTINCT CASE
                        WHEN t1.is_active_utdid = 1 THEN t1.session_id
                        ELSE NULL
                    END
                ) AS app_visit_cnt,
                COUNT(
                    DISTINCT CASE
                        WHEN t1.is_active_utdid = 1 THEN t1.utdid
                        ELSE NULL
                    END
                ) AS app_dau_cnt,
                SUM(
                    CASE
                        WHEN t1.url_type IN ('ipv') THEN NVL(t1.pv_count, 0)
                        ELSE 0
                    END
                ) AS app_pdp_pv,
                COUNT(
                    DISTINCT CASE
                        WHEN t1.is_active_utdid = 1
                        AND t1.url_type IN ('ipv') THEN t1.utdid
                        ELSE NULL
                    END
                ) AS app_pdp_uv_cnt
            FROM (
                    SELECT ds,
                        is_active_utdid,
                        url_type,
                        session_id,
                        utdid,
                        pv_count
                    FROM lazada_cdm.dws_lzd_mkt_app_ad_mp_di
                    WHERE 1 = 1 --
                        -- AND     ds BETWEEN '${start_date}' AND '${end_date}' --<< Start
                        AND ds >= TO_CHAR(
                            DATEADD(TO_DATE('${bizdate}', 'yyyymmdd'), -3, 'dd'),
                            'yyyymmdd'
                        ) --<< Daily
                        AND venture = 'VN'
                        AND attr_model IN ('ft_1d_np')
                        AND utdid IS NOT NULL
                ) AS t1
                INNER JOIN (
                    SELECT ds,
                        MIN(
                            TO_CHAR(TO_DATE(visit_time, 'yyyymmddhhmiss'), 'hh')
                        ) AS hh,
                        utdid
                    FROM lazada_cdm.dwd_lzd_mkt_app_traffic_pv_di
                    WHERE 1 = 1 --
                        -- AND     ds BETWEEN '${start_date}' AND '${end_date}' --<< Start
                        AND ds >= TO_CHAR(
                            DATEADD(TO_DATE('${bizdate}', 'yyyymmdd'), -3, 'dd'),
                            'yyyymmdd'
                        ) --<< Daily
                        AND venture = 'VN'
                        AND utdid IS NOT NULL
                        AND TOLOWER(sg_udf :bi_get_aplus_appinfo(app_id, 'code')) IN ('lazada')
                    GROUP BY ds,
                        utdid
                ) AS t2 ON t1.ds = t2.ds
                AND t1.utdid = t2.utdid
            GROUP BY t1.ds,
                t2.hh
        )
    GROUP BY ds,
        hh
),
t_a2c_ads AS (
    SELECT ds,
        hh,
        SUM(SUM(a2c)) OVER (
            PARTITION BY ds
            ORDER BY hh ASC
        ) AS a2c,
        SUM(SUM(a2c_uv_cnt)) OVER (
            PARTITION BY ds
            ORDER BY hh ASC
        ) AS a2c_uv_cnt
    FROM (
            SELECT ds,
                hh,
                SUM(quantity_lt_1d_p) AS a2c,
                COUNT(
                    DISTINCT CASE
                        WHEN buyer_id__dedup = 1 THEN buyer_id
                        ELSE NULL
                    END
                ) AS a2c_uv_cnt
            FROM (
                    SELECT ds,
                        TO_CHAR(add_date, 'hh') AS hh,
                        buyer_id,
                        ROW_NUMBER() OVER (
                            PARTITION BY ds,
                            buyer_id
                            ORDER BY add_date ASC
                        ) AS buyer_id__dedup,
                        quantity AS quantity_lt_1d_p
                    FROM lazada_cdm.dwd_lzd_mkt_a2c_uam_core_di
                    WHERE 1 = 1 --
                        -- AND     ds BETWEEN '${start_date}' AND '${end_date}' --<< Start
                        AND ds >= TO_CHAR(
                            DATEADD(TO_DATE('${bizdate}', 'yyyymmdd'), -3, 'dd'),
                            'yyyymmdd'
                        ) --<< Daily
                        AND venture = 'VN'
                        AND attr_model IN ('lt_1d_p')
                )
            GROUP BY ds,
                hh
        )
    GROUP BY ds,
        hh
),
t_trn_ads AS (
    SELECT ds,
        hh,
        SUM(SUM(buyer_cnt)) OVER (
            PARTITION BY ds
            ORDER BY hh ASC
        ) AS buyer_cnt,
        SUM(SUM(new_buyer_cnt)) OVER (
            PARTITION BY ds
            ORDER BY hh ASC
        ) AS new_buyer_cnt,
        SUM(SUM(reacquired_buyer_cnt)) OVER (
            PARTITION BY ds
            ORDER BY hh ASC
        ) AS reacquired_buyer_cnt,
        SUM(SUM(order_id_cnt)) OVER (
            PARTITION BY ds
            ORDER BY hh ASC
        ) AS order_id_cnt,
        SUM(SUM(gmv_vnd)) OVER (
            PARTITION BY ds
            ORDER BY hh ASC
        ) AS gmv_vnd,
        SUM(SUM(gmv_usd)) OVER (
            PARTITION BY ds
            ORDER BY hh ASC
        ) AS gmv_usd
    FROM (
            SELECT ds,
                hh,
                COUNT(
                    DISTINCT CASE
                        WHEN buyer_id_dedup = 1 THEN buyer_id
                        ELSE NULL
                    END
                ) AS buyer_cnt,
                COUNT(
                    DISTINCT CASE
                        WHEN new_buyer_id_dedup = 1 THEN buyer_id
                        ELSE NULL
                    END
                ) AS new_buyer_cnt,
                COUNT(
                    DISTINCT CASE
                        WHEN reacquired_buyer_id_dedup = 1 THEN buyer_id
                        ELSE NULL
                    END
                ) AS reacquired_buyer_cnt,
                COUNT(DISTINCT order_id) AS order_id_cnt,
                SUM(gmv_vnd) AS gmv_vnd,
                SUM(gmv_usd) AS gmv_usd
            FROM (
                    SELECT TO_CHAR(created_date, 'yyyymmdd') AS ds,
                        TO_CHAR(created_date, 'hh') AS hh,
                        buyer_id AS buyer_id,
                        ROW_NUMBER() OVER (
                            PARTITION BY ds,
                            buyer_id
                            ORDER BY created_date ASC
                        ) AS buyer_id_dedup,
CASE
                            WHEN is_first_order = 1 THEN ROW_NUMBER() OVER (
                                PARTITION BY ds,
                                buyer_id,
                                is_first_order = 1
                                ORDER BY created_date ASC
                            )
                            ELSE NULL
                        END AS new_buyer_id_dedup,
CASE
                            WHEN is_reacquired_buyer = 1 THEN ROW_NUMBER() OVER (
                                PARTITION BY ds,
                                buyer_id,
                                is_reacquired_buyer = 1
                                ORDER BY created_date ASC
                            )
                            ELSE NULL
                        END AS reacquired_buyer_id_dedup,
                        CONCAT(sales_order_id, sku_id) AS order_id,
                        COALESCE(actual_gmv, 0) AS gmv_vnd,
                        COALESCE(actual_gmv, 0) * COALESCE(exchange_rate, 0) AS gmv_usd
                    FROM lazada_cdm.dws_lzd_mkt_trn_uam_di
                    WHERE 1 = 1 --
                        -- AND     ds >= TO_CHAR(DATEADD(TO_DATE('${start_date}','yyyymmdd'),-5,'dd'),'yyyymmdd') --<< Start
                        AND ds >= TO_CHAR(
                            DATEADD(TO_DATE('${bizdate}', 'yyyymmdd'), -3, 'dd'),
                            'yyyymmdd'
                        ) --<< Daily
                        AND venture = 'VN' --
                        -- AND     TO_CHAR(created_date,'yyyymmdd') BETWEEN '${start_date}' AND '${end_date}' --<< Start
                        AND TO_CHAR(created_date, 'yyyymmdd') >= TO_CHAR(
                            DATEADD(TO_DATE('${bizdate}', 'yyyymmdd'), -3, 'dd'),
                            'yyyymmdd'
                        ) --<< Daily
                        AND attr_model IN ('lt_1d_p')
                )
            GROUP BY ds,
                hh
        )
    GROUP BY ds,
        hh
) --
INSERT OVERWRITE TABLE lazada_analyst_dev.tmp_loutruong_plt_perf_hh_1 PARTITION (ds, hh)
SELECT SUM(app_visit_cnt) AS app_visit_cnt_ft_1d_np,
    SUM(app_dau_cnt) AS app_dau_cnt_ft_1d_np,
    SUM(app_pdp_pv) AS app_pdp_pv_ft_1d_np,
    SUM(app_pdp_uv_cnt) AS app_pdp_uv_cnt_ft_1d_np,
    SUM(a2c) AS a2c_lt_1d_p,
    SUM(a2c_uv_cnt) AS a2c_uv_cnt_lt_1d_p,
    SUM(buyer_cnt) AS buyer_cnt_lt_1d_p,
    SUM(new_buyer_cnt) AS new_buyer_cnt_lt_1d_p,
    SUM(reacquired_buyer_cnt) AS reacquired_buyer_cnt_lt_1d_p,
    SUM(order_id_cnt) AS order_cnt_lt_1d_p,
    SUM(gmv_vnd) AS gmv_vnd_lt_1d_p,
    SUM(gmv_usd) AS gmv_usd_lt_1d_p,
    ds,
    hh
FROM (
        SELECT ds,
            hh,
            app_visit_cnt,
            app_dau_cnt,
            app_pdp_pv,
            app_pdp_uv_cnt,
            0 AS a2c,
            0 AS a2c_uv_cnt,
            0 AS buyer_cnt,
            0 AS new_buyer_cnt,
            0 AS reacquired_buyer_cnt,
            0 AS order_id_cnt,
            0 AS gmv_vnd,
            0 AS gmv_usd
        FROM t_trf_app_ads
        UNION ALL
        SELECT ds,
            hh,
            0 AS app_visit_cnt,
            0 AS app_dau_cnt,
            0 AS app_pdp_pv,
            0 AS app_pdp_uv_cnt,
            a2c,
            a2c_uv_cnt,
            0 AS buyer_cnt,
            0 AS new_buyer_cnt,
            0 AS reacquired_buyer_cnt,
            0 AS order_id_cnt,
            0 AS gmv_vnd,
            0 AS gmv_usd
        FROM t_a2c_ads
        UNION ALL
        SELECT ds,
            hh,
            0 AS app_visit_cnt,
            0 AS app_dau_cnt,
            0 AS app_pdp_pv,
            0 AS app_pdp_uv_cnt,
            0 AS a2c,
            0 AS a2c_uv_cnt,
            buyer_cnt,
            new_buyer_cnt,
            reacquired_buyer_cnt,
            order_id_cnt,
            gmv_vnd,
            gmv_usd
        FROM t_trn_ads
    )
GROUP BY ds,
    hh;
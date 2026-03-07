-- MaxCompute SQL 
-- ********************************************************************--
-- author:Truong, Van Thanh
-- create time:2024-05-10 12:46:23
-- ********************************************************************--
--@@ Input = lazada_cdm.dwd_lzd_clickserver_log_di
--@@ Input = lazada_cdm.dws_lzd_mkt_app_ad_mp_di
--@@ Input = lazada_cdm.dwd_lzd_mkt_a2c_uam_core_di
--@@ Input = lazada_cdm.dws_lzd_mkt_trn_uam_di
--@@ Input = lazada_ads.dim_lzd_mkt_affiliate_biz_type_df
--@@ Input = lazada_cdm.dws_lzd_asset_usr_utdid_nonbuyer_segment_1d
--@@ Input Exclude = t_app_traffic
--@@ Input Exclude = t_a2c
--@@ Input Exclude = t_trn
--@@ Input = lazada_ads_data.ads_lzd_cps_partner_info_df
--@@ Output = lazada_analyst_dev.loutruong_aff_sub_id_perf
----------------------------------------------------------------------------------------------------------------------------
-- Raw level
----------------------------------------------------------------------------------------------------------------------------
-- DROP TABLE IF EXISTS lazada_analyst_dev.loutruong_aff_sub_id_perf
-- ;
-- CREATE TABLE IF NOT EXISTS lazada_analyst_dev.loutruong_aff_sub_id_perf
-- (
--     member_id                     STRING
--     ,sub_aff_id                   STRING
--     ,sub_id1                      STRING
--     ,sub_id2                      STRING
--     ,sub_id3                      STRING
--     ,sub_id4                      STRING
--     ,sub_id5                      STRING
--     ,sub_id6                      STRING
--     ,app_click_cnt                DOUBLE
--     ,app_visit_cnt_any_touch      DOUBLE
--     ,app_visit_cnt_ft_1d_np       DOUBLE
--     ,app_dau_cnt_any_touch        DOUBLE
--     ,app_dau_cnt_ft_1d_np         DOUBLE
--     ,app_pdp_pv_ft_1d_np          DOUBLE
--     ,app_pdp_uv_cnt_ft_1d_np      DOUBLE
--     ,a2c_cnt_lt_1d_p              DOUBLE
--     ,a2c_uv_cnt_lt_1d_p           DOUBLE
--     ,buyer_cnt_lt_1d_p            DOUBLE
--     ,new_buyer_cnt_lt_1d_p        DOUBLE
--     ,reacquired_buyer_cnt_lt_1d_p DOUBLE
--     ,order_cnt_lt_1d_p            DOUBLE
--     ,gmv_usd_lt_1d_p              DOUBLE
-- )
-- PARTITIONED BY 
-- (
--     ds                            STRING
-- )
-- LIFECYCLE 3600
-- ;
WITH t_click_ads AS (
    SELECT ds,
        member_id,
        sub_aff_id,
        sub_id1,
        sub_id2,
        sub_id3,
        sub_id4,
        sub_id5,
        sub_id6,
        COUNT(*) AS app_click_cnt
    FROM (
            SELECT ds,
                member_id,
                clickid,
                sub_aff_id,
                sub_id1,
                sub_id2,
                sub_id3,
                sub_id4,
                sub_id5,
                sub_id6,
                MAX(is_app) AS is_app
            FROM (
                    SELECT ds,
                        COALESCE(SPLIT(pid, '_') [1], '') AS member_id,
                        clickid,
                        sub_aff_id,
                        sub_id1,
                        sub_id2,
                        sub_id3,
                        sub_id4,
                        sub_id5,
                        sub_id6,
                        IF(LENGTH(utdid) > 0, 1, 0) AS is_app
                    FROM lazada_cdm.dwd_lzd_clickserver_log_di
                    WHERE 1 = 1 --
                        -- AND     ds BETWEEN '${start_date}' AND '${end_date}' --<< Start
                        AND ds >= TO_CHAR(
                            DATEADD(TO_DATE('${bizdate}', 'yyyymmdd'), -3, 'dd'),
                            'yyyymmdd'
                        ) --<< Daily
                        AND venture = 'VN'
                        AND bm = 2
                        AND old_clickid IS NULL
                    UNION ALL
                    SELECT ds,
                        COALESCE(SPLIT(pid, '_') [1], '') AS member_id,
                        old_clickid AS clickid,
                        sub_aff_id,
                        sub_id1,
                        sub_id2,
                        sub_id3,
                        sub_id4,
                        sub_id5,
                        sub_id6,
                        1 AS is_app
                    FROM lazada_cdm.dwd_lzd_clickserver_log_di
                    WHERE 1 = 1 --
                        -- AND     ds BETWEEN '${start_date}' AND '${end_date}' --<< Start
                        AND ds >= TO_CHAR(
                            DATEADD(TO_DATE('${bizdate}', 'yyyymmdd'), -3, 'dd'),
                            'yyyymmdd'
                        ) --<< Daily
                        AND venture = 'VN'
                        AND bm = 2
                        AND old_clickid IS NOT NULL
                )
            GROUP BY ds,
                member_id,
                clickid,
                sub_aff_id,
                sub_id1,
                sub_id2,
                sub_id3,
                sub_id4,
                sub_id5,
                sub_id6
        )
    GROUP BY ds,
        member_id,
        sub_aff_id,
        sub_id1,
        sub_id2,
        sub_id3,
        sub_id4,
        sub_id5,
        sub_id6
),
t_app_traffic_ads AS (
    SELECT ds,
        member_id,
        sub_aff_id,
        sub_id1,
        sub_id2,
        sub_id3,
        sub_id4,
        sub_id5,
        sub_id6,
        COUNT(
            DISTINCT CASE
                WHEN attr_model IN ('pc_session')
                AND is_active_utdid = 1 THEN session_id
                ELSE NULL
            END
        ) AS app_visit_cnt_any_touch,
        COUNT(
            DISTINCT CASE
                WHEN attr_model IN ('ft_1d_np')
                AND is_active_utdid = 1 THEN session_id
                ELSE NULL
            END
        ) AS app_visit_cnt_ft_1d_np,
        COUNT(
            DISTINCT CASE
                WHEN attr_model IN ('pc_session')
                AND is_active_utdid = 1 THEN utdid
                ELSE NULL
            END
        ) AS app_dau_cnt_any_touch,
        COUNT(
            DISTINCT CASE
                WHEN attr_model IN ('ft_1d_np')
                AND is_active_utdid = 1 THEN utdid
                ELSE NULL
            END
        ) AS app_dau_cnt_ft_1d_np,
        SUM(
            CASE
                WHEN attr_model IN ('ft_1d_np')
                AND url_type IN ('ipv') THEN COALESCE(NVL(pv_count, 0), 0)
                ELSE 0
            END
        ) AS app_pdp_pv_ft_1d_np,
        COUNT(
            DISTINCT CASE
                WHEN attr_model IN ('ft_1d_np')
                AND is_active_utdid = 1
                AND url_type IN ('ipv') THEN utdid
                ELSE NULL
            END
        ) AS app_pdp_uv_cnt_ft_1d_np
    FROM lazada_cdm.dws_lzd_mkt_app_ad_mp_di
    WHERE 1 = 1 --
        -- AND     ds BETWEEN '${start_date}' AND '${end_date}' --<< Start
        AND ds >= TO_CHAR(
            DATEADD(TO_DATE('${bizdate}', 'yyyymmdd'), -3, 'dd'),
            'yyyymmdd'
        ) --<< Daily
        AND venture = 'VN'
        AND TOLOWER(channel) IN ('cps affiliate', 'cpi affiliate')
        AND attr_model IN ('ft_1d_np', 'pc_session')
        AND utdid IS NOT NULL
    GROUP BY ds,
        member_id,
        sub_aff_id,
        sub_id1,
        sub_id2,
        sub_id3,
        sub_id4,
        sub_id5,
        sub_id6
),
t_a2c_ads AS (
    SELECT ds,
        member_id,
        sub_aff_id,
        sub_id1,
        sub_id2,
        sub_id3,
        sub_id4,
        sub_id5,
        sub_id6,
        SUM(quantity) AS a2c_cnt_lt_1d_p,
        COUNT(DISTINCT buyer_id) AS a2c_uv_cnt_lt_1d_p
    FROM lazada_cdm.dwd_lzd_mkt_a2c_uam_core_di
    WHERE 1 = 1 --
        -- AND     ds BETWEEN '${start_date}' AND '${end_date}' --<< Start
        AND ds >= TO_CHAR(
            DATEADD(TO_DATE('${bizdate}', 'yyyymmdd'), -3, 'dd'),
            'yyyymmdd'
        ) --<< Daily
        AND venture = 'VN'
        AND TOLOWER(channel) IN ('cps affiliate', 'cpi affiliate')
        AND attr_model IN ('lt_1d_p')
    GROUP BY ds,
        member_id,
        sub_aff_id,
        sub_id1,
        sub_id2,
        sub_id3,
        sub_id4,
        sub_id5,
        sub_id6
),
t_trn_ads AS (
    SELECT TO_CHAR(fulfillment_create_date, 'yyyymmdd') AS ds,
        member_id,
        sub_aff_id,
        sub_id1,
        sub_id2,
        sub_id3,
        sub_id4,
        sub_id5,
        sub_id6,
        COUNT(DISTINCT buyer_id) AS buyer_cnt_lt_1d_p,
        COUNT(
            DISTINCT CASE
                WHEN is_first_order = 1 THEN buyer_id
                ELSE NULL
            END
        ) AS new_buyer_cnt_lt_1d_p,
        COUNT(
            DISTINCT CASE
                WHEN is_reacquired_buyer = 1 THEN buyer_id
            END
        ) AS reacquired_buyer_cnt_lt_1d_p,
        COUNT(DISTINCT sales_order_id, sku_id) AS order_cnt_lt_1d_p,
        SUM(COALESCE(actual_gmv, 0) * exchange_rate) AS gmv_usd_lt_1d_p
    FROM lazada_cdm.dws_lzd_mkt_trn_uam_di
    WHERE 1 = 1 --
        -- AND     ds >= TO_CHAR(DATEADD(TO_DATE('${start_date}','yyyymmdd'),-5,'dd'),'yyyymmdd') --<< Start
        AND ds >= TO_CHAR(
            DATEADD(TO_DATE('${bizdate}', 'yyyymmdd'), -8, 'dd'),
            'yyyymmdd'
        ) --<< Daily
        AND venture = 'VN' --
        -- AND     TO_CHAR(fulfillment_create_date,'yyyymmdd') BETWEEN '${start_date}' AND '${end_date}' --<< Start
        AND TO_CHAR(fulfillment_create_date, 'yyyymmdd') >= TO_CHAR(
            DATEADD(TO_DATE('${bizdate}', 'yyyymmdd'), -3, 'dd'),
            'yyyymmdd'
        ) --<< Daily
        AND TOLOWER(channel) IN ('cps affiliate', 'cpi affiliate')
        AND attr_model IN ('lt_1d_p')
    GROUP BY TO_CHAR(fulfillment_create_date, 'yyyymmdd'),
        member_id,
        sub_aff_id,
        sub_id1,
        sub_id2,
        sub_id3,
        sub_id4,
        sub_id5,
        sub_id6
),
t_agg AS (
    SELECT ds,
        member_id,
        sub_aff_id,
        sub_id1,
        sub_id2,
        sub_id3,
        sub_id4,
        sub_id5,
        sub_id6,
        SUM(app_click_cnt) AS app_click_cnt,
        SUM(app_visit_cnt_any_touch) AS app_visit_cnt_any_touch,
        SUM(app_visit_cnt_ft_1d_np) AS app_visit_cnt_ft_1d_np,
        SUM(app_dau_cnt_any_touch) AS app_dau_cnt_any_touch,
        SUM(app_dau_cnt_ft_1d_np) AS app_dau_cnt_ft_1d_np,
        SUM(app_pdp_pv_ft_1d_np) AS app_pdp_pv_ft_1d_np,
        SUM(app_pdp_uv_cnt_ft_1d_np) AS app_pdp_uv_cnt_ft_1d_np,
        SUM(a2c_cnt_lt_1d_p) AS a2c_cnt_lt_1d_p,
        SUM(a2c_uv_cnt_lt_1d_p) AS a2c_uv_cnt_lt_1d_p,
        SUM(buyer_cnt_lt_1d_p) AS buyer_cnt_lt_1d_p,
        SUM(new_buyer_cnt_lt_1d_p) AS new_buyer_cnt_lt_1d_p,
        SUM(reacquired_buyer_cnt_lt_1d_p) AS reacquired_buyer_cnt_lt_1d_p,
        SUM(order_cnt_lt_1d_p) AS order_cnt_lt_1d_p,
        SUM(gmv_usd_lt_1d_p) AS gmv_usd_lt_1d_p
    FROM (
            SELECT ds,
                member_id,
                sub_aff_id,
                sub_id1,
                sub_id2,
                sub_id3,
                sub_id4,
                sub_id5,
                sub_id6,
                app_click_cnt,
                0 AS app_visit_cnt_any_touch,
                0 AS app_visit_cnt_ft_1d_np,
                0 AS app_dau_cnt_any_touch,
                0 AS app_dau_cnt_ft_1d_np,
                0 AS app_pdp_pv_ft_1d_np,
                0 AS app_pdp_uv_cnt_ft_1d_np,
                0 AS a2c_cnt_lt_1d_p,
                0 AS a2c_uv_cnt_lt_1d_p,
                0 AS buyer_cnt_lt_1d_p,
                0 AS new_buyer_cnt_lt_1d_p,
                0 AS reacquired_buyer_cnt_lt_1d_p,
                0 AS order_cnt_lt_1d_p,
                0 AS gmv_usd_lt_1d_p
            FROM t_click_ads
            UNION ALL
            SELECT ds,
                member_id,
                sub_aff_id,
                sub_id1,
                sub_id2,
                sub_id3,
                sub_id4,
                sub_id5,
                sub_id6,
                0 AS app_click_cnt,
                app_visit_cnt_any_touch,
                app_visit_cnt_ft_1d_np,
                app_dau_cnt_any_touch,
                app_dau_cnt_ft_1d_np,
                app_pdp_pv_ft_1d_np,
                app_pdp_uv_cnt_ft_1d_np,
                0 AS a2c_cnt_lt_1d_p,
                0 AS a2c_uv_cnt_lt_1d_p,
                0 AS buyer_cnt_lt_1d_p,
                0 AS new_buyer_cnt_lt_1d_p,
                0 AS reacquired_buyer_cnt_lt_1d_p,
                0 AS order_cnt_lt_1d_p,
                0 AS gmv_usd_lt_1d_p
            FROM t_app_traffic_ads
            UNION ALL
            SELECT ds,
                member_id,
                sub_aff_id,
                sub_id1,
                sub_id2,
                sub_id3,
                sub_id4,
                sub_id5,
                sub_id6,
                0 AS app_click_cnt,
                0 AS app_visit_cnt_any_touch,
                0 AS app_visit_cnt_ft_1d_np,
                0 AS app_dau_cnt_any_touch,
                0 AS app_dau_cnt_ft_1d_np,
                0 AS app_pdp_pv_ft_1d_np,
                0 AS app_pdp_uv_cnt_ft_1d_np,
                a2c_cnt_lt_1d_p,
                a2c_uv_cnt_lt_1d_p,
                0 AS buyer_cnt_lt_1d_p,
                0 AS new_buyer_cnt_lt_1d_p,
                0 AS reacquired_buyer_cnt_lt_1d_p,
                0 AS order_cnt_lt_1d_p,
                0 AS gmv_usd_lt_1d_p
            FROM t_a2c_ads
            UNION ALL
            SELECT ds,
                member_id,
                sub_aff_id,
                sub_id1,
                sub_id2,
                sub_id3,
                sub_id4,
                sub_id5,
                sub_id6,
                0 AS app_click_cnt,
                0 AS app_visit_cnt_any_touch,
                0 AS app_visit_cnt_ft_1d_np,
                0 AS app_dau_cnt_any_touch,
                0 AS app_dau_cnt_ft_1d_np,
                0 AS app_pdp_pv_ft_1d_np,
                0 AS app_pdp_uv_cnt_ft_1d_np,
                0 AS a2c_cnt_lt_1d_p,
                0 AS a2c_uv_cnt_lt_1d_p,
                buyer_cnt_lt_1d_p,
                new_buyer_cnt_lt_1d_p,
                reacquired_buyer_cnt_lt_1d_p,
                order_cnt_lt_1d_p,
                gmv_usd_lt_1d_p
            FROM t_trn_ads
        )
    GROUP BY ds,
        member_id,
        sub_aff_id,
        sub_id1,
        sub_id2,
        sub_id3,
        sub_id4,
        sub_id5,
        sub_id6
) --
INSERT OVERWRITE TABLE lazada_analyst_dev.loutruong_aff_sub_id_perf PARTITION (ds)
SELECT member_id,
    sub_aff_id,
    sub_id1,
    sub_id2,
    sub_id3,
    sub_id4,
    sub_id5,
    sub_id6,
    app_click_cnt,
    app_visit_cnt_any_touch,
    app_visit_cnt_ft_1d_np,
    app_dau_cnt_any_touch,
    app_dau_cnt_ft_1d_np,
    app_pdp_pv_ft_1d_np,
    app_pdp_uv_cnt_ft_1d_np,
    a2c_cnt_lt_1d_p,
    a2c_uv_cnt_lt_1d_p,
    buyer_cnt_lt_1d_p,
    new_buyer_cnt_lt_1d_p,
    reacquired_buyer_cnt_lt_1d_p,
    order_cnt_lt_1d_p,
    gmv_usd_lt_1d_p,
    ds
FROM t_agg;
----------------------------------------------------------------------------------------------------------------------------
-- Aggregated level
----------------------------------------------------------------------------------------------------------------------------
-- SELECT  TO_CHAR(TO_DATE(t1.ds,'yyyymmdd'),'yyyy-mm-dd') AS ds
--         ,COALESCE(t2.group_segment,'Unknown_group_segment') AS group_segment
--         ,COALESCE(t2.segment,'Unknown_segment') AS segment
--         ,t1.member_id AS member_id
--         ,t2.member_name AS member_name
--         ,t1.sub_aff_id AS sub_aff_id
--         ,t1.sub_id1 AS sub_id1
--         ,t1.sub_id2 AS sub_id2
--         ,t1.sub_id3 AS sub_id3
--         ,t1.sub_id4 AS sub_id4
--         ,t1.sub_id5 AS sub_id5
--         ,t1.sub_id6 AS sub_id6
--         ,t1.app_click_cnt AS app_click_cnt
--         ,t1.app_visit_cnt_any_touch AS app_visit_cnt_any_touch
--         ,t1.app_visit_cnt_ft_1d_np AS app_visit_cnt_ft_1d_np
--         ,t1.app_dau_cnt_any_touch AS app_dau_cnt_any_touch
--         ,t1.app_dau_cnt_ft_1d_np AS app_dau_cnt_ft_1d_np
--         ,t1.app_pdp_pv_ft_1d_np AS app_pdp_pv_ft_1d_np
--         ,t1.app_pdp_uv_cnt_ft_1d_np AS app_pdp_uv_cnt_ft_1d_np
--         ,t1.a2c_cnt_lt_1d_p AS a2c_cnt_lt_1d_p
--         ,t1.a2c_uv_cnt_lt_1d_p AS a2c_uv_cnt_lt_1d_p
--         ,t1.buyer_cnt_lt_1d_p AS buyer_cnt_lt_1d_p
--         ,t1.new_buyer_cnt_lt_1d_p AS new_buyer_cnt_lt_1d_p
--         ,t1.reacquired_buyer_cnt_lt_1d_p AS reacquired_buyer_cnt_lt_1d_p
--         ,t1.order_cnt_lt_1d_p AS order_cnt_lt_1d_p
--         ,t1.gmv_usd_lt_1d_p AS gmv_usd_lt_1d_p
-- FROM    (
--             SELECT  ds
--                     ,member_id
--                     ,sub_aff_id
--                     ,sub_id1
--                     ,sub_id2
--                     ,sub_id3
--                     ,sub_id4
--                     ,sub_id5
--                     ,sub_id6
--                     ,app_click_cnt
--                     ,app_visit_cnt_any_touch
--                     ,app_visit_cnt_ft_1d_np
--                     ,app_dau_cnt_any_touch
--                     ,app_dau_cnt_ft_1d_np
--                     ,app_pdp_pv_ft_1d_np
--                     ,app_pdp_uv_cnt_ft_1d_np
--                     ,a2c_cnt_lt_1d_p
--                     ,a2c_uv_cnt_lt_1d_p
--                     ,buyer_cnt_lt_1d_p
--                     ,new_buyer_cnt_lt_1d_p
--                     ,reacquired_buyer_cnt_lt_1d_p
--                     ,order_cnt_lt_1d_p
--                     ,gmv_usd_lt_1d_p
--             FROM    lazada_analyst_dev.loutruong_aff_sub_id_perf
--             WHERE   1 = 1
--             AND     ds >= TO_CHAR(DATEADD(GETDATE(),-11,'dd'),'yyyymmdd')
--             AND     member_id IN (
--                         SELECT  DISTINCT member_id
--                         FROM    lazada_ads_data.ads_lzd_cps_partner_info_df
--                         WHERE   1 = 1
--                         AND     ds = MAX_PT('lazada_ads_data.ads_lzd_cps_partner_info_df')
--                         AND     venture = 'VN'
--                     ) 
--         ) AS t1
-- LEFT JOIN   (
--                 SELECT  CASE    WHEN TOLOWER(CLUSTER) LIKE '%b2c%' THEN 'B2C'
--                                 WHEN TOLOWER(CLUSTER) LIKE '%b2b%' THEN 'B2B'
--                                 WHEN TOLOWER(CLUSTER) LIKE '%seller affiliates%' THEN 'Seller affiliates'
--                         END AS group_segment
--                         ,segment
--                         ,member_id
--                         ,member_name
--                 FROM    lazada_ads_data.ads_lzd_cps_partner_info_df
--                 WHERE   1 = 1
--                 AND     ds = MAX_PT('lazada_ads_data.ads_lzd_cps_partner_info_df')
--                 AND     venture = 'VN'
--             ) AS t2
-- ON      t1.member_id = t2.member_id
-- WHERE   1 = 1
-- AND     t2.member_id IS NOT NULL
-- AND     t2.group_segment NOT IN ('Seller affiliates','B2B')
-- ;
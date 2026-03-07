-- MaxCompute SQL 
-- ********************************************************************--
-- author:Truong, Van Thanh
-- create time:2024-05-19 11:55:37
-- ********************************************************************--
--@@ Input = lazada_analyst_dev.loutruong_dim_hh
--@@ Input = lazada_cdm.dws_lzd_mkt_app_ad_mp_di
--@@ Input = lazada_cdm.dwd_lzd_mkt_app_traffic_pv_di
--@@ Input = lazada_cdm.dwd_lzd_mkt_a2c_uam_core_di
--@@ Input = lazada_cdm.dws_lzd_mkt_trn_uam_di
--@@ Include-exclude = t_trf_app
--@@ Include-exclude = t_a2c
--@@ Include-exclude = t_trn
--@@ Include-exclude = t_trf_app_ads
--@@ Include-exclude = t_a2c_ads
--@@ Include-exclude = t_trn_ads
--@@ Include-exclude = t_agg
--@@ Output = lazada_analyst_dev.tmp_loutruong_aff_perf_hh_1
-- DROP TABLE IF EXISTS lazada_analyst_dev.tmp_loutruong_aff_perf_hh_1
-- ;
-- CREATE TABLE IF NOT EXISTS lazada_analyst_dev.tmp_loutruong_aff_perf_hh_1
-- (
--     member_id                     STRING COMMENT 'Member ID has performance'
--     ,engaged_member_id_pc_session STRING COMMENT 'Member ID has app_visit base on attr_model = pc_session/any_touch'
--     ,engaged_member_id_lt_1d      STRING COMMENT 'Member ID has app_visit base on attr_model = lt_1d'
--     ,engaged_member_id_ft_1d_np   STRING COMMENT 'Member ID has app_visit base on attr_model = ft_1d_np'
--     ,active_member_id_lt_1d_p     STRING COMMENT 'Member ID has order_id_cnt base on attr_model = lt_1d_p'
--     ,app_visit_cnt_pc_session     DOUBLE COMMENT 'Performance base on attr_model = pc_session/any_touch'
--     ,app_visit_cnt_lt_1d          DOUBLE COMMENT 'Performance base on attr_model = lt_1d'
--     ,app_visit_cnt_ft_1d_np       DOUBLE COMMENT 'Performance base on attr_model = ft_1d_np'
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
-- COMMENT 'Intraday historical performance accumulatived and deduplicated of VN Affiliate operation x Multiple attr_model'
-- PARTITIONED BY 
-- (
--     ds                            STRING COMMENT 'visit_time for trf, add_date for a2c, order_create_date for trn'
--     ,hh                           BIGINT COMMENT 'Min 00 Max 23'
-- )
-- LIFECYCLE 3600
-- ;
WITH t_trf_app AS (
    SELECT t1.ds AS ds,
        t2.hh AS hh,
        t1.member_id AS member_id,
        COUNT(
            DISTINCT CASE
                WHEN t1.attr_model IN ('pc_session')
                AND t1.is_active_utdid = 1 THEN t1.session_id
                ELSE NULL
            END
        ) AS app_visit_cnt_pc_session,
        COUNT(
            DISTINCT CASE
                WHEN t1.attr_model IN ('lt_1d')
                AND t1.is_active_utdid = 1 THEN t1.session_id
                ELSE NULL
            END
        ) AS app_visit_cnt_lt_1d,
        COUNT(
            DISTINCT CASE
                WHEN t1.attr_model IN ('ft_1d_np')
                AND t1.is_active_utdid = 1 THEN t1.session_id
                ELSE NULL
            END
        ) AS app_visit_cnt_ft_1d_np,
        COUNT(
            DISTINCT CASE
                WHEN t1.attr_model IN ('ft_1d_np')
                AND t1.is_active_utdid = 1 THEN t1.utdid
                ELSE NULL
            END
        ) AS app_dau_cnt_ft_1d_np,
        SUM(
            CASE
                WHEN t1.attr_model IN ('ft_1d_np')
                AND t1.url_type IN ('ipv') THEN NVL(t1.pv_count, 0)
                ELSE 0
            END
        ) AS app_pdp_pv_ft_1d_np,
        COUNT(
            DISTINCT CASE
                WHEN t1.attr_model IN ('ft_1d_np')
                AND t1.is_active_utdid = 1
                AND t1.url_type IN ('ipv') THEN t1.utdid
                ELSE NULL
            END
        ) AS app_pdp_uv_cnt_ft_1d_np
    FROM (
            SELECT ds,
                attr_model,
                member_id,
                is_active_utdid,
                utdid,
                session_id,
                url_type,
                pv_count
            FROM lazada_cdm.dws_lzd_mkt_app_ad_mp_di
            WHERE 1 = 1 --
                -- AND     ds BETWEEN '${start_date}' AND '${end_date}' --<< Start
                AND ds >= TO_CHAR(
                    DATEADD(TO_DATE('${bizdate}', 'yyyymmdd'), -3, 'dd'),
                    'yyyymmdd'
                ) --<< Daily
                AND venture = 'VN'
                AND attr_model IN ('pc_session', 'lt_1d', 'ft_1d_np')
                AND utdid IS NOT NULL
                AND TOLOWER(
                    COALESCE(
                        lazada_cdm.mkt_get_updated_funding_bucket(
                            channel,
                            GET_JSON_OBJECT(campaign_info, '$.funding_bucket'),
                            partner
                        ),
                        'Unknown'
                    )
                ) IN ('lazada om')
                AND TOLOWER(
                    COALESCE(
                        lazada_cdm.mkt_get_updated_funding_type(
                            channel,
                            GET_JSON_OBJECT(campaign_info, '$.funding_bucket'),
                            GET_JSON_OBJECT(campaign_info, '$.funding_type'),
                            partner
                        ),
                        'Unknown'
                    )
                ) IN ('om', 'ams')
                AND TOLOWER(
                    lazada_cdm.mkt_get_sub_channel_from_json(
                        sg_udf :bi_put_json_values(
                            '{}',
                            'channel',
                            sg_udf :bi_to_json_string(channel),
                            'funding_bucket',
                            sg_udf :bi_to_json_string(
                                lazada_cdm.mkt_get_updated_funding_bucket(
                                    channel,
                                    COALESCE(
                                        GET_JSON_OBJECT(campaign_info, '$.funding_bucket'),
                                        'Unknown'
                                    ),
                                    partner
                                )
                            ),
                            'free_paid',
                            sg_udf :bi_to_json_string('Paid'),
                            'segmentation',
                            sg_udf :bi_to_json_string(segmentation),
                            'rt_bucket',
                            sg_udf :bi_to_json_string(
                                COALESCE(
                                    GET_JSON_OBJECT(campaign_info, '$.rt_bucket'),
                                    'Unknown'
                                )
                            ),
                            'campaign_type',
                            sg_udf :bi_to_json_string(
                                COALESCE(
                                    GET_JSON_OBJECT(campaign_info, '$.campaign_type'),
                                    'Unknown'
                                )
                            ),
                            'placement',
                            sg_udf :bi_to_json_string(placement)
                        )
                    )
                ) IN ('cps affiliate', 'cpi affiliate')
        ) AS t1
        INNER JOIN (
            SELECT ds,
                utdid,
                MIN(
                    TO_CHAR(TO_DATE(visit_time, 'yyyymmddhhmiss'), 'hh')
                ) AS hh
            FROM lazada_cdm.dwd_lzd_mkt_app_traffic_pv_di
            WHERE 1 = 1 --
                -- AND     ds BETWEEN '${start_date}' AND '${end_date}' --<< Start
                AND ds >= TO_CHAR(
                    DATEADD(TO_DATE('${bizdate}', 'yyyymmdd'), -3, 'dd'),
                    'yyyymmdd'
                ) --<< Daily
                AND venture = 'VN'
                AND utdid IS NOT NULL
                AND is_active_utdid = 1
                AND TOLOWER(sg_udf :bi_get_aplus_appinfo(app_id, 'code')) IN ('lazada')
            GROUP BY ds,
                utdid
        ) AS t2 ON t1.ds = t2.ds
        AND t1.utdid = t2.utdid
    GROUP BY t1.ds,
        t2.hh,
        t1.member_id
),
t_a2c AS (
    SELECT ds,
        hh,
        member_id,
        SUM(quantity_lt_1d_p) AS a2c_lt_1d_p,
        COUNT(
            DISTINCT CASE
                WHEN buyer_id_lt_1d_p_dedup = 1 THEN buyer_id
                ELSE NULL
            END
        ) AS a2c_uv_cnt_lt_1d_p
    FROM (
            SELECT ds,
                TO_CHAR(add_date, 'hh') AS hh,
                member_id,
                buyer_id,
                ROW_NUMBER() OVER (
                    PARTITION BY ds,
                    member_id,
                    buyer_id
                    ORDER BY add_date ASC
                ) AS buyer_id_lt_1d_p_dedup,
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
                AND TOLOWER(
                    COALESCE(
                        lazada_cdm.mkt_get_updated_funding_bucket(
                            channel,
                            GET_JSON_OBJECT(campaign_info, '$.funding_bucket'),
                            partner
                        ),
                        'Unknown'
                    )
                ) IN ('lazada om')
                AND TOLOWER(
                    COALESCE(
                        lazada_cdm.mkt_get_updated_funding_type(
                            channel,
                            GET_JSON_OBJECT(campaign_info, '$.funding_bucket'),
                            GET_JSON_OBJECT(campaign_info, '$.funding_type'),
                            partner
                        ),
                        'Unknown'
                    )
                ) IN ('om', 'ams')
                AND TOLOWER(
                    lazada_cdm.mkt_get_sub_channel_from_json(
                        sg_udf :bi_put_json_values(
                            '{}',
                            'channel',
                            sg_udf :bi_to_json_string(channel),
                            'funding_bucket',
                            sg_udf :bi_to_json_string(
                                lazada_cdm.mkt_get_updated_funding_bucket(
                                    channel,
                                    COALESCE(
                                        GET_JSON_OBJECT(campaign_info, '$.funding_bucket'),
                                        'Unknown'
                                    ),
                                    partner
                                )
                            ),
                            'free_paid',
                            sg_udf :bi_to_json_string('Paid'),
                            'segmentation',
                            sg_udf :bi_to_json_string(segmentation),
                            'rt_bucket',
                            sg_udf :bi_to_json_string(
                                COALESCE(
                                    GET_JSON_OBJECT(campaign_info, '$.rt_bucket'),
                                    'Unknown'
                                )
                            ),
                            'campaign_type',
                            sg_udf :bi_to_json_string(
                                COALESCE(
                                    GET_JSON_OBJECT(campaign_info, '$.campaign_type'),
                                    'Unknown'
                                )
                            ),
                            'placement',
                            sg_udf :bi_to_json_string(placement)
                        )
                    )
                ) IN ('cps affiliate', 'cpi affiliate')
        )
    GROUP BY ds,
        hh,
        member_id
),
t_trn AS (
    SELECT ds,
        hh,
        member_id,
        COUNT(
            DISTINCT CASE
                WHEN buyer_id_lt_1d_p_dedup = 1 THEN buyer_id_lt_1d_p
                ELSE NULL
            END
        ) AS buyer_cnt_lt_1d_p,
        COUNT(
            DISTINCT CASE
                WHEN new_buyer_id_lt_1d_p_dedup = 1 THEN buyer_id_lt_1d_p
                ELSE NULL
            END
        ) AS new_buyer_cnt_lt_1d_p,
        COUNT(
            DISTINCT CASE
                WHEN reacquired_buyer_id_lt_1d_p_dedup = 1 THEN buyer_id_lt_1d_p
                ELSE NULL
            END
        ) AS reacquired_buyer_cnt_lt_1d_p,
        COUNT(DISTINCT order_id_lt_1d_p) AS order_cnt_lt_1d_p,
        SUM(gmv_vnd_lt_1d_p) AS gmv_vnd_lt_1d_p,
        SUM(gmv_usd_lt_1d_p) AS gmv_usd_lt_1d_p
    FROM (
            SELECT TO_CHAR(created_date, 'yyyymmdd') AS ds,
                TO_CHAR(created_date, 'hh') AS hh,
                member_id,
                buyer_id AS buyer_id_lt_1d_p,
                ROW_NUMBER() OVER (
                    PARTITION BY ds,
                    member_id,
                    buyer_id
                    ORDER BY created_date ASC
                ) AS buyer_id_lt_1d_p_dedup,
                CASE
                    WHEN is_first_order = 1 THEN ROW_NUMBER() OVER (
                        PARTITION BY ds,
                        member_id,
                        buyer_id,
                        is_first_order = 1
                        ORDER BY created_date ASC
                    )
                    ELSE NULL
                END AS new_buyer_id_lt_1d_p_dedup,
                CASE
                    WHEN is_reacquired_buyer = 1 THEN ROW_NUMBER() OVER (
                        PARTITION BY ds,
                        member_id,
                        buyer_id,
                        is_reacquired_buyer = 1
                        ORDER BY created_date ASC
                    )
                    ELSE NULL
                END AS reacquired_buyer_id_lt_1d_p_dedup,
                CONCAT(sales_order_id, sku_id) AS order_id_lt_1d_p,
                COALESCE(actual_gmv, 0) AS gmv_vnd_lt_1d_p,
                COALESCE(actual_gmv, 0) * COALESCE(exchange_rate, 0) AS gmv_usd_lt_1d_p
            FROM lazada_cdm.dws_lzd_mkt_trn_uam_di
            WHERE 1 = 1 --
                -- AND     ds >= TO_CHAR(DATEADD(TO_DATE('${start_date}','yyyymmdd'),-5,'dd'),'yyyymmdd') --<< Start
                AND ds >= TO_CHAR(
                    DATEADD(TO_DATE('${bizdate}', 'yyyymmdd'), -15, 'dd'),
                    'yyyymmdd'
                ) --<< Daily
                AND venture = 'VN' --
                -- AND     TO_CHAR(created_date,'yyyymmdd') BETWEEN '${start_date}' AND '${end_date}' --<< Start
                AND TO_CHAR(created_date, 'yyyymmdd') >= TO_CHAR(
                    DATEADD(TO_DATE('${bizdate}', 'yyyymmdd'), -3, 'dd'),
                    'yyyymmdd'
                ) --<< Daily
                AND attr_model IN ('lt_1d_p')
                AND TOLOWER(
                    COALESCE(
                        lazada_cdm.mkt_get_updated_funding_bucket(
                            channel,
                            GET_JSON_OBJECT(campaign_info, '$.funding_bucket'),
                            partner
                        ),
                        'Unknown'
                    )
                ) IN ('lazada om')
                AND TOLOWER(
                    COALESCE(
                        lazada_cdm.mkt_get_updated_funding_type(
                            channel,
                            GET_JSON_OBJECT(campaign_info, '$.funding_bucket'),
                            GET_JSON_OBJECT(campaign_info, '$.funding_type'),
                            partner
                        ),
                        'Unknown'
                    )
                ) IN ('om', 'ams')
                AND TOLOWER(
                    lazada_cdm.mkt_get_sub_channel_from_json(
                        sg_udf :bi_put_json_values(
                            '{}',
                            'channel',
                            sg_udf :bi_to_json_string(channel),
                            'funding_bucket',
                            sg_udf :bi_to_json_string(
                                lazada_cdm.mkt_get_updated_funding_bucket(
                                    channel,
                                    COALESCE(
                                        GET_JSON_OBJECT(campaign_info, '$.funding_bucket'),
                                        'Unknown'
                                    ),
                                    partner
                                )
                            ),
                            'free_paid',
                            sg_udf :bi_to_json_string('Paid'),
                            'segmentation',
                            sg_udf :bi_to_json_string(segmentation),
                            'rt_bucket',
                            sg_udf :bi_to_json_string(
                                COALESCE(
                                    GET_JSON_OBJECT(campaign_info, '$.rt_bucket'),
                                    'Unknown'
                                )
                            ),
                            'campaign_type',
                            sg_udf :bi_to_json_string(
                                COALESCE(
                                    GET_JSON_OBJECT(campaign_info, '$.campaign_type'),
                                    'Unknown'
                                )
                            ),
                            'placement',
                            sg_udf :bi_to_json_string(placement)
                        )
                    )
                ) IN ('cps affiliate', 'cpi affiliate')
        )
    GROUP BY ds,
        hh,
        member_id
),
t_trf_app_ads AS (
    SELECT ds,
        hh,
        member_id,
        SUM(SUM(app_visit_cnt_pc_session)) OVER (
            PARTITION BY ds,
            member_id
            ORDER BY hh ASC
        ) AS app_visit_cnt_pc_session,
        SUM(SUM(app_visit_cnt_lt_1d)) OVER (
            PARTITION BY ds,
            member_id
            ORDER BY hh ASC
        ) AS app_visit_cnt_lt_1d,
        SUM(SUM(app_visit_cnt_ft_1d_np)) OVER (
            PARTITION BY ds,
            member_id
            ORDER BY hh ASC
        ) AS app_visit_cnt_ft_1d_np,
        SUM(SUM(app_dau_cnt_ft_1d_np)) OVER (
            PARTITION BY ds,
            member_id
            ORDER BY hh ASC
        ) AS app_dau_cnt_ft_1d_np,
        SUM(SUM(app_pdp_pv_ft_1d_np)) OVER (
            PARTITION BY ds,
            member_id
            ORDER BY hh ASC
        ) AS app_pdp_pv_ft_1d_np,
        SUM(SUM(app_pdp_uv_cnt_ft_1d_np)) OVER (
            PARTITION BY ds,
            member_id
            ORDER BY hh ASC
        ) AS app_pdp_uv_cnt_ft_1d_np
    FROM (
            SELECT ds,
                hh,
                member_id,
                app_visit_cnt_pc_session,
                app_visit_cnt_lt_1d,
                app_visit_cnt_ft_1d_np,
                app_dau_cnt_ft_1d_np,
                app_pdp_pv_ft_1d_np,
                app_pdp_uv_cnt_ft_1d_np
            FROM t_trf_app
            UNION ALL
            SELECT t1.ds AS ds,
                t2.hh AS hh,
                t1.member_id AS member_id,
                0 AS app_visit_cnt_pc_session,
                0 AS app_visit_cnt_lt_1d,
                0 AS app_visit_cnt_ft_1d_np,
                0 AS app_dau_cnt_ft_1d_np,
                0 AS app_pdp_pv_ft_1d_np,
                0 AS app_pdp_uv_cnt_ft_1d_np
            FROM (
                    SELECT ds,
                        member_id,
                        1 AS map_key
                    FROM t_trf_app
                ) AS t1
                LEFT JOIN (
                    SELECT hh,
                        1 AS map_key
                    FROM lazada_analyst_dev.loutruong_dim_hh
                ) AS t2 ON t1.map_key = t2.map_key LEFT ANTI
                JOIN (
                    SELECT ds,
                        hh,
                        member_id
                    FROM t_trf_app
                ) AS t3 ON t1.ds = t3.ds
                AND t2.hh = t3.hh
                AND t1.member_id = t3.member_id
        )
    GROUP BY ds,
        hh,
        member_id
),
t_a2c_ads AS (
    SELECT ds,
        hh,
        member_id,
        SUM(SUM(a2c_lt_1d_p)) OVER (
            PARTITION BY ds,
            member_id
            ORDER BY hh ASC
        ) AS a2c_lt_1d_p,
        SUM(SUM(a2c_uv_cnt_lt_1d_p)) OVER (
            PARTITION BY ds,
            member_id
            ORDER BY hh ASC
        ) AS a2c_uv_cnt_lt_1d_p
    FROM (
            SELECT ds,
                hh,
                member_id,
                a2c_lt_1d_p,
                a2c_uv_cnt_lt_1d_p
            FROM t_a2c
            UNION ALL
            SELECT t1.ds AS ds,
                t2.hh AS hh,
                t1.member_id AS member_id,
                0 AS a2c_lt_1d_p,
                0 AS a2c_uv_cnt_lt_1d_p
            FROM (
                    SELECT ds,
                        member_id,
                        1 AS map_key
                    FROM t_a2c
                ) AS t1
                LEFT JOIN (
                    SELECT hh,
                        1 AS map_key
                    FROM lazada_analyst_dev.loutruong_dim_hh
                ) AS t2 ON t1.map_key = t2.map_key LEFT ANTI
                JOIN (
                    SELECT ds,
                        hh,
                        member_id
                    FROM t_a2c
                ) AS t3 ON t1.ds = t3.ds
                AND t2.hh = t3.hh
                AND t1.member_id = t3.member_id
        )
    GROUP BY ds,
        hh,
        member_id
),
t_trn_ads AS (
    SELECT ds,
        hh,
        member_id,
        SUM(SUM(buyer_cnt_lt_1d_p)) OVER (
            PARTITION BY ds,
            member_id
            ORDER BY hh ASC
        ) AS buyer_cnt_lt_1d_p,
        SUM(SUM(new_buyer_cnt_lt_1d_p)) OVER (
            PARTITION BY ds,
            member_id
            ORDER BY hh ASC
        ) AS new_buyer_cnt_lt_1d_p,
        SUM(SUM(reacquired_buyer_cnt_lt_1d_p)) OVER (
            PARTITION BY ds,
            member_id
            ORDER BY hh ASC
        ) AS reacquired_buyer_cnt_lt_1d_p,
        SUM(SUM(order_cnt_lt_1d_p)) OVER (
            PARTITION BY ds,
            member_id
            ORDER BY hh ASC
        ) AS order_cnt_lt_1d_p,
        SUM(SUM(gmv_vnd_lt_1d_p)) OVER (
            PARTITION BY ds,
            member_id
            ORDER BY hh ASC
        ) AS gmv_vnd_lt_1d_p,
        SUM(SUM(gmv_usd_lt_1d_p)) OVER (
            PARTITION BY ds,
            member_id
            ORDER BY hh ASC
        ) AS gmv_usd_lt_1d_p
    FROM (
            SELECT ds,
                hh,
                member_id,
                buyer_cnt_lt_1d_p,
                new_buyer_cnt_lt_1d_p,
                reacquired_buyer_cnt_lt_1d_p,
                order_cnt_lt_1d_p,
                gmv_vnd_lt_1d_p,
                gmv_usd_lt_1d_p
            FROM t_trn
            UNION ALL
            SELECT t1.ds AS ds,
                t2.hh AS hh,
                t1.member_id AS member_id,
                0 AS buyer_cnt_lt_1d_p,
                0 AS new_buyer_cnt_lt_1d_p,
                0 AS reacquired_buyer_cnt_lt_1d_p,
                0 AS order_cnt_lt_1d_p,
                0 AS gmv_vnd_lt_1d_p,
                0 AS gmv_usd_lt_1d_p
            FROM (
                    SELECT ds,
                        member_id,
                        1 AS map_key
                    FROM t_trn
                ) AS t1
                LEFT JOIN (
                    SELECT hh,
                        1 AS map_key
                    FROM lazada_analyst_dev.loutruong_dim_hh
                ) AS t2 ON t1.map_key = t2.map_key LEFT ANTI
                JOIN (
                    SELECT ds,
                        hh,
                        member_id
                    FROM t_trn
                ) AS t3 ON t1.ds = t3.ds
                AND t2.hh = t3.hh
                AND t1.member_id = t3.member_id
        )
    GROUP BY ds,
        hh,
        member_id
),
t_agg AS (
    SELECT ds,
        hh,
        member_id,
        CASE
            WHEN engaged_member_id_dedup_pc_session = 1 THEN member_id
            ELSE NULL
        END AS engaged_member_id_pc_session,
        CASE
            WHEN engaged_member_id_dedup_lt_1d = 1 THEN member_id
            ELSE NULL
        END AS engaged_member_id_lt_1d,
        CASE
            WHEN engaged_member_id_dedup_ft_1d_np = 1 THEN member_id
            ELSE NULL
        END AS engaged_member_id_ft_1d_np,
        CASE
            WHEN active_member_id_dedup_lt_1d_p = 1 THEN member_id
            ELSE NULL
        END AS active_member_id_lt_1d_p,
        app_visit_cnt_pc_session,
        app_visit_cnt_lt_1d,
        app_visit_cnt_ft_1d_np,
        app_dau_cnt_ft_1d_np,
        app_pdp_pv_ft_1d_np,
        app_pdp_uv_cnt_ft_1d_np,
        a2c_lt_1d_p,
        a2c_uv_cnt_lt_1d_p,
        buyer_cnt_lt_1d_p,
        new_buyer_cnt_lt_1d_p,
        reacquired_buyer_cnt_lt_1d_p,
        order_cnt_lt_1d_p,
        gmv_vnd_lt_1d_p,
        gmv_usd_lt_1d_p
    FROM (
            SELECT ds,
                hh,
                member_id,
                CASE
                    WHEN app_visit_cnt_pc_session > 0 THEN ROW_NUMBER() OVER (
                        PARTITION BY ds,
                        member_id,
                        app_visit_cnt_pc_session > 0
                        ORDER BY hh ASC
                    )
                    ELSE NULL
                END AS engaged_member_id_dedup_pc_session,
                CASE
                    WHEN app_visit_cnt_lt_1d > 0 THEN ROW_NUMBER() OVER (
                        PARTITION BY ds,
                        member_id,
                        app_visit_cnt_lt_1d > 0
                        ORDER BY hh ASC
                    )
                    ELSE NULL
                END AS engaged_member_id_dedup_lt_1d,
                CASE
                    WHEN app_visit_cnt_ft_1d_np > 0 THEN ROW_NUMBER() OVER (
                        PARTITION BY ds,
                        member_id,
                        app_visit_cnt_ft_1d_np > 0
                        ORDER BY hh ASC
                    )
                    ELSE NULL
                END AS engaged_member_id_dedup_ft_1d_np,
                CASE
                    WHEN order_cnt_lt_1d_p > 0 THEN ROW_NUMBER() OVER (
                        PARTITION BY ds,
                        member_id,
                        order_cnt_lt_1d_p > 0
                        ORDER BY hh ASC
                    )
                    ELSE NULL
                END AS active_member_id_dedup_lt_1d_p,
                app_visit_cnt_pc_session,
                app_visit_cnt_lt_1d,
                app_visit_cnt_ft_1d_np,
                app_dau_cnt_ft_1d_np,
                app_pdp_pv_ft_1d_np,
                app_pdp_uv_cnt_ft_1d_np,
                a2c_lt_1d_p,
                a2c_uv_cnt_lt_1d_p,
                buyer_cnt_lt_1d_p,
                new_buyer_cnt_lt_1d_p,
                reacquired_buyer_cnt_lt_1d_p,
                order_cnt_lt_1d_p,
                gmv_vnd_lt_1d_p,
                gmv_usd_lt_1d_p
            FROM (
                    SELECT ds,
                        hh,
                        member_id,
                        SUM(app_visit_cnt_pc_session) AS app_visit_cnt_pc_session,
                        SUM(app_visit_cnt_lt_1d) AS app_visit_cnt_lt_1d,
                        SUM(app_visit_cnt_ft_1d_np) AS app_visit_cnt_ft_1d_np,
                        SUM(app_dau_cnt_ft_1d_np) AS app_dau_cnt_ft_1d_np,
                        SUM(app_pdp_pv_ft_1d_np) AS app_pdp_pv_ft_1d_np,
                        SUM(app_pdp_uv_cnt_ft_1d_np) AS app_pdp_uv_cnt_ft_1d_np,
                        SUM(a2c_lt_1d_p) AS a2c_lt_1d_p,
                        SUM(a2c_uv_cnt_lt_1d_p) AS a2c_uv_cnt_lt_1d_p,
                        SUM(buyer_cnt_lt_1d_p) AS buyer_cnt_lt_1d_p,
                        SUM(new_buyer_cnt_lt_1d_p) AS new_buyer_cnt_lt_1d_p,
                        SUM(reacquired_buyer_cnt_lt_1d_p) AS reacquired_buyer_cnt_lt_1d_p,
                        SUM(order_cnt_lt_1d_p) AS order_cnt_lt_1d_p,
                        SUM(gmv_vnd_lt_1d_p) AS gmv_vnd_lt_1d_p,
                        SUM(gmv_usd_lt_1d_p) AS gmv_usd_lt_1d_p
                    FROM (
                            SELECT ds,
                                hh,
                                member_id,
                                app_visit_cnt_pc_session,
                                app_visit_cnt_lt_1d,
                                app_visit_cnt_ft_1d_np,
                                app_dau_cnt_ft_1d_np,
                                app_pdp_pv_ft_1d_np,
                                app_pdp_uv_cnt_ft_1d_np,
                                0 AS a2c_lt_1d_p,
                                0 AS a2c_uv_cnt_lt_1d_p,
                                0 AS buyer_cnt_lt_1d_p,
                                0 AS new_buyer_cnt_lt_1d_p,
                                0 AS reacquired_buyer_cnt_lt_1d_p,
                                0 AS order_cnt_lt_1d_p,
                                0 AS gmv_vnd_lt_1d_p,
                                0 AS gmv_usd_lt_1d_p
                            FROM t_trf_app_ads
                            UNION ALL
                            SELECT ds,
                                hh,
                                member_id,
                                0 AS app_visit_cnt_pc_session,
                                0 AS app_visit_cnt_lt_1d,
                                0 AS app_visit_cnt_ft_1d_np,
                                0 AS app_dau_cnt_ft_1d_np,
                                0 AS app_pdp_pv_ft_1d_np,
                                0 AS app_pdp_uv_cnt_ft_1d_np,
                                a2c_lt_1d_p,
                                a2c_uv_cnt_lt_1d_p,
                                0 AS buyer_cnt_lt_1d_p,
                                0 AS new_buyer_cnt_lt_1d_p,
                                0 AS reacquired_buyer_cnt_lt_1d_p,
                                0 AS order_cnt_lt_1d_p,
                                0 AS gmv_vnd_lt_1d_p,
                                0 AS gmv_usd_lt_1d_p
                            FROM t_a2c_ads
                            UNION ALL
                            SELECT ds,
                                hh,
                                member_id,
                                0 AS app_visit_cnt_pc_session,
                                0 AS app_visit_cnt_lt_1d,
                                0 AS app_visit_cnt_ft_1d_np,
                                0 AS app_dau_cnt_ft_1d_np,
                                0 AS app_pdp_pv_ft_1d_np,
                                0 AS app_pdp_uv_cnt_ft_1d_np,
                                0 AS a2c_lt_1d_p,
                                0 AS a2c_uv_cnt_lt_1d_p,
                                buyer_cnt_lt_1d_p,
                                new_buyer_cnt_lt_1d_p,
                                reacquired_buyer_cnt_lt_1d_p,
                                order_cnt_lt_1d_p,
                                gmv_vnd_lt_1d_p,
                                gmv_usd_lt_1d_p
                            FROM t_trn_ads
                        )
                    GROUP BY ds,
                        hh,
                        member_id
                )
        )
) --
INSERT OVERWRITE TABLE lazada_analyst_dev.tmp_loutruong_aff_perf_hh_1 PARTITION (ds, hh)
SELECT member_id,
    engaged_member_id_pc_session,
    engaged_member_id_lt_1d,
    engaged_member_id_ft_1d_np,
    active_member_id_lt_1d_p,
    app_visit_cnt_pc_session,
    app_visit_cnt_lt_1d,
    app_visit_cnt_ft_1d_np,
    app_dau_cnt_ft_1d_np,
    app_pdp_pv_ft_1d_np,
    app_pdp_uv_cnt_ft_1d_np,
    a2c_lt_1d_p,
    a2c_uv_cnt_lt_1d_p,
    buyer_cnt_lt_1d_p,
    new_buyer_cnt_lt_1d_p,
    reacquired_buyer_cnt_lt_1d_p,
    order_cnt_lt_1d_p,
    gmv_vnd_lt_1d_p,
    gmv_usd_lt_1d_p,
    ds,
    hh
FROM t_agg
ORDER BY ds ASC,
    hh ASC,
    member_id ASC;
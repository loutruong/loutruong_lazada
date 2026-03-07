-- MaxCompute SQL 
-- ********************************************************************--
-- author:Truong, Van Thanh
-- create time:2024-01-05 12:00:01
-- ********************************************************************--
--@@ Input = lazada_cdm.dwd_lzd_clickserver_log_di
--@@ Input = lazada_cdm.dws_lzd_mkt_app_install_di
--@@ Input = lazada_cdm.dws_lzd_mkt_app_reinstall_di
--@@ Input = lazada_cdm.dwd_lzd_mkt_uam_all_reg_usr_di
--@@ Input = lazada_ads.ads_lzd_marketing_cps_conversion_report_mi
--@@ Input = lazada_cdm.dws_lzd_mkt_app_ad_mp_di
--@@ Input = lazada_cdm.dwd_lzd_mkt_a2c_uam_core_di
--@@ Input = lazada_cdm.dws_lzd_mkt_trn_uam_di
--@@ Input = lazada_cdm.dim_lzd_exchange_rate
--@@ Input Exclude = t_click_app_ads
--@@ Input Exclude = t_install_reinstall_app_ads
--@@ Input Exclude = t_reg_ads
--@@ Input Exclude = t_payout
--@@ Input Exclude = t_payout_ads
--@@ Input Exclude = t_trf_app_ads
--@@ Input Exclude = t_a2c_ads
--@@ Input Exclude = t_trn_ads
--@@ Output = lazada_analyst_dev.loutruong_aff_perf_plm_ops_di
----------------------------------------------------------------------------------------------------------------------------
-- Performance base table
----------------------------------------------------------------------------------------------------------------------------
-- DROP TABLE IF EXISTS lazada_analyst_dev.loutruong_aff_perf_plm_ops_di
-- ;
-- CREATE TABLE IF NOT EXISTS lazada_analyst_dev.loutruong_aff_perf_plm_ops_di
-- (
--     member_id                     STRING
--     ,app_click_cnt                DOUBLE COMMENT 'click sever logic'
--     ,app_install_cnt_lt_1d        DOUBLE COMMENT 'attr_model = lt_1d'
--     ,app_reinstall_cnt_lt_1d      DOUBLE COMMENT 'attr_model = lt_1d'
--     ,register_cnt_ft_1d_np        DOUBLE COMMENT 'attr_model = ft_1d_np'
--     ,total_payout_vnd             DOUBLE COMMENT 'attr_model = ltpdp_settlement_voucher_p & status <> rejected, returned'
--     ,platform_payout_vnd          DOUBLE COMMENT 'attr_model = ltpdp_settlement_voucher_p & status <> rejected, returned'
--     ,brand_payout_vnd             DOUBLE COMMENT 'attr_model = ltpdp_settlement_voucher_p & status <> rejected, returned'
--     ,total_payout_usd             DOUBLE COMMENT 'attr_model = ltpdp_settlement_voucher_p & status <> rejected, returned'
--     ,platform_payout_usd          DOUBLE COMMENT 'attr_model = ltpdp_settlement_voucher_p & status <> rejected, returned'
--     ,brand_payout_usd             DOUBLE COMMENT 'attr_model = ltpdp_settlement_voucher_p & status <> rejected, returned'
--     ,app_visit_cnt_pc_session     DOUBLE COMMENT 'attr_model = pc_session / any_touch & use to filter daily engaged partner'
--     ,app_visit_cnt_ft_1d_np       DOUBLE COMMENT 'attr_model = ft_1d_np'
--     ,app_dau_cnt_ft_1d_np         DOUBLE COMMENT 'attr_model = ft_1d_np'
--     ,app_pdp_pv_ft_1d_np          DOUBLE COMMENT 'attr_model = ft_1d_np'
--     ,app_pdp_uv_cnt_ft_1d_np      DOUBLE COMMENT 'attr_model = ft_1d_np'
--     ,a2c_cnt_lt_1d_p              DOUBLE COMMENT 'attr_model = lt_1d_p'
--     ,a2c_uv_cnt_lt_1d_p           DOUBLE COMMENT 'attr_model = lt_1d_p'
--     ,buyer_cnt_lt_1d_p            DOUBLE COMMENT 'attr_model = lt_1d_p'
--     ,existing_buyer_cnt_lt_1d_p   DOUBLE COMMENT 'attr_model = lt_1d_p'
--     ,new_buyer_cnt_lt_1d_p        DOUBLE COMMENT 'attr_model = lt_1d_p'
--     ,reacquired_buyer_cnt_lt_1d_p DOUBLE COMMENT 'attr_model = lt_1d_p'
--     ,order_cnt_lt_1d_p            DOUBLE COMMENT 'attr_model = lt_1d_p & use to filter daily active partner'
--     ,sa_order_cnt_lt_1d_p         DOUBLE COMMENT 'attr_model = lt_1d_p x ltpdp_settlement_voucher_p'
--     ,non_sa_order_cnt_lt_1d_p     DOUBLE COMMENT 'attr_model = lt_1d_p x ltpdp_settlement_voucher_p'
--     ,gmv_vnd_lt_1d_p              DOUBLE COMMENT 'attr_model = lt_1d_p'
--     ,sa_gmv_vnd_lt_1d_p           DOUBLE COMMENT 'attr_model = lt_1d_p x ltpdp_settlement_voucher_p'
--     ,non_sa_gmv_vnd_lt_1d_p       DOUBLE COMMENT 'attr_model = lt_1d_p x ltpdp_settlement_voucher_p'
--     ,gmv_usd_lt_1d_p              DOUBLE COMMENT 'attr_model = lt_1d_p'
--     ,sa_gmv_usd_lt_1d_p           DOUBLE COMMENT 'attr_model = lt_1d_p x ltpdp_settlement_voucher_p'
--     ,non_sa_gmv_usd_lt_1d_p       DOUBLE COMMENT 'attr_model = lt_1d_p x ltpdp_settlement_voucher_p'
-- )
-- COMMENT 'Table store all data of VN Affiliate with multiple attr_model logic'
-- PARTITIONED BY 
-- (
--     ds                            STRING
-- )
-- LIFECYCLE 3600
-- ;
WITH t_click_app_ads AS (
    SELECT ds,
        member_id,
        COUNT(*) AS app_click_cnt
    FROM (
            SELECT ds,
                member_id,
                clickid,
                MAX(is_app) AS is_app
            FROM (
                    SELECT ds,
                        COALESCE(SPLIT(pid, '_') [1], '') AS member_id,
                        clickid,
                        IF(LENGTH(utdid) > 0, 1, 0) AS is_app
                    FROM lazada_cdm.dwd_lzd_clickserver_log_di
                    WHERE 1 = 1 --
                        -- AND      ds BETWEEN '${start_date}' AND '${end_date}' --<< Start
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
                clickid
        )
    GROUP BY ds,
        member_id
),
t_install_reinstall_app_ads AS (
    SELECT ds,
        member_id,
        COUNT(
            DISTINCT CASE
                WHEN is_new_device = 1 THEN utdid
                ELSE NULL
            END
        ) AS app_install_cnt_lt_1d,
        COUNT(
            DISTINCT CASE
                WHEN is_new_device = 0 THEN utdid
                ELSE NULL
            END
        ) AS app_reinstall_cnt_lt_1d
    FROM (
            SELECT ds,
                funding_bucket,
                funding_type,
                channel,
                COALESCE(SPLIT(pid, '_') [1], '') AS member_id,
                is_new_device,
                utdid
            FROM lazada_cdm.dws_lzd_mkt_app_install_di
            WHERE 1 = 1 --
                -- AND     ds BETWEEN '${start_date}' AND '${end_date}' --<< Start
                AND ds >= TO_CHAR(
                    DATEADD(TO_DATE('${bizdate}', 'yyyymmdd'), -3, 'dd'),
                    'yyyymmdd'
                ) --<< Daily
                AND venture = 'VN'
                AND TOLOWER(
                    COALESCE(
                        lazada_cdm.mkt_get_updated_funding_bucket(channel, funding_bucket, partner),
                        'Unknown'
                    )
                ) IN ('lazada om')
                AND TOLOWER(
                    COALESCE(
                        lazada_cdm.mkt_get_updated_funding_type(channel, funding_bucket, funding_type, partner),
                        'Unknown'
                    )
                ) IN ('om', 'ams')
                AND TOLOWER(
                    lazada_cdm.mkt_get_sub_channel_from_json(
                        sg_udf :bi_put_json_values(
                            '{}',
                            'channel',
                            sg_udf :bi_to_json_string(COALESCE(channel, 'Direct')),
                            'funding_bucket',
                            sg_udf :bi_to_json_string(
                                lazada_cdm.mkt_get_updated_funding_bucket(
                                    channel,
                                    COALESCE(funding_bucket, 'Unknown'),
                                    partner
                                )
                            ),
                            'free_paid',
                            sg_udf :bi_to_json_string(COALESCE(is_paid, 'Free')),
                            'segmentation',
                            sg_udf :bi_to_json_string(segmentation),
                            'rt_bucket',
                            sg_udf :bi_to_json_string(COALESCE(rt_bucket, 'Unknown')),
                            'campaign_type',
                            sg_udf :bi_to_json_string(COALESCE(campaign_type, 'Unknown')),
                            'placement',
                            sg_udf :bi_to_json_string(COALESCE(placement, 'Unkown'))
                        )
                    )
                ) IN ('cps affiliate')
            UNION ALL
            SELECT ds,
                funding_bucket,
                funding_type,
                channel,
                COALESCE(SPLIT(pid, '_') [1], '') AS member_id,
                is_new_device,
                utdid
            FROM lazada_cdm.dws_lzd_mkt_app_reinstall_di
            WHERE 1 = 1 --
                -- AND     ds BETWEEN '${start_date}' AND '${end_date}' --<< Start
                AND ds >= TO_CHAR(
                    DATEADD(TO_DATE('${bizdate}', 'yyyymmdd'), -3, 'dd'),
                    'yyyymmdd'
                ) --<< Daily
                AND venture = 'VN'
                AND TOLOWER(
                    COALESCE(
                        lazada_cdm.mkt_get_updated_funding_bucket(channel, funding_bucket, partner),
                        'Unknown'
                    )
                ) IN ('lazada om')
                AND TOLOWER(
                    COALESCE(
                        lazada_cdm.mkt_get_updated_funding_type(channel, funding_bucket, funding_type, partner),
                        'Unknown'
                    )
                ) IN ('om', 'ams')
                AND TOLOWER(
                    lazada_cdm.mkt_get_sub_channel_from_json(
                        sg_udf :bi_put_json_values(
                            '{}',
                            'channel',
                            sg_udf :bi_to_json_string(COALESCE(channel, 'Direct')),
                            'funding_bucket',
                            sg_udf :bi_to_json_string(
                                lazada_cdm.mkt_get_updated_funding_bucket(
                                    channel,
                                    COALESCE(funding_bucket, 'Unknown'),
                                    partner
                                )
                            ),
                            'free_paid',
                            sg_udf :bi_to_json_string(COALESCE(is_paid, 'Free')),
                            'segmentation',
                            sg_udf :bi_to_json_string(COALESCE(segmentation, 'Unkown')),
                            'rt_bucket',
                            sg_udf :bi_to_json_string(COALESCE(rt_bucket, 'Unknown')),
                            'campaign_type',
                            sg_udf :bi_to_json_string(COALESCE(campaign_type, 'Unknown')),
                            'placement',
                            sg_udf :bi_to_json_string(COALESCE(placement, 'Unknown'))
                        )
                    )
                ) IN ('cps affiliate')
        )
    GROUP BY ds,
        member_id
),
t_reg_ads AS (
    SELECT ds,
        COALESCE(SPLIT(pid, '_') [1], '') AS member_id,
        COUNT(DISTINCT user_id) AS register_cnt_ft_1d_np
    FROM lazada_cdm.dwd_lzd_mkt_uam_all_reg_usr_di
    WHERE 1 = 1 --
        -- AND     ds BETWEEN '${start_date}' AND '${end_date}' --<< Start
        AND ds >= TO_CHAR(
            DATEADD(TO_DATE('${bizdate}', 'yyyymmdd'), -3, 'dd'),
            'yyyymmdd'
        ) --<< Daily
        AND venture = 'VN'
        AND attr_model IN ('ft_1d_np')
        AND TOLOWER(
            COALESCE(
                lazada_cdm.mkt_get_updated_funding_bucket(channel, funding_bucket, partner),
                'Unknown'
            )
        ) IN ('lazada om')
        AND TOLOWER(
            lazada_cdm.mkt_get_updated_funding_type(
                channel,
                GET_JSON_OBJECT(campaign_info, '$.funding_bucket'),
                GET_JSON_OBJECT(campaign_info, '$.funding_type'),
                partner
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
                    sg_udf :bi_to_json_string(is_paid),
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
        ) IN ('cps affiliate')
    GROUP BY ds,
        COALESCE(SPLIT(pid, '_') [1], '')
),
t_payout AS (
    SELECT t1.ds AS ds,
        t1.member_id AS member_id,
        t1.sub_order_id AS sales_order_item_id,
        t1.total_payout_vnd AS total_payout_vnd,
        t1.platform_payout_vnd AS platform_payout_vnd,
        t1.brand_payout_vnd AS brand_payout_vnd,
        t1.total_payout_vnd * t2.to_usd AS total_payout_usd,
        t1.platform_payout_vnd * t2.to_usd AS platform_payout_usd,
        t1.brand_payout_vnd * t2.to_usd AS brand_payout_usd
    FROM (
            SELECT TO_CHAR(
                    TO_DATE(fulfilled_time, 'yyyy-mm-ddTHH:mi'),
                    'yyyymmdd'
                ) AS ds,
                member_id,
                sub_order_id,
                COALESCE(est_payout, 0) AS total_payout_vnd,
                COALESCE(platform_commission_fee, 0) AS platform_payout_vnd,
                COALESCE(brand_commission_fee, 0) AS brand_payout_vnd
            FROM lazada_ads.ads_lzd_marketing_cps_conversion_report_mi
            WHERE 1 = 1 --
                -- AND     mm >= TO_CHAR(DATEADD(TO_DATE('${start_date}','yyyymmdd'),-1,'mm'),'yyyymm') --<< Start
                AND mm >= TO_CHAR(
                    DATEADD(TO_DATE('${bizdate}', 'yyyymmdd'), -1, 'mm'),
                    'yyyymm'
                ) --<< Daily
                AND venture = 'VN' --
                -- AND     TO_CHAR(TO_DATE(fulfilled_time,'yyyy-mm-ddTHH:mi'),'yyyymmdd') BETWEEN '${start_date}' AND '${end_date}' --<< Start
                AND TO_CHAR(
                    TO_DATE(fulfilled_time, 'yyyy-mm-ddTHH:mi'),
                    'yyyymmdd'
                ) >= TO_CHAR(
                    DATEADD(TO_DATE('${bizdate}', 'yyyymmdd'), -3, 'dd'),
                    'yyyymmdd'
                ) --<< Daily
                AND TOLOWER(status) NOT IN ('rejected', 'returned')
                AND source IS NOT NULL
        ) AS t1
        LEFT JOIN (
            SELECT ds,
                to_usd
            FROM lazada_cdm.dim_lzd_exchange_rate
            WHERE 1 = 1 --
                -- AND     ds BETWEEN '${start_date}' AND '${end_date}' --<< Start
                AND ds >= TO_CHAR(
                    DATEADD(TO_DATE('${bizdate}', 'yyyymmdd'), -3, 'dd'),
                    'yyyymmdd'
                ) --<< Daily
                AND venture = 'VN'
            GROUP BY ds,
                to_usd
        ) AS t2 ON t1.ds = t2.ds
),
t_payout_ads AS (
    SELECT ds,
        member_id,
        SUM(total_payout_vnd) AS total_payout_vnd,
        SUM(platform_payout_vnd) AS platform_payout_vnd,
        SUM(brand_payout_vnd) AS brand_payout_vnd,
        SUM(total_payout_usd) AS total_payout_usd,
        SUM(platform_payout_usd) AS platform_payout_usd,
        SUM(brand_payout_usd) AS brand_payout_usd
    FROM t_payout
    GROUP BY ds,
        member_id
),
t_trf_app_ads AS (
    SELECT ds,
        member_id,
        COUNT(
            DISTINCT CASE
                WHEN TOLOWER(attr_model) IN ('pc_session')
                AND is_active_utdid = 1 THEN session_id
                ELSE NULL
            END
        ) AS app_visit_cnt_pc_session,
        COUNT(
            DISTINCT CASE
                WHEN TOLOWER(attr_model) IN ('ft_1d_np')
                AND is_active_utdid = 1 THEN session_id
                ELSE NULL
            END
        ) AS app_visit_cnt_ft_1d_np,
        COUNT(
            DISTINCT CASE
                WHEN TOLOWER(attr_model) IN ('ft_1d_np')
                AND is_active_utdid = 1 THEN utdid
                ELSE NULL
            END
        ) AS app_dau_cnt_ft_1d_np,
        SUM(
            CASE
                WHEN attr_model IN ('ft_1d_np')
                AND url_type IN ('ipv') THEN NVL(pv_count, 0)
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
        AND attr_model IN ('ft_1d_np', 'pc_session')
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
        ) IN ('cps affiliate')
    GROUP BY ds,
        member_id
),
t_a2c_ads AS (
    SELECT ds,
        member_id,
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
        ) IN ('cps affiliate')
    GROUP BY ds,
        member_id
),
t_trn_ads AS (
    SELECT t1.ds AS ds,
        t1.member_id AS member_id,
        COUNT(DISTINCT t1.buyer_id) AS buyer_cnt_lt_1d_p,
        COUNT(DISTINCT t1.new_buyer_id) AS new_buyer_cnt_lt_1d_p,
        COUNT(DISTINCT t1.reacquired_buyer_id) AS reacquired_buyer_cnt_lt_1d_p,
        COUNT(DISTINCT t1.order_id) AS order_cnt_lt_1d_p,
        COUNT(
            DISTINCT CASE
                WHEN COALESCE(t2.brand_payout_vnd, 0) > 0 THEN t1.order_id
                ELSE NULL
            END
        ) AS sa_order_cnt_lt_1d_p,
        COUNT(
            DISTINCT CASE
                WHEN COALESCE(t2.brand_payout_vnd, 0) = 0 THEN t1.order_id
                ELSE NULL
            END
        ) AS non_sa_order_cnt_lt_1d_p,
        SUM(COALESCE(t1.gmv_vnd, 0)) AS gmv_vnd_lt_1d_p,
        SUM(
            CASE
                WHEN COALESCE(t2.brand_payout_vnd, 0) > 0 THEN COALESCE(t1.gmv_vnd, 0)
                ELSE 0
            END
        ) AS sa_gmv_vnd_lt_1d_p,
        SUM(
            CASE
                WHEN COALESCE(t2.brand_payout_vnd, 0) = 0 THEN COALESCE(t1.gmv_vnd, 0)
                ELSE 0
            END
        ) AS non_sa_gmv_vnd_lt_1d_p,
        SUM(COALESCE(t1.gmv_usd, 0)) AS gmv_usd_lt_1d_p,
        SUM(
            CASE
                WHEN COALESCE(t2.brand_payout_vnd, 0) > 0 THEN COALESCE(t1.gmv_usd, 0)
                ELSE 0
            END
        ) AS sa_gmv_usd_lt_1d_p,
        SUM(
            CASE
                WHEN COALESCE(t2.brand_payout_vnd, 0) = 0 THEN COALESCE(t1.gmv_usd, 0)
                ELSE 0
            END
        ) AS non_sa_gmv_usd_lt_1d_p
    FROM (
            SELECT TO_CHAR(fulfillment_create_date, 'yyyymmdd') AS ds,
                attr_model,
                member_id,
                sales_order_item_id,
                buyer_id,
                CASE
                    WHEN is_first_order = 1 THEN buyer_id
                    ELSE NULL
                END AS new_buyer_id,
                CASE
                    WHEN is_reacquired_buyer = 1 THEN buyer_id
                    ELSE NULL
                END AS reacquired_buyer_id,
                CONCAT(sales_order_id, sku_id) AS order_id,
                COALESCE(actual_gmv, 0) AS gmv_vnd,
                COALESCE(actual_gmv, 0) * COALESCE(exchange_rate, 0) AS gmv_usd
            FROM lazada_cdm.dws_lzd_mkt_trn_uam_di
            WHERE 1 = 1 --
                -- AND     ds >= TO_CHAR(DATEADD(TO_DATE('${start_date}','yyyymmdd'),-5,'dd'),'yyyymmdd') --<< Start
                AND ds >= TO_CHAR(
                    DATEADD(TO_DATE('${bizdate}', 'yyyymmdd'), -15, 'dd'),
                    'yyyymmdd'
                ) --<< Daily
                AND venture = 'VN' --
                -- AND     TO_CHAR(fulfillment_create_date,'yyyymmdd') BETWEEN '${start_date}' AND '${end_date}' --<< Start
                AND TO_CHAR(fulfillment_create_date, 'yyyymmdd') >= TO_CHAR(
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
                ) IN ('cps affiliate')
        ) AS t1
        LEFT JOIN (
            SELECT sales_order_item_id,
                brand_payout_vnd
            FROM t_payout
        ) AS t2 ON t1.sales_order_item_id = t2.sales_order_item_id
    GROUP BY t1.ds,
        t1.member_id
),
t_agg AS (
    SELECT ds,
        member_id,
        SUM(app_click_cnt) AS app_click_cnt,
        SUM(app_install_cnt_lt_1d) AS app_install_cnt_lt_1d,
        SUM(app_reinstall_cnt_lt_1d) AS app_reinstall_cnt_lt_1d,
        SUM(register_cnt_ft_1d_np) AS register_cnt_ft_1d_np,
        SUM(total_payout_vnd) AS total_payout_vnd,
        SUM(platform_payout_vnd) AS platform_payout_vnd,
        SUM(brand_payout_vnd) AS brand_payout_vnd,
        SUM(total_payout_usd) AS total_payout_usd,
        SUM(platform_payout_usd) AS platform_payout_usd,
        SUM(brand_payout_usd) AS brand_payout_usd,
        SUM(app_visit_cnt_pc_session) AS app_visit_cnt_pc_session,
        SUM(app_visit_cnt_ft_1d_np) AS app_visit_cnt_ft_1d_np,
        SUM(app_dau_cnt_ft_1d_np) AS app_dau_cnt_ft_1d_np,
        SUM(app_pdp_pv_ft_1d_np) AS app_pdp_pv_ft_1d_np,
        SUM(app_pdp_uv_cnt_ft_1d_np) AS app_pdp_uv_cnt_ft_1d_np,
        SUM(a2c_cnt_lt_1d_p) AS a2c_cnt_lt_1d_p,
        SUM(a2c_uv_cnt_lt_1d_p) AS a2c_uv_cnt_lt_1d_p,
        SUM(buyer_cnt_lt_1d_p) AS buyer_cnt_lt_1d_p,
        SUM(existing_buyer_cnt_lt_1d_p) AS existing_buyer_cnt_lt_1d_p,
        SUM(new_buyer_cnt_lt_1d_p) AS new_buyer_cnt_lt_1d_p,
        SUM(reacquired_buyer_cnt_lt_1d_p) AS reacquired_buyer_cnt_lt_1d_p,
        SUM(order_cnt_lt_1d_p) AS order_cnt_lt_1d_p,
        SUM(sa_order_cnt_lt_1d_p) AS sa_order_cnt_lt_1d_p,
        SUM(non_sa_order_cnt_lt_1d_p) AS non_sa_order_cnt_lt_1d_p,
        SUM(gmv_vnd_lt_1d_p) AS gmv_vnd_lt_1d_p,
        SUM(sa_gmv_vnd_lt_1d_p) AS sa_gmv_vnd_lt_1d_p,
        SUM(non_sa_gmv_vnd_lt_1d_p) AS non_sa_gmv_vnd_lt_1d_p,
        SUM(gmv_usd_lt_1d_p) AS gmv_usd_lt_1d_p,
        SUM(sa_gmv_usd_lt_1d_p) AS sa_gmv_usd_lt_1d_p,
        SUM(non_sa_gmv_usd_lt_1d_p) AS non_sa_gmv_usd_lt_1d_p
    FROM (
            SELECT ds,
                member_id,
                app_click_cnt,
                0 AS app_install_cnt_lt_1d,
                0 AS app_reinstall_cnt_lt_1d,
                0 AS register_cnt_ft_1d_np,
                0 AS total_payout_vnd,
                0 AS platform_payout_vnd,
                0 AS brand_payout_vnd,
                0 AS total_payout_usd,
                0 AS platform_payout_usd,
                0 AS brand_payout_usd,
                0 AS app_visit_cnt_pc_session,
                0 AS app_visit_cnt_ft_1d_np,
                0 AS app_dau_cnt_ft_1d_np,
                0 AS app_pdp_pv_ft_1d_np,
                0 AS app_pdp_uv_cnt_ft_1d_np,
                0 AS a2c_cnt_lt_1d_p,
                0 AS a2c_uv_cnt_lt_1d_p,
                0 AS buyer_cnt_lt_1d_p,
                0 AS existing_buyer_cnt_lt_1d_p,
                0 AS new_buyer_cnt_lt_1d_p,
                0 AS reacquired_buyer_cnt_lt_1d_p,
                0 AS order_cnt_lt_1d_p,
                0 AS sa_order_cnt_lt_1d_p,
                0 AS non_sa_order_cnt_lt_1d_p,
                0 AS gmv_vnd_lt_1d_p,
                0 AS sa_gmv_vnd_lt_1d_p,
                0 AS non_sa_gmv_vnd_lt_1d_p,
                0 AS gmv_usd_lt_1d_p,
                0 AS sa_gmv_usd_lt_1d_p,
                0 AS non_sa_gmv_usd_lt_1d_p
            FROM t_click_app_ads
            UNION ALL
            SELECT ds,
                member_id,
                0 AS app_click_cnt,
                app_install_cnt_lt_1d,
                app_reinstall_cnt_lt_1d,
                0 AS register_cnt_ft_1d_np,
                0 AS total_payout_vnd,
                0 AS platform_payout_vnd,
                0 AS brand_payout_vnd,
                0 AS total_payout_usd,
                0 AS platform_payout_usd,
                0 AS brand_payout_usd,
                0 AS app_visit_cnt_pc_session,
                0 AS app_visit_cnt_ft_1d_np,
                0 AS app_dau_cnt_ft_1d_np,
                0 AS app_pdp_pv_ft_1d_np,
                0 AS app_pdp_uv_cnt_ft_1d_np,
                0 AS a2c_cnt_lt_1d_p,
                0 AS a2c_uv_cnt_lt_1d_p,
                0 AS buyer_cnt_lt_1d_p,
                0 AS existing_buyer_cnt_lt_1d_p,
                0 AS new_buyer_cnt_lt_1d_p,
                0 AS reacquired_buyer_cnt_lt_1d_p,
                0 AS order_cnt_lt_1d_p,
                0 AS sa_order_cnt_lt_1d_p,
                0 AS non_sa_order_cnt_lt_1d_p,
                0 AS gmv_vnd_lt_1d_p,
                0 AS sa_gmv_vnd_lt_1d_p,
                0 AS non_sa_gmv_vnd_lt_1d_p,
                0 AS gmv_usd_lt_1d_p,
                0 AS sa_gmv_usd_lt_1d_p,
                0 AS non_sa_gmv_usd_lt_1d_p
            FROM t_install_reinstall_app_ads
            UNION ALL
            SELECT ds,
                member_id,
                0 AS app_click_cnt,
                0 AS app_install_cnt_lt_1d,
                0 AS app_reinstall_cnt_lt_1d,
                register_cnt_ft_1d_np,
                0 AS total_payout_vnd,
                0 AS platform_payout_vnd,
                0 AS brand_payout_vnd,
                0 AS total_payout_usd,
                0 AS platform_payout_usd,
                0 AS brand_payout_usd,
                0 AS app_visit_cnt_pc_session,
                0 AS app_visit_cnt_ft_1d_np,
                0 AS app_dau_cnt_ft_1d_np,
                0 AS app_pdp_pv_ft_1d_np,
                0 AS app_pdp_uv_cnt_ft_1d_np,
                0 AS a2c_cnt_lt_1d_p,
                0 AS a2c_uv_cnt_lt_1d_p,
                0 AS buyer_cnt_lt_1d_p,
                0 AS existing_buyer_cnt_lt_1d_p,
                0 AS new_buyer_cnt_lt_1d_p,
                0 AS reacquired_buyer_cnt_lt_1d_p,
                0 AS order_cnt_lt_1d_p,
                0 AS sa_order_cnt_lt_1d_p,
                0 AS non_sa_order_cnt_lt_1d_p,
                0 AS gmv_vnd_lt_1d_p,
                0 AS sa_gmv_vnd_lt_1d_p,
                0 AS non_sa_gmv_vnd_lt_1d_p,
                0 AS gmv_usd_lt_1d_p,
                0 AS sa_gmv_usd_lt_1d_p,
                0 AS non_sa_gmv_usd_lt_1d_p
            FROM t_reg_ads
            UNION ALL
            SELECT ds,
                member_id,
                0 AS app_click_cnt,
                0 AS app_install_cnt_lt_1d,
                0 AS app_reinstall_cnt_lt_1d,
                0 AS register_cnt_ft_1d_np,
                total_payout_vnd,
                platform_payout_vnd,
                brand_payout_vnd,
                total_payout_usd,
                platform_payout_usd,
                brand_payout_usd,
                0 AS app_visit_cnt_pc_session,
                0 AS app_visit_cnt_ft_1d_np,
                0 AS app_dau_cnt_ft_1d_np,
                0 AS app_pdp_pv_ft_1d_np,
                0 AS app_pdp_uv_cnt_ft_1d_np,
                0 AS a2c_cnt_lt_1d_p,
                0 AS a2c_uv_cnt_lt_1d_p,
                0 AS buyer_cnt_lt_1d_p,
                0 AS existing_buyer_cnt_lt_1d_p,
                0 AS new_buyer_cnt_lt_1d_p,
                0 AS reacquired_buyer_cnt_lt_1d_p,
                0 AS order_cnt_lt_1d_p,
                0 AS sa_order_cnt_lt_1d_p,
                0 AS non_sa_order_cnt_lt_1d_p,
                0 AS gmv_vnd_lt_1d_p,
                0 AS sa_gmv_vnd_lt_1d_p,
                0 AS non_sa_gmv_vnd_lt_1d_p,
                0 AS gmv_usd_lt_1d_p,
                0 AS sa_gmv_usd_lt_1d_p,
                0 AS non_sa_gmv_usd_lt_1d_p
            FROM t_payout_ads
            UNION ALL
            SELECT ds,
                member_id,
                0 AS app_click_cnt,
                0 AS app_install_cnt_lt_1d,
                0 AS app_reinstall_cnt_lt_1d,
                0 AS register_cnt_ft_1d_np,
                0 AS total_payout_vnd,
                0 AS platform_payout_vnd,
                0 AS brand_payout_vnd,
                0 AS total_payout_usd,
                0 AS platform_payout_usd,
                0 AS brand_payout_usd,
                app_visit_cnt_pc_session,
                app_visit_cnt_ft_1d_np,
                app_dau_cnt_ft_1d_np,
                app_pdp_pv_ft_1d_np,
                app_pdp_uv_cnt_ft_1d_np,
                0 AS a2c_cnt_lt_1d_p,
                0 AS a2c_uv_cnt_lt_1d_p,
                0 AS buyer_cnt_lt_1d_p,
                0 AS existing_buyer_cnt_lt_1d_p,
                0 AS new_buyer_cnt_lt_1d_p,
                0 AS reacquired_buyer_cnt_lt_1d_p,
                0 AS order_cnt_lt_1d_p,
                0 AS sa_order_cnt_lt_1d_p,
                0 AS non_sa_order_cnt_lt_1d_p,
                0 AS gmv_vnd_lt_1d_p,
                0 AS sa_gmv_vnd_lt_1d_p,
                0 AS non_sa_gmv_vnd_lt_1d_p,
                0 AS gmv_usd_lt_1d_p,
                0 AS sa_gmv_usd_lt_1d_p,
                0 AS non_sa_gmv_usd_lt_1d_p
            FROM t_trf_app_ads
            UNION ALL
            SELECT ds,
                member_id,
                0 AS app_click_cnt,
                0 AS app_install_cnt_lt_1d,
                0 AS app_reinstall_cnt_lt_1d,
                0 AS register_cnt_ft_1d_np,
                0 AS total_payout_vnd,
                0 AS platform_payout_vnd,
                0 AS brand_payout_vnd,
                0 AS total_payout_usd,
                0 AS platform_payout_usd,
                0 AS brand_payout_usd,
                0 AS app_visit_cnt_pc_session,
                0 AS app_visit_cnt_ft_1d_np,
                0 AS app_dau_cnt_ft_1d_np,
                0 AS app_pdp_pv_ft_1d_np,
                0 AS app_pdp_uv_cnt_ft_1d_np,
                a2c_cnt_lt_1d_p,
                a2c_uv_cnt_lt_1d_p,
                0 AS buyer_cnt_lt_1d_p,
                0 AS existing_buyer_cnt_lt_1d_p,
                0 AS new_buyer_cnt_lt_1d_p,
                0 AS reacquired_buyer_cnt_lt_1d_p,
                0 AS order_cnt_lt_1d_p,
                0 AS sa_order_cnt_lt_1d_p,
                0 AS non_sa_order_cnt_lt_1d_p,
                0 AS gmv_vnd_lt_1d_p,
                0 AS sa_gmv_vnd_lt_1d_p,
                0 AS non_sa_gmv_vnd_lt_1d_p,
                0 AS gmv_usd_lt_1d_p,
                0 AS sa_gmv_usd_lt_1d_p,
                0 AS non_sa_gmv_usd_lt_1d_p
            FROM t_a2c_ads
            UNION ALL
            SELECT ds,
                member_id,
                0 AS app_click_cnt,
                0 AS app_install_cnt_lt_1d,
                0 AS app_reinstall_cnt_lt_1d,
                0 AS register_cnt_ft_1d_np,
                0 AS total_payout_vnd,
                0 AS platform_payout_vnd,
                0 AS brand_payout_vnd,
                0 AS total_payout_usd,
                0 AS platform_payout_usd,
                0 AS brand_payout_usd,
                0 AS app_visit_cnt_pc_session,
                0 AS app_visit_cnt_ft_1d_np,
                0 AS app_dau_cnt_ft_1d_np,
                0 AS app_pdp_pv_ft_1d_np,
                0 AS app_pdp_uv_cnt_ft_1d_np,
                0 AS a2c_cnt_lt_1d_p,
                0 AS a2c_uv_cnt_lt_1d_p,
                buyer_cnt_lt_1d_p,
                buyer_cnt_lt_1d_p - new_buyer_cnt_lt_1d_p - reacquired_buyer_cnt_lt_1d_p AS existing_buyer_cnt_lt_1d_p,
                new_buyer_cnt_lt_1d_p,
                reacquired_buyer_cnt_lt_1d_p,
                order_cnt_lt_1d_p,
                sa_order_cnt_lt_1d_p,
                non_sa_order_cnt_lt_1d_p,
                gmv_vnd_lt_1d_p,
                sa_gmv_vnd_lt_1d_p,
                non_sa_gmv_vnd_lt_1d_p,
                gmv_usd_lt_1d_p,
                sa_gmv_usd_lt_1d_p,
                non_sa_gmv_usd_lt_1d_p
            FROM t_trn_ads
        )
    GROUP BY ds,
        member_id
) --
INSERT OVERWRITE TABLE lazada_analyst_dev.loutruong_aff_perf_plm_ops_di PARTITION (ds)
SELECT member_id,
    app_click_cnt,
    app_install_cnt_lt_1d,
    app_reinstall_cnt_lt_1d,
    register_cnt_ft_1d_np,
    total_payout_vnd,
    platform_payout_vnd,
    brand_payout_vnd,
    total_payout_usd,
    platform_payout_usd,
    brand_payout_usd,
    app_visit_cnt_pc_session,
    app_visit_cnt_ft_1d_np,
    app_dau_cnt_ft_1d_np,
    app_pdp_pv_ft_1d_np,
    app_pdp_uv_cnt_ft_1d_np,
    a2c_cnt_lt_1d_p,
    a2c_uv_cnt_lt_1d_p,
    buyer_cnt_lt_1d_p,
    existing_buyer_cnt_lt_1d_p,
    new_buyer_cnt_lt_1d_p,
    reacquired_buyer_cnt_lt_1d_p,
    order_cnt_lt_1d_p,
    sa_order_cnt_lt_1d_p,
    non_sa_order_cnt_lt_1d_p,
    gmv_vnd_lt_1d_p,
    sa_gmv_vnd_lt_1d_p,
    non_sa_gmv_vnd_lt_1d_p,
    gmv_usd_lt_1d_p,
    sa_gmv_usd_lt_1d_p,
    non_sa_gmv_usd_lt_1d_p,
    ds
FROM t_agg;
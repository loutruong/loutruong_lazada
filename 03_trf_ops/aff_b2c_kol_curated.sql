-- MaxCompute SQL 
-- ********************************************************************--
-- author:Truong, Van Thanh
-- create time:2024-05-09 10:28:34
-- ********************************************************************--
--@@ Input = lazada_analyst.loutruong_external_mapping --<< Mapping rule
--@@ Input = lazada_cdm.dim_lzd_mkt_pid_info_df
--@@ Input = lazada_analyst.loutruong_aff_mem_id_offline_info
--@@ Input = lazada_ads.ads_lzd_marketing_cps_conversion_report_mi -->> lazada_analyst_dev.loutruong_aff_console_fulfill_di
--@@ Input = lazada_biz_sec.elbert_cps_affiliate_visitsource_performance_daily_table --<< For ref, Elbert table to learn about the logic external sources
--@@ Input = lazada_cdm.dwd_lzd_clickserver_log_di -->> lazada_analyst_dev.loutruong_dwd_lzd_clickserver_log_di
--@@ Input = lazada_cdm.dws_lzd_mkt_app_ad_mp_di --<< App LAYER 1 (Performance)
--@@ Input = lazada_cdm.dwd_lzd_mkt_app_visit_uam_base_di --<< App LAYER 2
--@@ Input = lazada_cdm.dwd_lzd_mkt_app_traffic_pv_di --<< App LAYER 3 (LORD LAYER)
--@@ Input = lazada_cdm.dws_lzd_mkt_web_ad_mp_di --<< Web / Wapp LAYER 1
--@@ Input = lazada_cdm.dwd_lzd_mkt_web_visit_uam_base_di --<< Web / Wapp LAYER 2
--@@ Input = lazada_cdm.dwd_lzd_mkt_web_traffic_pv_di --<< Web / Wapp LAYER 3 (LORD LAYER)
--@@ Input = lazada_cdm.dwd_lzd_mkt_a2c_uam_core_di
--@@ Input = lazada_cdm.dws_lzd_mkt_trn_uam_di
--@@ Input = lazada_cdm.dim_lzd_exchange_rate
--@@ Input = lazada_cdm.dwd_lzd_app_pv_channel_di
--@@ Input = lazada_cdm.dwd_lzd_web_pv_channel_di
--@@ Input = lazada_analyst_dev.hg_aff_positive_pc25_monthly
-- ********************************************************************--
-- Clean up data
-- ********************************************************************--
WITH t_click_1 AS (
    SELECT t1.ds AS ds,
        t2.member_id_clk_svr AS member_id_clk_svr_first,
        t1.member_id_clk_svr AS member_id_clk_svr_second,
        t2.clickid AS clickid_first,
        t1.clickid AS clickid_second,
        t1.old_clickid AS old_clickid_second,
        t2.domain AS domain_clickid_first,
        t1.domain AS domain_clickid_second,
        t2.referer_url AS referer_url_first,
        t2.referer_host AS referer_host_first,
        t1.referer_url AS referer_url_second,
        t1.referer_host AS referer_host_second,
        t2.click_url AS click_url_first,
        t2.lp_url AS lp_url_first,
        t1.click_url AS click_url_second,
        t1.lp_url AS lp_url_second,
        COALESCE(
            t1.referer_url,
            t1.click_url,
            t2.referer_url,
            t2.click_url
        ) AS referer_url_fixed,
        COALESCE(
            t1.referer_host,
            t1.domain,
            t2.referer_host,
            t2.domain
        ) AS referer_host_fixed
    FROM (
            SELECT ds,
                clickid,
                old_clickid,
                member_id AS member_id_clk_svr,
                referer_url,
                referer_host,
                domain,
                click_url,
                lp_url
            FROM lazada_analyst_dev.loutruong_dwd_lzd_clickserver_log_di
            WHERE 1 = 1
                AND ds BETWEEN '20240401' AND TO_CHAR(DATEADD(GETDATE(), -1, 'dd'), 'yyyymmdd') --<< CONTROLLER
                AND old_clickid IS NOT NULL
        ) AS t1 --<< Click ID Second
        LEFT JOIN (
            SELECT ds,
                clickid,
                old_clickid,
                member_id AS member_id_clk_svr,
                referer_url,
                referer_host,
                domain,
                click_url,
                lp_url
            FROM lazada_analyst_dev.loutruong_dwd_lzd_clickserver_log_di
            WHERE 1 = 1
                AND ds BETWEEN '20240401' AND TO_CHAR(DATEADD(GETDATE(), -1, 'dd'), 'yyyymmdd') --<< CONTROLLER
                AND old_clickid IS NULL
        ) AS t2 --<< Click ID First
        ON t1.old_clickid = t2.clickid
    ORDER BY t2.clickid ASC,
        t1.clickid DESC,
        t1.old_clickid ASC
),
t_click_2 AS (
    SELECT t1.ds AS ds,
        t1.click_id AS click_id,
        t1.member_id_clk_svr_first AS member_id_clk_svr_first,
        t1.member_id_clk_svr_second AS member_id_clk_svr_second,
        t1.domain_clickid_first AS domain_clickid_first,
        t1.domain_clickid_second AS domain_clickid_second,
        t1.referer_url_first AS referer_url_first,
        t1.referer_host_first AS referer_host_first,
        t1.referer_url_second AS referer_url_second,
        t1.referer_host_second AS referer_host_second,
        t1.click_url_first AS click_url_first,
        t1.lp_url_first AS lp_url_first,
        t1.click_url_second AS click_url_second,
        t1.lp_url_second AS lp_url_second,
        t1.referer_url_fixed AS referer_url_fixed,
        t1.referer_host_fixed AS referer_host_fixed,
        COALESCE(t2.referer_name, 'Other') AS referer_name,
        COALESCE(t2.referer_type, 'Other') AS referer_type
    FROM (
            SELECT ds,
                member_id_clk_svr_first,
                member_id_clk_svr_second,
                clickid_first AS click_id,
                domain_clickid_first,
                domain_clickid_second,
                referer_url_first,
                referer_host_first,
                referer_url_second,
                referer_host_second,
                click_url_first,
                lp_url_first,
                click_url_second,
                lp_url_second,
                referer_url_fixed,
                referer_host_fixed
            FROM t_click_1
            UNION ALL
            --<< BREAK POINT
            SELECT ds,
                member_id_clk_svr_first,
                member_id_clk_svr_second,
                clickid_second AS click_id,
                domain_clickid_first,
                domain_clickid_second,
                referer_url_first,
                referer_host_first,
                referer_url_second,
                referer_host_second,
                click_url_first,
                lp_url_first,
                click_url_second,
                lp_url_second,
                referer_url_fixed,
                referer_host_fixed
            FROM t_click_1
        ) AS t1
        LEFT JOIN (
            SELECT referer_host,
                referer_name,
                referer_type
            FROM lazada_analyst.loutruong_external_mapping
        ) AS t2 ON TOLOWER(REPLACE(t1.referer_host_fixed, ' ', '')) = TOLOWER(REPLACE(t2.referer_host, ' ', ''))
),
t_click_3 AS (
    SELECT t1.ds AS ds,
        t1.clickid AS click_id,
        t1.member_id_clk_svr AS member_id_clk_svr,
        t1.referer_url AS referer_url,
        t1.referer_host AS referer_host,
        t1.domain AS domain,
        t1.click_url AS click_url,
        t1.lp_url AS lp_url,
        t1.referer_url_fixed AS referer_url_fixed,
        t1.referer_host_fixed AS referer_host_fixed,
        COALESCE(t2.referer_name, 'Other') AS referer_name,
        COALESCE(t2.referer_type, 'Other') AS referer_type
    FROM (
            SELECT ds,
                clickid,
                old_clickid,
                member_id AS member_id_clk_svr,
                referer_url,
                referer_host,
                domain,
                click_url,
                lp_url,
                COALESCE(referer_url, click_url) AS referer_url_fixed,
                COALESCE(referer_host, domain) AS referer_host_fixed
            FROM lazada_analyst_dev.loutruong_dwd_lzd_clickserver_log_di
            WHERE 1 = 1
                AND ds BETWEEN '20240401' AND TO_CHAR(DATEADD(GETDATE(), -1, 'dd'), 'yyyymmdd') --<< CONTROLLER
                AND old_clickid IS NULL --<< Click ID First
        ) AS t1
        LEFT JOIN (
            SELECT referer_host,
                referer_name,
                referer_type
            FROM lazada_analyst.loutruong_external_mapping
        ) AS t2 ON TOLOWER(REPLACE(t1.referer_host_fixed, ' ', '')) = TOLOWER(REPLACE(t2.referer_host, ' ', ''))
),
t_click_ads AS (
    SELECT t1.ds AS ds,
        t1.member_id AS member_id,
        COALESCE(t2.referer_name, 'Other') AS referer_name,
        COALESCE(t2.referer_type, 'Other') AS referer_type,
        COUNT(*) AS click
    FROM (
            SELECT ds,
                clickid,
                old_clickid,
                member_id,
                referer_url,
                referer_host,
                domain,
                click_url,
                lp_url,
                COALESCE(referer_url, click_url, 'Untrackable') AS referer_url_fixed,
                COALESCE(referer_host, domain, 'Untrackable') AS referer_host_fixed
            FROM lazada_analyst_dev.loutruong_dwd_lzd_clickserver_log_di
            WHERE 1 = 1
                AND ds BETWEEN '20240401' AND TO_CHAR(DATEADD(GETDATE(), -1, 'dd'), 'yyyymmdd') --<< CONTROLLER
                AND bm = 2
            ORDER BY clickid DESC,
                old_clickid ASC
        ) AS t1
        LEFT JOIN (
            SELECT referer_host,
                referer_name,
                referer_type
            FROM lazada_analyst.loutruong_external_mapping
        ) AS t2 ON TOLOWER(REPLACE(t1.referer_host_fixed, ' ', '')) = TOLOWER(REPLACE(t2.referer_host, ' ', ''))
    GROUP BY t1.ds,
        t1.member_id,
        COALESCE(t2.referer_name, 'Other'),
        COALESCE(t2.referer_type, 'Other')
),
t_trf_ads AS (
    SELECT t1.ds AS ds,
        t1.member_id AS member_id,
        COALESCE(t2.referer_name, t3.referer_name, 'Untrackable') AS referer_name,
        COALESCE(t2.referer_type, t3.referer_type, 'Untrackable') AS referer_type,
        COUNT(
            DISTINCT CASE
                WHEN t1.is_active_utdid = 1 THEN t1.session_id
                ELSE NULL
            END
        ) AS app_visit,
        COUNT(
            DISTINCT CASE
                WHEN t1.is_active_utdid = 1 THEN t1.utdid
                ELSE NULL
            END
        ) AS app_dau,
        SUM(
            CASE
                WHEN t1.url_type IN ('ipv') THEN t1.pv_count
                ELSE 0
            END
        ) AS app_pdp_pv,
        COUNT(
            DISTINCT CASE
                WHEN t1.is_active_utdid = 1
                AND t1.url_type IN ('ipv') THEN t1.utdid
                ELSE NULL
            END
        ) AS app_pdp_uv
    FROM (
            SELECT ds,
                member_id,
                click_id,
                old_clickid,
                session_id,
                utdid,
                is_active_utdid,
                url_type,
                NVL(pv_count, 0) AS pv_count
            FROM lazada_cdm.dws_lzd_mkt_app_ad_mp_di
            WHERE 1 = 1
                AND ds BETWEEN '20240401' AND TO_CHAR(DATEADD(GETDATE(), -1, 'dd'), 'yyyymmdd') --<< CONTROLLER
                AND venture = 'VN'
                AND attr_model IN ('ft_1d_np')
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
        ) AS t1
        LEFT JOIN (
            SELECT ds,
                member_id_clk_svr_first,
                member_id_clk_svr_second,
                click_id,
                domain_clickid_first,
                domain_clickid_second,
                referer_url_first,
                referer_host_first,
                referer_url_second,
                referer_host_second,
                click_url_first,
                lp_url_first,
                click_url_second,
                lp_url_second,
                referer_url_fixed,
                referer_host_fixed,
                referer_name,
                referer_type
            FROM t_click_2
        ) AS t2 ON COALESCE(t1.click_id, CONCAT('nodata', rand() * 9999)) = t2.click_id
        LEFT JOIN (
            SELECT ds,
                click_id,
                member_id_clk_svr,
                referer_url,
                referer_host,
                domain,
                click_url,
                lp_url,
                referer_url_fixed,
                referer_host_fixed,
                referer_name,
                referer_type
            FROM t_click_3
        ) AS t3 ON COALESCE(t1.click_id, CONCAT('nodata', rand() * 9999)) = t3.click_id
    GROUP BY t1.ds,
        t1.member_id,
        COALESCE(t2.referer_name, t3.referer_name, 'Untrackable'),
        COALESCE(t2.referer_type, t3.referer_type, 'Untrackable')
),
t_trn_ads AS (
    SELECT t1.ds AS ds,
        t1.member_id AS member_id,
        COALESCE(t4.referer_name, t5.referer_name, 'Untrackable') AS referer_name,
        COALESCE(t4.referer_type, t5.referer_type, 'Untrackable') AS referer_type,
        COUNT(DISTINCT t1.buyer_id) AS buyer_cnt,
        COUNT(
            DISTINCT CASE
                WHEN t1.is_first_order <> 1
                OR t1.is_reacquired_buyer <> 1 THEN t1.buyer_id
                ELSE NULL
            END
        ) AS exist_buyer_cnt,
        COUNT(
            DISTINCT CASE
                WHEN t1.is_first_order = 1 THEN t1.buyer_id
                ELSE NULL
            END
        ) AS new_buyer_cnt,
        COUNT(
            DISTINCT CASE
                WHEN t1.is_reacquired_buyer = 1 THEN t1.buyer_id
                ELSE NULL
            END
        ) AS reacq_buyer_cnt,
        COUNT(DISTINCT t1.order_id) AS order_cnt,
        COUNT(
            DISTINCT CASE
                WHEN t2.brand_commission_fee > 0 THEN t1.order_id
                ELSE NULL
            END
        ) AS sa_order_cnt,
        COUNT(
            DISTINCT CASE
                WHEN t2.brand_commission_fee = 0 THEN t1.order_id
                ELSE NULL
            END
        ) AS non_sa_order_cnt,
        COUNT(
            DISTINCT CASE
                WHEN t3.seller_id IS NOT NULL THEN t1.order_id
                ELSE NULL
            END
        ) AS pc25_positive_order_cnt,
        COUNT(
            DISTINCT CASE
                WHEN t3.seller_id IS NULL THEN t1.order_id
                ELSE NULL
            END
        ) AS pc25_negative_order_cnt,
        SUM(t1.gmv_usd) AS gmv,
        SUM(
            CASE
                WHEN t2.brand_commission_fee > 0 THEN t1.gmv_usd
                ELSE 0
            END
        ) AS sa_gmv,
        SUM(
            CASE
                WHEN t2.brand_commission_fee = 0 THEN t1.gmv_usd
                ELSE 0
            END
        ) AS non_sa_gmv,
        SUM(
            CASE
                WHEN t3.seller_id IS NOT NULL THEN t1.gmv_usd
                ELSE 0
            END
        ) AS pc25_positive_gmv,
        SUM(
            CASE
                WHEN t3.seller_id IS NULL THEN t1.gmv_usd
                ELSE 0
            END
        ) AS pc25_negative_gmv
    FROM (
            SELECT TO_CHAR(fulfillment_create_date, 'yyyymmdd') AS ds,
                member_id,
                click_id,
                old_clickid,
                sales_order_item_id,
                buyer_id,
                is_first_order,
                is_reacquired_buyer,
                seller_id,
                CONCAT(sales_order_id, sku_id) AS order_id,
                COALESCE(actual_gmv, 0) * COALESCE(exchange_rate, 0) AS gmv_usd
            FROM lazada_cdm.dws_lzd_mkt_trn_uam_di
            WHERE 1 = 1
                AND ds BETWEEN '20240401' AND TO_CHAR(DATEADD(GETDATE(), -1, 'dd'), 'yyyymmdd') --<< CONTROLLER
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
        ) AS t1
        LEFT JOIN (
            SELECT sub_order_id,
                brand_commission_fee
            FROM lazada_ads.ads_lzd_marketing_cps_conversion_report_mi
            WHERE 1 = 1
                AND mm >= 202404
                AND venture = 'VN'
                AND TO_CHAR(
                    TO_DATE(fulfilled_time, 'yyyy-mm-ddTHH:mi'),
                    'yyyymmdd'
                ) BETWEEN '20240401' AND TO_CHAR(DATEADD(GETDATE(), -1, 'dd'), 'yyyymmdd') --<< CONTROLLER
                AND TOLOWER(status) IN ('rejected', 'returned')
                AND source IS NOT NULL
        ) AS t2 ON t1.sales_order_item_id = t2.sub_order_id
        LEFT JOIN (
            SELECT seller_id
            FROM lazada_analyst_dev.hg_aff_positive_pc25_monthly
            WHERE 1 = 1
                AND ds = MAX_PT(
                    'lazada_analyst_dev.hg_aff_positive_pc25_monthly'
                )
                AND month IN (
                    TO_CHAR(
                        DATEADD(DATEADD(GETDATE(), -1, 'dd'), -1, 'mm'),
                        'yyyymm'
                    )
                )
        ) AS t3 ON t1.seller_id = t3.seller_id
        LEFT JOIN (
            SELECT ds,
                member_id_clk_svr_first,
                member_id_clk_svr_second,
                click_id,
                domain_clickid_first,
                domain_clickid_second,
                referer_url_first,
                referer_host_first,
                referer_url_second,
                referer_host_second,
                click_url_first,
                lp_url_first,
                click_url_second,
                lp_url_second,
                referer_url_fixed,
                referer_host_fixed,
                referer_name,
                referer_type
            FROM t_click_2
        ) AS t4 ON COALESCE(t1.click_id, CONCAT('nodata', rand() * 9999)) = t4.click_id
        LEFT JOIN (
            SELECT ds,
                click_id,
                member_id_clk_svr,
                referer_url,
                referer_host,
                domain,
                click_url,
                lp_url,
                referer_url_fixed,
                referer_host_fixed,
                referer_name,
                referer_type
            FROM t_click_3
        ) AS t5 ON COALESCE(t1.click_id, CONCAT('nodata', rand() * 9999)) = t5.click_id
    GROUP BY t1.ds,
        t1.member_id,
        COALESCE(t4.referer_name, t5.referer_name, 'Untrackable'),
        COALESCE(t4.referer_type, t5.referer_type, 'Untrackable')
),
t_payout_ads AS (
    SELECT t1.ds AS ds,
        t1.member_id AS member_id,
        COALESCE(t3.referer_name, t4.referer_name, 'Untrackable') AS referer_name,
        COALESCE(t3.referer_type, t4.referer_type, 'Untrackable') AS referer_type,
        SUM(
            COALESCE(t1.total_payout_vnd, 0) * COALESCE(t2.exchange_rate, 0)
        ) AS po_eligible,
        SUM(
            COALESCE(t1.platform_payout_vnd, 0) * COALESCE(t2.exchange_rate, 0)
        ) AS plt_po_eligible,
        SUM(
            COALESCE(t1.brand_payout_vnd, 0) * COALESCE(t2.exchange_rate, 0)
        ) AS brd_po_eligible
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
            WHERE 1 = 1
                AND mm >= 202405
                AND venture = 'VN'
                AND TO_CHAR(
                    TO_DATE(fulfilled_time, 'yyyy-mm-ddTHH:mi'),
                    'yyyymmdd'
                ) BETWEEN '20240401' AND TO_CHAR(DATEADD(GETDATE(), -1, 'dd'), 'yyyymmdd') --<< CONTROLLER
                AND TOLOWER(adjust_type) NOT IN ('stop_first_order')
                AND COALESCE(is_fraud, 0) = 0
                AND TOLOWER(status) NOT IN ('delivered')
                AND source IS NOT NULL
        ) AS t1
        LEFT JOIN (
            SELECT DISTINCT sales_order_item_id,
                click_id,
                old_clickid,
                exchange_rate
            FROM lazada_cdm.dws_lzd_mkt_trn_uam_di
            WHERE 1 = 1
                AND ds BETWEEN '20240401' AND TO_CHAR(DATEADD(GETDATE(), -1, 'dd'), 'yyyymmdd') --<< CONTROLLER
                AND venture = 'VN'
                AND attr_model IN ('ltpdp_settlement_voucher_p')
        ) AS t2 ON t1.sub_order_id = t2.sales_order_item_id
        LEFT JOIN (
            SELECT ds,
                member_id_clk_svr_first,
                member_id_clk_svr_second,
                click_id,
                domain_clickid_first,
                domain_clickid_second,
                referer_url_first,
                referer_host_first,
                referer_url_second,
                referer_host_second,
                click_url_first,
                lp_url_first,
                click_url_second,
                lp_url_second,
                referer_url_fixed,
                referer_host_fixed,
                referer_name,
                referer_type
            FROM t_click_2
        ) AS t3 ON COALESCE(t2.click_id, CONCAT('nodata', rand() * 9999)) = t3.click_id
        LEFT JOIN (
            SELECT ds,
                click_id,
                member_id_clk_svr,
                referer_url,
                referer_host,
                domain,
                click_url,
                lp_url,
                referer_url_fixed,
                referer_host_fixed,
                referer_name,
                referer_type
            FROM t_click_3
        ) AS t4 ON COALESCE(t2.click_id, CONCAT('nodata', rand() * 9999)) = t4.click_id
    GROUP BY t1.ds,
        t1.member_id,
        COALESCE(t3.referer_name, t4.referer_name, 'Untrackable'),
        COALESCE(t3.referer_type, t4.referer_type, 'Untrackable')
)
SELECT t1.ds AS ds,
    t2.member_id AS member_id,
    t2.member_name AS member_name,
    t2.segment_inventory AS segment_inventory,
    t2.team AS team,
    t2.plm_tier AS plm_tier,
    t2.pc25_tier AS pc25_tier,
    t2.pic AS pic,
    t1.referer_name AS source_name,
    t1.referer_type AS source_type,
    t1.po_eligible AS po_eligible,
    t1.plt_po_eligible AS plt_po_eligible,
    t1.brd_po_eligible AS brd_po_eligible,
    t1.click AS click,
    t1.app_visit AS app_visit,
    t1.app_dau AS app_dau,
    t1.app_pdp_pv AS app_pdp_pv,
    t1.app_pdp_uv AS app_pdp_uv,
    t1.buyer_cnt AS buyer_cnt,
    t1.exist_buyer_cnt AS exist_buyer_cnt,
    t1.new_buyer_cnt AS new_buyer_cnt,
    t1.reacq_buyer_cnt AS reacq_buyer_cnt,
    t1.order_cnt AS order_cnt,
    t1.sa_order_cnt AS sa_order_cnt,
    t1.non_sa_order_cnt AS non_sa_order_cnt,
    t1.pc25_positive_order_cnt AS pc25_positive_order_cnt,
    t1.pc25_negative_order_cnt AS pc25_negative_order_cnt,
    t1.gmv AS gmv,
    t1.sa_gmv AS sa_gmv,
    t1.non_sa_gmv AS non_sa_gmv,
    t1.pc25_positive_gmv AS pc25_positive_gmv,
    t1.pc25_negative_gmv AS pc25_negative_gmv
FROM (
        SELECT ds,
            member_id,
            referer_name,
            referer_type,
            SUM(po_eligible) AS po_eligible,
            SUM(plt_po_eligible) AS plt_po_eligible,
            SUM(brd_po_eligible) AS brd_po_eligible,
            SUM(click) AS click,
            SUM(app_visit) AS app_visit,
            SUM(app_dau) AS app_dau,
            SUM(app_pdp_pv) AS app_pdp_pv,
            SUM(app_pdp_uv) AS app_pdp_uv,
            SUM(buyer_cnt) AS buyer_cnt,
            SUM(exist_buyer_cnt) AS exist_buyer_cnt,
            SUM(new_buyer_cnt) AS new_buyer_cnt,
            SUM(reacq_buyer_cnt) AS reacq_buyer_cnt,
            SUM(order_cnt) AS order_cnt,
            SUM(sa_order_cnt) AS sa_order_cnt,
            SUM(non_sa_order_cnt) AS non_sa_order_cnt,
            SUM(pc25_positive_order_cnt) AS pc25_positive_order_cnt,
            SUM(pc25_negative_order_cnt) AS pc25_negative_order_cnt,
            SUM(gmv) AS gmv,
            SUM(sa_gmv) AS sa_gmv,
            SUM(non_sa_gmv) AS non_sa_gmv,
            SUM(pc25_positive_gmv) AS pc25_positive_gmv,
            SUM(pc25_negative_gmv) AS pc25_negative_gmv
        FROM (
                SELECT ds,
                    member_id,
                    referer_name,
                    referer_type,
                    0 AS po_eligible,
                    0 AS plt_po_eligible,
                    0 AS brd_po_eligible,
                    click,
                    0 AS app_visit,
                    0 AS app_dau,
                    0 AS app_pdp_pv,
                    0 AS app_pdp_uv,
                    0 AS buyer_cnt,
                    0 AS exist_buyer_cnt,
                    0 AS new_buyer_cnt,
                    0 AS reacq_buyer_cnt,
                    0 AS order_cnt,
                    0 AS sa_order_cnt,
                    0 AS non_sa_order_cnt,
                    0 AS pc25_positive_order_cnt,
                    0 AS pc25_negative_order_cnt,
                    0 AS gmv,
                    0 AS sa_gmv,
                    0 AS non_sa_gmv,
                    0 AS pc25_positive_gmv,
                    0 AS pc25_negative_gmv
                FROM t_click_ads
                UNION ALL
                --<< BREAK POINT
                SELECT ds,
                    member_id,
                    referer_name,
                    referer_type,
                    0 AS po_eligible,
                    0 AS plt_po_eligible,
                    0 AS brd_po_eligible,
                    0 AS click,
                    app_visit,
                    app_dau,
                    app_pdp_pv,
                    app_pdp_uv,
                    0 AS buyer_cnt,
                    0 AS exist_buyer_cnt,
                    0 AS new_buyer_cnt,
                    0 AS reacq_buyer_cnt,
                    0 AS order_cnt,
                    0 AS sa_order_cnt,
                    0 AS non_sa_order_cnt,
                    0 AS pc25_positive_order_cnt,
                    0 AS pc25_negative_order_cnt,
                    0 AS gmv,
                    0 AS sa_gmv,
                    0 AS non_sa_gmv,
                    0 AS pc25_positive_gmv,
                    0 AS pc25_negative_gmv
                FROM t_trf_ads
                UNION ALL
                --<< BREAK POINT
                SELECT ds,
                    member_id,
                    referer_name,
                    referer_type,
                    0 AS po_eligible,
                    0 AS plt_po_eligible,
                    0 AS brd_po_eligible,
                    0 AS click,
                    0 AS app_visit,
                    0 AS app_dau,
                    0 AS app_pdp_pv,
                    0 AS app_pdp_uv,
                    buyer_cnt,
                    exist_buyer_cnt,
                    new_buyer_cnt,
                    reacq_buyer_cnt,
                    order_cnt,
                    sa_order_cnt,
                    non_sa_order_cnt,
                    pc25_positive_order_cnt,
                    pc25_negative_order_cnt,
                    gmv,
                    sa_gmv,
                    non_sa_gmv,
                    pc25_positive_gmv,
                    pc25_negative_gmv
                FROM t_trn_ads
                UNION ALL
                --<< BREAK POINT
                SELECT ds,
                    member_id,
                    referer_name,
                    referer_type,
                    po_eligible,
                    plt_po_eligible,
                    brd_po_eligible,
                    0 AS click,
                    0 AS app_visit,
                    0 AS app_dau,
                    0 AS app_pdp_pv,
                    0 AS app_pdp_uv,
                    0 AS buyer_cnt,
                    0 AS exist_buyer_cnt,
                    0 AS new_buyer_cnt,
                    0 AS reacq_buyer_cnt,
                    0 AS order_cnt,
                    0 AS sa_order_cnt,
                    0 AS non_sa_order_cnt,
                    0 AS pc25_positive_order_cnt,
                    0 AS pc25_negative_order_cnt,
                    0 AS gmv,
                    0 AS sa_gmv,
                    0 AS non_sa_gmv,
                    0 AS pc25_positive_gmv,
                    0 AS pc25_negative_gmv
                FROM t_payout_ads
            )
        GROUP BY ds,
            member_id,
            referer_name,
            referer_type
    ) AS t1
    INNER JOIN (
        SELECT month_apply,
            ds,
            ms,
            cycle,
            exclude_reason,
            exclude_number,
            member_id,
            member_name,
            segment_inventory,
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
            AND member_id IN (
                180901237,
                150031234,
                150081363,
                150111201,
                152811302,
                150691243,
                183941377,
                150071392,
                150591363,
                150431395,
                165561321,
                150761314,
                189411273,
                150081394,
                150071377,
                150011267,
                164791354,
                150591315,
                191511301,
                150421238,
                150051277,
                150081396,
                150611381,
                158831398,
                183741388,
                150131334,
                172861257,
                150081381,
                150751239,
                150761203,
                182461242,
                211531212,
                150071374,
                193501366,
                182541341,
                185771220,
                165871274,
                207921394,
                158941398,
                167811381,
                211601396,
                168961297,
                150061378,
                160621391,
                150081376,
                165301293,
                150441307,
                185051368,
                154301392,
                150641305,
                168761226,
                154351292,
                168681285,
                159561321,
                178541282,
                153941204,
                209791292,
                158911339,
                150451212,
                160201382,
                150611262,
                197851292,
                150491246,
                150381220,
                182481242,
                150081352,
                150441226,
                161251230,
                160581331,
                154031333,
                178991225,
                199771253,
                207821307,
                172971364,
                201851390,
                150091307,
                150461355,
                177471394,
                178591380,
                153311226,
                155541250,
                169071360,
                208971288,
                200251286,
                150081388,
                195211277,
                150651294,
                156291370,
                211061217,
                155611379,
                195921337,
                193141354,
                186911251,
                158151282,
                150461222,
                182031317,
                195631334,
                150261319,
                192651283,
                178841250,
                186451358,
                177051210,
                175811229,
                150421368,
                182571268,
                168201298,
                197111395,
                191321307,
                212241229,
                169711385,
                178651362,
                193741219,
                178041269,
                192671354,
                202911294,
                178241246,
                189431211,
                192531218,
                180591396,
                150021209,
                212041361,
                201951322,
                150041286,
                200421398,
                154031354,
                169161223,
                150421335,
                209151296,
                177631274,
                165011237,
                192521250,
                150131335,
                153311286,
                186661374,
                150761216,
                208711251,
                212371394,
                234811314,
                191481373,
                180251292,
                183771303,
                150441230,
                181951275,
                179041203,
                211951204,
                208891224,
                197261223,
                150691285,
                176081230,
                160351291,
                150031235,
                181531285,
                150581400,
                167761203,
                150011292,
                150611224,
                179471390,
                211841287,
                159961228,
                155861382,
                211441245,
                200421249,
                177001318,
                177301253,
                179741379,
                207451366,
                160331380,
                167551302,
                194311282,
                150431332,
                150051266,
                188391397,
                192531238,
                208321399,
                182491228,
                211721354,
                203651213,
                174721262,
                153261382,
                158451208,
                181531247,
                208521264,
                177511315,
                211651315,
                208141253,
                185021276,
                203211323,
                208101319,
                194151278,
                211591380,
                185741286,
                195341283,
                199881345,
                150361296,
                179091369,
                158161238,
                174691385,
                150011230,
                165551371,
                180931272,
                155851204,
                211901231,
                150641232,
                150071368,
                186671383,
                192661264,
                208171294,
                177881257,
                177691242,
                185791230,
                160721251,
                197831295,
                179411216,
                211941349,
                169261233,
                150511226,
                150521239,
                209281256,
                193671300,
                194461215,
                211161308,
                178761213,
                156131251,
                150071369,
                150091327,
                210771237,
                178321375,
                179451218,
                155541213,
                185771229,
                186531336,
                185781214,
                199771351,
                150311386,
                182401322,
                192651286,
                174791301,
                200451400,
                197751390,
                150171286,
                150081326,
                200401362,
                201951321,
                192671260,
                202931217,
                150591397,
                166191336,
                182391375,
                150511299,
                203111204,
                194961322,
                177551356,
                150091301,
                179721344,
                183771307,
                212181336,
                194721384,
                184041295,
                192521249,
                182501235,
                172851223,
                194831219,
                155861371,
                193681255,
                195341338,
                177961282,
                150121344,
                193491303,
                152831256,
                191361388,
                178551293,
                180251340,
                208541226,
                180801222,
                199131293,
                188401364,
                212011248,
                169001352,
                150071376,
                177721237,
                180041285,
                192701310,
                180601361,
                150411374,
                153311265,
                150081360,
                155311261,
                182391376,
                183771320,
                153261400,
                208351365,
                179371320,
                180531224,
                208341372,
                158951347,
                186871297,
                200251279,
                150021299,
                212251269,
                150111251,
                184601251,
                178971212,
                200411280,
                168341275,
                179791314,
                156641373,
                194721247,
                194761377,
                195431279,
                193761265,
                191381343,
                211821212,
                184311309,
                192501299,
                182581270,
                211601341,
                193481304,
                161191362,
                198701394,
                185791214,
                208561208,
                150411315,
                211551396,
                155471342,
                150031237,
                208411315,
                158441281,
                200461239,
                183721397,
                183401339,
                154031334,
                150751213,
                202001349,
                177691224,
                192701305,
                212161380,
                175181389,
                210941207,
                197771220,
                212041214,
                196041219,
                185751296,
                178891300,
                193821317,
                155471370,
                194761381,
                150051276,
                152811360,
                150531294,
                179431294,
                184991399,
                194111225,
                186671326,
                192421269,
                211201362,
                176821319,
                182441216,
                150511247,
                202471355,
                185001355,
                176091201,
                183721386,
                150111207,
                206291296,
                150071255,
                188411374,
                210901308,
                159101301,
                178611386,
                180021249,
                181331346,
                150011279,
                194301214,
                150101300,
                211401327,
                150091360,
                193931344,
                185741284,
                244111400,
                229261225,
                180801359,
                181791304,
                150011228,
                150501279,
                158831399,
                210681384,
                150531249,
                193791359,
                192661257,
                189431227,
                196041233,
                150431295,
                168961270,
                182251397,
                179291374,
                150691286,
                153941205,
                180861242,
                183121360,
                208821315,
                211411209,
                150451389,
                191501342,
                212071400,
                185771235,
                150061237,
                181981234,
                166861344,
                177841203,
                186491323,
                208171326,
                156641316,
                157571351,
                150471308,
                150101368,
                194671375,
                200421237,
                185781204,
                193771235,
                150431294,
                211161331,
                170041208,
                188411346,
                208401360,
                182581258,
                168121337,
                208461211,
                211881268,
                178481394,
                184351387,
                150111241,
                210721391,
                174111230,
                194111293,
                164671364,
                229831221,
                150751226,
                150081226,
                150371320,
                192521279,
                178831268,
                210251248,
                186461340,
                178811223,
                194301240,
                150541357,
                193731288,
                153951203,
                171271206,
                241091277,
                192411310,
                182531380,
                182411385,
                193931382,
                184291342,
                169161276,
                202401394,
                202811348,
                150391307,
                192471241,
                211801223,
                152831246,
                158551203,
                192701306,
                184051268,
                164801392,
                191341349,
                200431378,
                168831376,
                182461234,
                228151288,
                228451242,
                233141239
            )
    ) AS t2 ON t1.member_id = t2.member_id;
-- ********************************************************************--
-- Supper raw
-- ********************************************************************--
WITH t_click_1 AS (
    SELECT t1.ds AS ds,
        t2.member_id_clk_svr AS member_id_clk_svr_first,
        t1.member_id_clk_svr AS member_id_clk_svr_second,
        t2.clickid AS clickid_first,
        t1.clickid AS clickid_second,
        t1.old_clickid AS old_clickid_second,
        t2.domain AS domain_clickid_first,
        t1.domain AS domain_clickid_second,
        t2.referer_url AS referer_url_first,
        t2.referer_host AS referer_host_first,
        t1.referer_url AS referer_url_second,
        t1.referer_host AS referer_host_second,
        t2.click_url AS click_url_first,
        t2.lp_url AS lp_url_first,
        t1.click_url AS click_url_second,
        t1.lp_url AS lp_url_second,
        COALESCE(
            t1.referer_url,
            t1.click_url,
            t2.referer_url,
            t2.click_url
        ) AS referer_url_fixed,
        COALESCE(
            t1.referer_host,
            t1.domain,
            t2.referer_host,
            t2.domain
        ) AS referer_host_fixed
    FROM (
            SELECT ds,
                clickid,
                old_clickid,
                member_id AS member_id_clk_svr,
                referer_url,
                referer_host,
                domain,
                click_url,
                lp_url
            FROM lazada_analyst_dev.loutruong_dwd_lzd_clickserver_log_di
            WHERE 1 = 1
                AND ds BETWEEN '20240401' AND TO_CHAR(DATEADD(GETDATE(), -1, 'dd'), 'yyyymmdd') --<< CONTROLLER
                AND old_clickid IS NOT NULL
        ) AS t1 --<< Click ID Second
        LEFT JOIN (
            SELECT ds,
                clickid,
                old_clickid,
                member_id AS member_id_clk_svr,
                referer_url,
                referer_host,
                domain,
                click_url,
                lp_url
            FROM lazada_analyst_dev.loutruong_dwd_lzd_clickserver_log_di
            WHERE 1 = 1
                AND ds BETWEEN '20240401' AND TO_CHAR(DATEADD(GETDATE(), -1, 'dd'), 'yyyymmdd') --<< CONTROLLER
                AND old_clickid IS NULL
        ) AS t2 --<< Click ID First
        ON t1.old_clickid = t2.clickid
    ORDER BY t2.clickid ASC,
        t1.clickid DESC,
        t1.old_clickid ASC
)
SELECT *
FROM t_click_1;
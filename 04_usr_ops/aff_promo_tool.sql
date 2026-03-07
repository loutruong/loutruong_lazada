-- MaxCompute SQL 
-- ********************************************************************--
-- author:Truong, Van Thanh
-- create time:2024-03-02 16:33:02
-- ********************************************************************--
--@@ Input = lazada_analyst.loutruong_aff_mem_id_offline_info ==>> lazada_analyst_dev.loutruong_aff_mem_id_offline_info
--@@ Input = lazada_analyst.loutruong_aff_b2c_ops_vc_mapping
--@@ Input = lazada_cdm.dwd_lzd_pro_promotion_item_hh_vn
--@@ Input = lazada_cdm.dwd_lzd_pro_collectibles_hh
--@@ Input = lazada_analyst.loutruong_aff_vc_mapping
--@@ Input = lazada_cdm.dwd_lzd_trd_core_hh
--@@ Input = lazada_cdm.dwd_lzd_mkt_trn_uam_hi
-- ********************************************************************--
-- Current month
-- ********************************************************************--
SELECT t1.promo_month AS promo_month,
    CONCAT(
        TO_CHAR(DATETRUNC(GETDATE(), 'month'), 'yyyymmdd'),
        ' ',
        '-->>',
        ' ',
        TO_CHAR(GETDATE(), 'yyyymmdd')
    ) AS data_range,
    (
        SELECT MAX(TO_CHAR(create_date, 'hh'))
        FROM lazada_cdm.dwd_lzd_pro_promotion_item_hh_vn
        WHERE 1 = 1
            AND ds = TO_CHAR(DATEADD(GETDATE(), 0, 'dd'), 'yyyymmdd')
            AND venture = 'VN'
    ) AS hh_latest,
    t1.promo_period AS promo_period,
    t1.promo_id AS promo_id,
    t1.promo_code AS promo_code,
    t1.promo_scheme AS promo_scheme,
    t1.promo_dsv AS promo_dsv,
    t1.promo_aov AS promo_aov,
    t1.promo_type AS promo_type,
    t1.promo_start AS promo_start,
    t1.promo_end AS promo_end,
    t1.promo_mem_id_dis AS promo_mem_id_dis,
    t1.promo_stock AS promo_stock,
    MAX(MAX(t2.order_create_date)) OVER (PARTITION BY t1.promo_code) AS promo_used_latest,
    COUNT(DISTINCT t3.check_out_id) AS redeemed,
    SUM(
        COALESCE(t2.promo_amt_vnd, 0) * COALESCE(t3.exchange_rate, 0)
    ) AS promo_amt_usd,
    COUNT(
        DISTINCT CASE
            WHEN TOLOWER(t1.promo_type) IN ('vp')
            AND TOLOWER(COALESCE(t4.funding_bucket, 'Unknown')) IN ('lazada om')
            AND TOLOWER(COALESCE(t4.funding_type, 'Unknown')) IN ('om', 'ams')
            AND TOLOWER(COALESCE(t4.sub_channel, 'Unknown')) IN ('cps affiliate')
            AND t1.promo_mem_id_dis = t4.member_id THEN t3.check_out_id
            ELSE NULL
        END
    ) AS redeemed_correctly,
    SUM(
        CASE
            WHEN TOLOWER(t1.promo_type) IN ('vp')
            AND TOLOWER(COALESCE(t4.funding_bucket, 'Unknown')) IN ('lazada om')
            AND TOLOWER(COALESCE(t4.funding_type, 'Unknown')) IN ('om', 'ams')
            AND TOLOWER(COALESCE(t4.sub_channel, 'Unknown')) IN ('cps affiliate')
            AND t1.promo_mem_id_dis = t4.member_id THEN COALESCE(t2.promo_amt_vnd, 0) * COALESCE(t3.exchange_rate, 0)
            ELSE 0
        END
    ) AS promo_amt_correctly,
    COUNT(
        DISTINCT CASE
            WHEN TOLOWER(t1.promo_type) IN ('vp')
            AND t1.promo_mem_id_dis <> t4.member_id THEN t3.check_out_id
            ELSE NULL
        END
    ) AS redeemed_no_correctly,
    SUM(
        CASE
            WHEN TOLOWER(t1.promo_type) IN ('vp')
            AND t1.promo_mem_id_dis <> t4.member_id THEN COALESCE(t2.promo_amt_vnd, 0) * COALESCE(t3.exchange_rate, 0)
            ELSE 0
        END
    ) AS promo_amt_no_correctly,
    COUNT(
        DISTINCT CASE
            WHEN TOLOWER(t1.promo_type) IN ('vp')
            AND TOLOWER(COALESCE(t4.funding_bucket, 'Unknown')) IN ('lazada om')
            AND TOLOWER(COALESCE(t4.funding_type, 'Unknown')) IN ('om', 'ams')
            AND TOLOWER(COALESCE(t4.sub_channel, 'Unknown')) IN ('cps affiliate')
            AND t1.promo_mem_id_dis <> t4.member_id THEN t3.check_out_id
            ELSE NULL
        END
    ) AS redeemed_within_aff_no_correctly,
    SUM(
        CASE
            WHEN TOLOWER(t1.promo_type) IN ('vp')
            AND TOLOWER(COALESCE(t4.funding_bucket, 'Unknown')) IN ('lazada om')
            AND TOLOWER(COALESCE(t4.funding_type, 'Unknown')) IN ('om', 'ams')
            AND TOLOWER(COALESCE(t4.sub_channel, 'Unknown')) IN ('cps affiliate')
            AND t1.promo_mem_id_dis <> t4.member_id THEN COALESCE(t2.promo_amt_vnd, 0) * COALESCE(t3.exchange_rate, 0)
            ELSE 0
        END
    ) AS promo_amt_within_aff_no_correctly,
    COUNT(
        DISTINCT CASE
            WHEN TOLOWER(t1.promo_type) IN ('vp')
            AND TOLOWER(COALESCE(t4.funding_bucket, 'Unknown')) NOT IN ('lazada om')
            AND TOLOWER(COALESCE(t4.funding_type, 'Unknown')) NOT IN ('om', 'ams')
            AND TOLOWER(COALESCE(t4.sub_channel, 'Unknown')) NOT IN ('cps affiliate')
            AND t1.promo_mem_id_dis <> t4.member_id THEN t3.check_out_id
            ELSE NULL
        END
    ) AS redeemed_without_aff_no_correctly,
    SUM(
        CASE
            WHEN TOLOWER(t1.promo_type) IN ('vp')
            AND TOLOWER(COALESCE(t4.funding_bucket, 'Unknown')) NOT IN ('lazada om')
            AND TOLOWER(COALESCE(t4.funding_type, 'Unknown')) NOT IN ('om', 'ams')
            AND TOLOWER(COALESCE(t4.sub_channel, 'Unknown')) NOT IN ('cps affiliate')
            AND t1.promo_mem_id_dis <> t4.member_id THEN COALESCE(t2.promo_amt_vnd, 0) * COALESCE(t3.exchange_rate, 0)
            ELSE 0
        END
    ) AS promo_amt_without_aff_no_correctly,
    COUNT(
        DISTINCT CASE
            WHEN TOLOWER(t1.promo_type) IN ('alp')
            AND TOLOWER(COALESCE(t4.funding_bucket, 'Unknown')) IN ('lazada om')
            AND TOLOWER(COALESCE(t4.funding_type, 'Unknown')) IN ('om', 'ams')
            AND TOLOWER(COALESCE(t4.sub_channel, 'Unknown')) IN ('cps affiliate')
            AND t5.member_id IS NOT NULL THEN t3.check_out_id
            ELSE NULL
        END
    ) AS b2c_ops_redeemed_correctly,
    SUM(
        CASE
            WHEN TOLOWER(t1.promo_type) IN ('alp')
            AND TOLOWER(COALESCE(t4.funding_bucket, 'Unknown')) IN ('lazada om')
            AND TOLOWER(COALESCE(t4.funding_type, 'Unknown')) IN ('om', 'ams')
            AND TOLOWER(COALESCE(t4.sub_channel, 'Unknown')) IN ('cps affiliate')
            AND t5.member_id IS NOT NULL THEN COALESCE(t2.promo_amt_vnd, 0) * COALESCE(t3.exchange_rate, 0)
            ELSE 0
        END
    ) AS b2c_ops_promo_amt_correctly,
    COUNT(
        DISTINCT CASE
            WHEN TOLOWER(t1.promo_type) IN ('alp')
            AND t5.member_id IS NULL THEN t3.check_out_id
            ELSE NULL
        END
    ) AS b2c_ops_redeemed_no_correctly,
    SUM(
        CASE
            WHEN TOLOWER(t1.promo_type) IN ('alp')
            AND t5.member_id IS NULL THEN COALESCE(t2.promo_amt_vnd, 0) * COALESCE(t3.exchange_rate, 0)
            ELSE 0
        END
    ) AS b2c_ops_promo_amt_usd_no_correctly,
    COUNT(
        DISTINCT CASE
            WHEN TOLOWER(t1.promo_type) IN ('alp')
            AND TOLOWER(COALESCE(t4.funding_bucket, 'Unknown')) IN ('lazada om')
            AND TOLOWER(COALESCE(t4.funding_type, 'Unknown')) IN ('om', 'ams')
            AND TOLOWER(COALESCE(t4.sub_channel, 'Unknown')) IN ('cps affiliate')
            AND t5.member_id IS NULL THEN t3.check_out_id
            ELSE NULL
        END
    ) AS b2c_ops_redeemed_within_aff_no_correctly,
    SUM(
        CASE
            WHEN TOLOWER(t1.promo_type) IN ('alp')
            AND TOLOWER(COALESCE(t4.funding_bucket, 'Unknown')) IN ('lazada om')
            AND TOLOWER(COALESCE(t4.funding_type, 'Unknown')) IN ('om', 'ams')
            AND TOLOWER(COALESCE(t4.sub_channel, 'Unknown')) IN ('cps affiliate')
            AND t5.member_id IS NULL THEN COALESCE(t2.promo_amt_vnd, 0) * COALESCE(t3.exchange_rate, 0)
            ELSE 0
        END
    ) AS b2c_ops_promo_amt_within_aff_no_correctly,
    COUNT(
        DISTINCT CASE
            WHEN TOLOWER(t1.promo_type) IN ('alp')
            AND TOLOWER(COALESCE(t4.funding_bucket, 'Unknown')) IN ('lazada om')
            AND TOLOWER(COALESCE(t4.funding_type, 'Unknown')) IN ('om', 'ams')
            AND TOLOWER(COALESCE(t4.sub_channel, 'Unknown')) IN ('cps affiliate')
            AND t5.member_id IS NULL THEN t3.check_out_id
            ELSE NULL
        END
    ) AS b2c_ops_redeemed_without_aff_no_correctly,
    SUM(
        CASE
            WHEN TOLOWER(t1.promo_type) IN ('alp')
            AND TOLOWER(COALESCE(t4.funding_bucket, 'Unknown')) IN ('lazada om')
            AND TOLOWER(COALESCE(t4.funding_type, 'Unknown')) IN ('om', 'ams')
            AND TOLOWER(COALESCE(t4.sub_channel, 'Unknown')) IN ('cps affiliate')
            AND t5.member_id IS NULL THEN COALESCE(t2.promo_amt_vnd, 0) * COALESCE(t3.exchange_rate, 0)
            ELSE 0
        END
    ) AS b2c_ops_promo_amt_without_aff_no_correctly
FROM (
        SELECT promo_month,
            promo_period,
            promo_id,
            promo_code,
            promo_scheme,
            promo_dsv,
            promo_aov,
            promo_type,
            promo_start,
            promo_end,
            promo_stock,
            promo_mem_id_dis
        FROM lazada_analyst.loutruong_aff_b2c_ops_vc_mapping
    ) AS t1
    LEFT JOIN (
        SELECT create_date AS order_create_date,
            sales_order_item_id,
            promotion_id AS promo_id,
            voucher_code AS promo_code,
            promotion_amount AS promo_amt_vnd
        FROM lazada_cdm.dwd_lzd_pro_promotion_item_hh_vn
        WHERE 1 = 1
            AND ds = TO_CHAR(GETDATE(), 'yyyymmdd')
            AND venture = 'VN'
            AND promotion_type IN ('voucherCoupon')
            AND TOLOWER(promotion_role) IN ('platform')
            AND (
                TOLOWER(retail_sponsor) IN ('platform')
                OR retail_sponsor IS NULL
            )
        UNION ALL
        SELECT create_date AS order_create_date,
            sales_order_item_id,
            promotion_id AS promo_id,
            voucher_code AS promo_code,
            promotion_amount AS promo_amt_vnd
        FROM lazada_cdm.dwd_lzd_pro_promotion_item_di
        WHERE 1 = 1
            AND TO_CHAR(TO_DATE(ds, 'yyyymmdd'), 'yyyymm') IN (TO_CHAR(DATEADD(GETDATE(), -1, 'dd'), 'yyyymm'))
            AND venture = 'VN'
            AND promotion_type IN ('voucherCoupon')
            AND TOLOWER(promotion_role) IN ('platform')
            AND (
                TOLOWER(retail_sponsor) IN ('platform')
                OR retail_sponsor IS NULL
            )
    ) AS t2 ON t1.promo_id = t2.promo_id
    AND TOLOWER(t1.promo_code) = TOLOWER(t2.promo_code)
    LEFT JOIN (
        SELECT sales_order_item_id,
            check_out_id,
            exchange_rate,
            item_status_esm
        FROM lazada_cdm.dwd_lzd_trd_core_hh
        WHERE 1 = 1
            AND ds = TO_CHAR(GETDATE(), 'yyyymmdd')
            AND venture = 'VN'
            AND COALESCE(business_application, 'LZD') IN ('LZD,ZAL', 'LZD')
        UNION ALL
        SELECT sales_order_item_id,
            check_out_id,
            exchange_rate,
            item_status_esm
        FROM lazada_cdm.dwd_lzd_trd_core_create_di
        WHERE 1 = 1
            AND TO_CHAR(TO_DATE(ds, 'yyyymmdd'), 'yyyymm') IN (TO_CHAR(DATEADD(GETDATE(), -1, 'dd'), 'yyyymm'))
            AND venture = 'VN'
            AND COALESCE(business_application, 'LZD') IN ('LZD,ZAL', 'LZD')
    ) AS t3 ON t2.sales_order_item_id = t3.sales_order_item_id
    LEFT JOIN (
        SELECT DISTINCT sales_order_item_id,
            lazada_cdm.mkt_get_updated_funding_bucket(
                channel,
                COALESCE(funding_bucket, 'Unknown'),
                COALESCE(member_name, 'Unknown')
            ) AS funding_bucket,
            lazada_cdm.mkt_get_updated_funding_type(
                channel,
                COALESCE(funding_bucket, 'Unknown'),
                COALESCE(funding_type, 'Unknown'),
                COALESCE(member_name, 'Unknown')
            ) AS funding_type,
            lazada_cdm.mkt_get_sub_channel_from_json(
                sg_udf :bi_put_json_values(
                    '{}',
                    'channel',
                    sg_udf :bi_to_json_string(channel),
                    'funding_bucket',
                    sg_udf :bi_to_json_string(
                        lazada_cdm.mkt_get_updated_funding_bucket(
                            channel,
                            COALESCE(funding_bucket, 'Unknown'),
                            COALESCE(member_name, 'Unknown')
                        )
                    ),
                    'free_paid',
                    sg_udf :bi_to_json_string(free_paid),
                    'segmentation',
                    sg_udf :bi_to_json_string(segmentation),
                    'rt_bucket',
                    sg_udf :bi_to_json_string(COALESCE(rt_bucket, 'Unknown')),
                    'campaign_type',
                    sg_udf :bi_to_json_string(COALESCE(campaign_type, 'Unknown')),
                    'placement',
                    sg_udf :bi_to_json_string(COALESCE(placement, 'Unknown'))
                )
            ) AS sub_channel,
            SPLIT(pid, '_') [1] AS member_id
        FROM lazada_cdm.dwd_lzd_mkt_trn_uam_hi
        WHERE 1 = 1
            AND ds = TO_CHAR(GETDATE(), 'yyyymmdd')
            AND venture = 'VN'
            AND attr_model IN ('lt_1d_p')
        UNION ALL
        SELECT DISTINCT CAST(sales_order_item_id AS STRING) AS sales_order_item_id,
            lazada_cdm.mkt_get_updated_funding_bucket(
                channel,
                GET_JSON_OBJECT(campaign_info, '$.funding_bucket'),
                partner
            ) AS funding_bucket,
            lazada_cdm.mkt_get_updated_funding_type(
                channel,
                GET_JSON_OBJECT(campaign_info, '$.funding_bucket'),
                GET_JSON_OBJECT(campaign_info, '$.funding_type'),
                partner
            ) AS funding_type,
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
            ) AS sub_channel,
            SPLIT(pid, '_') [1] AS member_id
        FROM lazada_cdm.dws_lzd_mkt_trn_uam_di
        WHERE 1 = 1
            AND TO_CHAR(TO_DATE(ds, 'yyyymmdd'), 'yyyymm') IN (TO_CHAR(DATEADD(GETDATE(), -1, 'dd'), 'yyyymm'))
            AND venture = 'VN'
            AND attr_model IN ('lt_1d_p')
    ) AS t4 ON t3.sales_order_item_id = t4.sales_order_item_id
    LEFT JOIN (
        SELECT member_id
        FROM lazada_analyst_dev.loutruong_aff_mem_id_offline_info
        WHERE 1 = 1
            AND month_apply IN (
                SELECT MAX(month_apply)
                FROM lazada_analyst_dev.loutruong_aff_mem_id_offline_info
            )
    ) AS t5 ON t4.member_id = t5.member_id
GROUP BY t1.promo_month,
    CONCAT(
        TO_CHAR(DATETRUNC(GETDATE(), 'month'), 'yyyymmdd'),
        ' ',
        '-->>',
        ' ',
        TO_CHAR(GETDATE(), 'yyyymmdd')
    ),
    (
        SELECT MAX(TO_CHAR(create_date, 'hh'))
        FROM lazada_cdm.dwd_lzd_pro_promotion_item_hh_vn
        WHERE 1 = 1
            AND ds = TO_CHAR(DATEADD(GETDATE(), 0, 'dd'), 'yyyymmdd')
            AND venture = 'VN'
    ),
    t1.promo_period,
    t1.promo_id,
    t1.promo_code,
    t1.promo_scheme,
    t1.promo_dsv,
    t1.promo_aov,
    t1.promo_type,
    t1.promo_start,
    t1.promo_end,
    t1.promo_mem_id_dis,
    t1.promo_stock;
-- ********************************************************************--
-- Storage monthly
-- ********************************************************************--
SELECT t1.promo_month AS promo_month,
    t1.promo_period AS promo_period,
    t1.promo_id AS promo_id,
    t1.promo_code AS promo_code,
    t1.promo_scheme AS promo_scheme,
    t1.promo_dsv AS promo_dsv,
    t1.promo_aov AS promo_aov,
    t1.promo_type AS promo_type,
    t1.promo_start AS promo_start,
    t1.promo_end AS promo_end,
    t1.promo_mem_id_dis AS promo_mem_id_dis,
    t1.promo_stock AS promo_stock,
    MAX(MAX(t2.order_create_date)) OVER (PARTITION BY t1.promo_code) AS promo_used_latest,
    COUNT(DISTINCT t3.check_out_id) AS redeemed,
    SUM(
        COALESCE(t2.promo_amt_vnd, 0) * COALESCE(t3.exchange_rate, 0)
    ) AS promo_amt_usd,
    COUNT(
        DISTINCT CASE
            WHEN TOLOWER(t1.promo_type) IN ('vp')
            AND TOLOWER(COALESCE(t4.funding_bucket, 'Unknown')) IN ('lazada om')
            AND TOLOWER(COALESCE(t4.funding_type, 'Unknown')) IN ('om', 'ams')
            AND TOLOWER(COALESCE(t4.sub_channel, 'Unknown')) IN ('cps affiliate')
            AND t1.promo_mem_id_dis = t4.member_id THEN t3.check_out_id
            ELSE NULL
        END
    ) AS redeemed_correctly,
    SUM(
        CASE
            WHEN TOLOWER(t1.promo_type) IN ('vp')
            AND TOLOWER(COALESCE(t4.funding_bucket, 'Unknown')) IN ('lazada om')
            AND TOLOWER(COALESCE(t4.funding_type, 'Unknown')) IN ('om', 'ams')
            AND TOLOWER(COALESCE(t4.sub_channel, 'Unknown')) IN ('cps affiliate')
            AND t1.promo_mem_id_dis = t4.member_id THEN COALESCE(t2.promo_amt_vnd, 0) * COALESCE(t3.exchange_rate, 0)
            ELSE 0
        END
    ) AS promo_amt_correctly,
    COUNT(
        DISTINCT CASE
            WHEN TOLOWER(t1.promo_type) IN ('vp')
            AND t1.promo_mem_id_dis <> t4.member_id THEN t3.check_out_id
            ELSE NULL
        END
    ) AS redeemed_no_correctly,
    SUM(
        CASE
            WHEN TOLOWER(t1.promo_type) IN ('vp')
            AND t1.promo_mem_id_dis <> t4.member_id THEN COALESCE(t2.promo_amt_vnd, 0) * COALESCE(t3.exchange_rate, 0)
            ELSE 0
        END
    ) AS promo_amt_no_correctly,
    COUNT(
        DISTINCT CASE
            WHEN TOLOWER(t1.promo_type) IN ('vp')
            AND TOLOWER(COALESCE(t4.funding_bucket, 'Unknown')) IN ('lazada om')
            AND TOLOWER(COALESCE(t4.funding_type, 'Unknown')) IN ('om', 'ams')
            AND TOLOWER(COALESCE(t4.sub_channel, 'Unknown')) IN ('cps affiliate')
            AND t1.promo_mem_id_dis <> t4.member_id THEN t3.check_out_id
            ELSE NULL
        END
    ) AS redeemed_within_aff_no_correctly,
    SUM(
        CASE
            WHEN TOLOWER(t1.promo_type) IN ('vp')
            AND TOLOWER(COALESCE(t4.funding_bucket, 'Unknown')) IN ('lazada om')
            AND TOLOWER(COALESCE(t4.funding_type, 'Unknown')) IN ('om', 'ams')
            AND TOLOWER(COALESCE(t4.sub_channel, 'Unknown')) IN ('cps affiliate')
            AND t1.promo_mem_id_dis <> t4.member_id THEN COALESCE(t2.promo_amt_vnd, 0) * COALESCE(t3.exchange_rate, 0)
            ELSE 0
        END
    ) AS promo_amt_within_aff_no_correctly,
    COUNT(
        DISTINCT CASE
            WHEN TOLOWER(t1.promo_type) IN ('vp')
            AND TOLOWER(COALESCE(t4.funding_bucket, 'Unknown')) NOT IN ('lazada om')
            AND TOLOWER(COALESCE(t4.funding_type, 'Unknown')) NOT IN ('om', 'ams')
            AND TOLOWER(COALESCE(t4.sub_channel, 'Unknown')) NOT IN ('cps affiliate')
            AND t1.promo_mem_id_dis <> t4.member_id THEN t3.check_out_id
            ELSE NULL
        END
    ) AS redeemed_without_aff_no_correctly,
    SUM(
        CASE
            WHEN TOLOWER(t1.promo_type) IN ('vp')
            AND TOLOWER(COALESCE(t4.funding_bucket, 'Unknown')) NOT IN ('lazada om')
            AND TOLOWER(COALESCE(t4.funding_type, 'Unknown')) NOT IN ('om', 'ams')
            AND TOLOWER(COALESCE(t4.sub_channel, 'Unknown')) NOT IN ('cps affiliate')
            AND t1.promo_mem_id_dis <> t4.member_id THEN COALESCE(t2.promo_amt_vnd, 0) * COALESCE(t3.exchange_rate, 0)
            ELSE 0
        END
    ) AS promo_amt_without_aff_no_correctly,
    COUNT(
        DISTINCT CASE
            WHEN TOLOWER(t1.promo_type) IN ('alp')
            AND TOLOWER(COALESCE(t4.funding_bucket, 'Unknown')) IN ('lazada om')
            AND TOLOWER(COALESCE(t4.funding_type, 'Unknown')) IN ('om', 'ams')
            AND TOLOWER(COALESCE(t4.sub_channel, 'Unknown')) IN ('cps affiliate')
            AND t5.member_id IS NOT NULL THEN t3.check_out_id
            ELSE NULL
        END
    ) AS b2c_ops_redeemed_correctly,
    SUM(
        CASE
            WHEN TOLOWER(t1.promo_type) IN ('alp')
            AND TOLOWER(COALESCE(t4.funding_bucket, 'Unknown')) IN ('lazada om')
            AND TOLOWER(COALESCE(t4.funding_type, 'Unknown')) IN ('om', 'ams')
            AND TOLOWER(COALESCE(t4.sub_channel, 'Unknown')) IN ('cps affiliate')
            AND t5.member_id IS NOT NULL THEN COALESCE(t2.promo_amt_vnd, 0) * COALESCE(t3.exchange_rate, 0)
            ELSE 0
        END
    ) AS b2c_ops_promo_amt_correctly,
    COUNT(
        DISTINCT CASE
            WHEN TOLOWER(t1.promo_type) IN ('alp')
            AND t5.member_id IS NULL THEN t3.check_out_id
            ELSE NULL
        END
    ) AS b2c_ops_redeemed_no_correctly,
    SUM(
        CASE
            WHEN TOLOWER(t1.promo_type) IN ('alp')
            AND t5.member_id IS NULL THEN COALESCE(t2.promo_amt_vnd, 0) * COALESCE(t3.exchange_rate, 0)
            ELSE 0
        END
    ) AS b2c_ops_promo_amt_usd_no_correctly,
    COUNT(
        DISTINCT CASE
            WHEN TOLOWER(t1.promo_type) IN ('alp')
            AND TOLOWER(COALESCE(t4.funding_bucket, 'Unknown')) IN ('lazada om')
            AND TOLOWER(COALESCE(t4.funding_type, 'Unknown')) IN ('om', 'ams')
            AND TOLOWER(COALESCE(t4.sub_channel, 'Unknown')) IN ('cps affiliate')
            AND t5.member_id IS NULL THEN t3.check_out_id
            ELSE NULL
        END
    ) AS b2c_ops_redeemed_within_aff_no_correctly,
    SUM(
        CASE
            WHEN TOLOWER(t1.promo_type) IN ('alp')
            AND TOLOWER(COALESCE(t4.funding_bucket, 'Unknown')) IN ('lazada om')
            AND TOLOWER(COALESCE(t4.funding_type, 'Unknown')) IN ('om', 'ams')
            AND TOLOWER(COALESCE(t4.sub_channel, 'Unknown')) IN ('cps affiliate')
            AND t5.member_id IS NULL THEN COALESCE(t2.promo_amt_vnd, 0) * COALESCE(t3.exchange_rate, 0)
            ELSE 0
        END
    ) AS b2c_ops_promo_amt_within_aff_no_correctly,
    COUNT(
        DISTINCT CASE
            WHEN TOLOWER(t1.promo_type) IN ('alp')
            AND TOLOWER(COALESCE(t4.funding_bucket, 'Unknown')) IN ('lazada om')
            AND TOLOWER(COALESCE(t4.funding_type, 'Unknown')) IN ('om', 'ams')
            AND TOLOWER(COALESCE(t4.sub_channel, 'Unknown')) IN ('cps affiliate')
            AND t5.member_id IS NULL THEN t3.check_out_id
            ELSE NULL
        END
    ) AS b2c_ops_redeemed_without_aff_no_correctly,
    SUM(
        CASE
            WHEN TOLOWER(t1.promo_type) IN ('alp')
            AND TOLOWER(COALESCE(t4.funding_bucket, 'Unknown')) IN ('lazada om')
            AND TOLOWER(COALESCE(t4.funding_type, 'Unknown')) IN ('om', 'ams')
            AND TOLOWER(COALESCE(t4.sub_channel, 'Unknown')) IN ('cps affiliate')
            AND t5.member_id IS NULL THEN COALESCE(t2.promo_amt_vnd, 0) * COALESCE(t3.exchange_rate, 0)
            ELSE 0
        END
    ) AS b2c_ops_promo_amt_without_aff_no_correctly
FROM (
        SELECT promo_month,
            promo_period,
            promo_id,
            promo_code,
            promo_scheme,
            promo_dsv,
            promo_aov,
            promo_type,
            promo_start,
            promo_end,
            promo_stock,
            promo_mem_id_dis
        FROM lazada_analyst.loutruong_aff_b2c_ops_vc_mapping_storage
    ) AS t1
    LEFT JOIN (
        SELECT create_date AS order_create_date,
            sales_order_item_id,
            promotion_id AS promo_id,
            voucher_code AS promo_code,
            promotion_amount AS promo_amt_vnd
        FROM lazada_cdm.dwd_lzd_pro_promotion_item_di
        WHERE 1 = 1
            AND TO_CHAR(TO_DATE(ds, 'yyyymmdd'), 'yyyy') >= 2024 --<< Change point
            AND venture = 'VN'
            AND promotion_type IN ('voucherCoupon')
            AND TOLOWER(promotion_role) IN ('platform')
            AND (
                TOLOWER(retail_sponsor) IN ('platform')
                OR retail_sponsor IS NULL
            )
    ) AS t2 ON t1.promo_id = t2.promo_id
    AND TOLOWER(t1.promo_code) = TOLOWER(t2.promo_code)
    LEFT JOIN (
        SELECT sales_order_item_id,
            check_out_id,
            exchange_rate,
            item_status_esm
        FROM lazada_cdm.dwd_lzd_trd_core_create_di
        WHERE 1 = 1
            AND TO_CHAR(TO_DATE(ds, 'yyyymmdd'), 'yyyy') >= 2024 --<< Change point
            AND venture = 'VN'
            AND COALESCE(business_application, 'LZD') IN ('LZD,ZAL', 'LZD')
    ) AS t3 ON t2.sales_order_item_id = t3.sales_order_item_id
    LEFT JOIN (
        SELECT DISTINCT CAST(sales_order_item_id AS STRING) AS sales_order_item_id,
            lazada_cdm.mkt_get_updated_funding_bucket(
                channel,
                GET_JSON_OBJECT(campaign_info, '$.funding_bucket'),
                partner
            ) AS funding_bucket,
            lazada_cdm.mkt_get_updated_funding_type(
                channel,
                GET_JSON_OBJECT(campaign_info, '$.funding_bucket'),
                GET_JSON_OBJECT(campaign_info, '$.funding_type'),
                partner
            ) AS funding_type,
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
            ) AS sub_channel,
            SPLIT(pid, '_') [1] AS member_id
        FROM lazada_cdm.dws_lzd_mkt_trn_uam_di
        WHERE 1 = 1
            AND TO_CHAR(TO_DATE(ds, 'yyyymmdd'), 'yyyy') >= 2024 --<< Change point
            AND venture = 'VN'
            AND attr_model IN ('lt_1d_p')
    ) AS t4 ON t3.sales_order_item_id = t4.sales_order_item_id
    LEFT JOIN (
        SELECT member_id
        FROM lazada_analyst_dev.loutruong_aff_mem_id_offline_info
        WHERE 1 = 1
            AND month_apply IN (
                SELECT MAX(month_apply)
                FROM lazada_analyst_dev.loutruong_aff_mem_id_offline_info
            )
    ) AS t5 ON t4.member_id = t5.member_id
GROUP BY t1.promo_month,
    t1.promo_period,
    t1.promo_id,
    t1.promo_code,
    t1.promo_scheme,
    t1.promo_dsv,
    t1.promo_aov,
    t1.promo_type,
    t1.promo_start,
    t1.promo_end,
    t1.promo_mem_id_dis,
    t1.promo_stock;
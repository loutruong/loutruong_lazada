-- MaxCompute SQL 
-- ********************************************************************--
-- author:vanthanh.truong
-- create time:2023-11-12 15:52:55
-- ********************************************************************--
--@@ Input = lazada_cdm.dwd_lzd_trd_ord_itm_fulfill_hh
--@@ Input = lazada_cdm.dwd_lzd_mkt_trn_uam_hi
--@@ Input = lazada_analyst_dev.hg_aff_plm_mixed
--@@ Input = lazada_cdm.dim_lzd_prd_product_extra_info
--@@ Input = lazada_cdm.dim_lzd_slr_seller_extra_vn
--@@ Input = lazada_cdm.dim_lzd_slr_seller_vn
--@@ Input = lazada_analyst_dev.loutruong_dim_slr_brd_sku_cms
--@@ Input = lazada_analyst_dev.datx_mega_intraday_sku_performance
WITH t_intra_dim_hh AS (
    SELECT MAX(hh) AS hh
    FROM lazada_cdm.dwd_lzd_trd_ord_itm_fulfill_hh
    WHERE 1 = 1
        AND ds = TO_CHAR(DATEADD(GETDATE(), 0, 'dd'), 'yyyymmdd')
        AND venture = 'VN'
),
t_supper_trd_core_dwd AS (
    SELECT ds,
        hh,
        sales_order_item_id,
        order_id,
        order_number,
        mord_id,
        check_out_id,
        CASE
            WHEN buyer_id = 1 THEN buyer_id
            ELSE NULL
        END AS buyer_id,
        gmv_vnd,
        gmv_usd,
        product_id,
        seller_id,
        funding_bucket,
        funding_type,
        sub_channel,
        group_segment,
        segment,
        member_id
    FROM (
            SELECT t1.ds AS ds,
                t1.hh AS hh,
                t1.sales_order_item_id AS sales_order_item_id,
                t1.order_id AS order_id,
                t1.order_number AS order_number,
                t1.mord_id AS mord_id,
                t1.check_out_id AS check_out_id,
                ROW_NUMBER() OVER (
                    PARTITION BY t1.ds,
                    t1.buyer_id
                    ORDER BY t1.hh ASC
                ) AS buyer_id,
                t1.gmv_vnd AS gmv_vnd,
                t1.gmv_usd AS gmv_usd,
                t1.product_id AS product_id,
                t1.seller_id AS seller_id,
                COALESCE(t2.funding_bucket, 'Unknown') AS funding_bucket,
                COALESCE(t2.funding_type, 'Unknown') AS funding_type,
                COALESCE(t2.sub_channel, 'Unknown') AS sub_channel,
                COALESCE(t3.group_segment, 'Unknow_group_segment_2') AS group_segment,
                COALESCE(t3.segment, 'Unknow_segment_2') AS segment,
                COALESCE(t2.member_id, 'Unknown') AS member_id
            FROM (
                    SELECT TO_CHAR(
                            TO_DATE(fulfillment_create_date, 'yyyy-mm-dd hh:mi:ss'),
                            'yyyymmdd'
                        ) AS ds,
                        TO_CHAR(
                            TO_DATE(fulfillment_create_date, 'yyyy-mm-dd hh:mi:ss'),
                            'hh'
                        ) AS hh,
                        CAST(sales_order_item_id AS STRING) AS sales_order_item_id,
                        order_id,
                        CAST(sales_order_id AS STRING) AS order_number,
                        mord_id,
                        checkout_id AS check_out_id,
                        buyer_id,
                        actual_gmv AS gmv_vnd,
                        actual_gmv * exchange_rate AS gmv_usd,
                        product_id,
                        seller_id
                    FROM lazada_cdm.dwd_lzd_trd_ord_itm_fulfill_hh
                    WHERE 1 = 1
                        AND ds >= TO_CHAR(DATEADD(GETDATE(), 0, 'dd'), 'yyyymmdd')
                        AND hh = (
                            SELECT hh
                            FROM t_intra_dim_hh
                        )
                        AND venture = 'VN'
                        AND is_revenue = 1
                    UNION ALL
                    --<< BREAK POINT D0, D1, D2
                    SELECT TO_CHAR(
                            TO_DATE(fulfillment_create_date, 'yyyy-mm-dd hh:mi:ss'),
                            'yyyymmdd'
                        ) AS ds,
                        TO_CHAR(
                            TO_DATE(fulfillment_create_date, 'yyyy-mm-dd hh:mi:ss'),
                            'hh'
                        ) AS hh,
                        CAST(sales_order_item_id AS STRING) AS sales_order_item_id,
                        order_id,
                        CAST(sales_order_id AS STRING) AS order_number,
                        mord_id,
                        checkout_id AS check_out_id,
                        buyer_id,
                        actual_gmv AS gmv_vnd,
                        actual_gmv * exchange_rate AS gmv_usd,
                        product_id,
                        seller_id
                    FROM lazada_cdm.dwd_lzd_trd_ord_itm_fulfill_hh
                    WHERE 1 = 1
                        AND ds BETWEEN TO_CHAR(DATEADD(GETDATE(), -2, 'dd'), 'yyyymmdd') AND TO_CHAR(DATEADD(GETDATE(), -1, 'dd'), 'yyyymmdd')
                        AND hh = 23
                        AND venture = 'VN'
                ) AS t1
                LEFT JOIN (
                    SELECT sales_order_item_id,
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
                        AND ds >= TO_CHAR(DATEADD(GETDATE(), -3, 'dd'), 'yyyymmdd')
                        AND venture = 'VN'
                        AND attr_model IN ('lt_1d_p')
                ) AS t2 ON t1.sales_order_item_id = t2.sales_order_item_id
                LEFT JOIN (
                    SELECT group_segment,
                        team_ops,
                        plm_l30d_lv1,
                        plm_l30d_lv2,
                        segment,
                        member_id
                    FROM lazada_analyst_dev.hg_aff_plm_mixed
                    WHERE 1 = 1
                        AND ds = MAX_PT('lazada_analyst_dev.hg_aff_plm_mixed')
                ) AS t3 ON t2.member_id = t3.member_id --
                -- UNION ALL
                -- SELECT  ds
                --         ,CAST(hh AS STRING) AS hh
                --         ,sales_order_item_id
                --         ,order_id
                --         ,order_number
                --         ,mord_id
                --         ,check_out_id
                --         ,ROW_NUMBER() OVER (PARTITION BY ds,buyer_id ORDER BY hh ASC ) AS buyer_id
                --         ,gmv_vnd
                --         ,gmv_usd
                --         ,product_id
                --         ,seller_id
                --         ,funding_bucket
                --         ,funding_type
                --         ,sub_channel
                --         ,group_segment_2 AS group_segment
                --         ,segment_2 AS segment
                --         ,member_id
                -- FROM    lazada_analyst_dev.loutruong_trd_core_fulfill_ext_trf_di
                -- WHERE   1 = 1
                -- AND     ds < TO_CHAR(DATEADD(GETDATE(),-1,'dd'),'yyyymmdd')
        )
),
t_prd_dws AS (
    SELECT ds,
        hh,
        product_id,
        COUNT(DISTINCT buyer_id) AS buyer_cnt,
        COUNT(DISTINCT order_id) AS order_cnt,
        SUM(gmv_usd) AS gmv_usd,
        COUNT(
            DISTINCT CASE
                WHEN TOLOWER(funding_bucket) IN ('lazada om')
                AND TOLOWER(funding_type) IN ('om', 'ams')
                AND TOLOWER(sub_channel) IN ('cps affiliate') THEN buyer_id
                ELSE NULL
            END
        ) AS aff_buyer_cnt,
        COUNT(
            DISTINCT CASE
                WHEN TOLOWER(funding_bucket) IN ('lazada om')
                AND TOLOWER(funding_type) IN ('om', 'ams')
                AND TOLOWER(sub_channel) IN ('cps affiliate') THEN order_id
                ELSE NULL
            END
        ) AS aff_order_cnt,
        SUM(
            CASE
                WHEN TOLOWER(funding_bucket) IN ('lazada om')
                AND TOLOWER(funding_type) IN ('om', 'ams')
                AND TOLOWER(sub_channel) IN ('cps affiliate') THEN gmv_usd
                ELSE 0
            END
        ) AS aff_gmv_usd,
        COUNT(
            DISTINCT CASE
                WHEN TOLOWER(funding_bucket) IN ('lazada om')
                AND TOLOWER(funding_type) IN ('om', 'ams')
                AND TOLOWER(sub_channel) IN ('cps affiliate')
                AND TOLOWER(group_segment) IN ('b2c') THEN buyer_id
                ELSE NULL
            END
        ) AS aff_b2c_buyer_cnt,
        COUNT(
            DISTINCT CASE
                WHEN TOLOWER(funding_bucket) IN ('lazada om')
                AND TOLOWER(funding_type) IN ('om', 'ams')
                AND TOLOWER(sub_channel) IN ('cps affiliate')
                AND TOLOWER(group_segment) IN ('b2c') THEN order_id
                ELSE NULL
            END
        ) AS aff_b2c_order_cnt,
        SUM(
            CASE
                WHEN TOLOWER(funding_bucket) IN ('lazada om')
                AND TOLOWER(funding_type) IN ('om', 'ams')
                AND TOLOWER(sub_channel) IN ('cps affiliate')
                AND TOLOWER(group_segment) IN ('b2c') THEN gmv_usd
                ELSE 0
            END
        ) AS aff_b2c_gmv_usd
    FROM t_supper_trd_core_dwd
    GROUP BY ds,
        hh,
        product_id
),
t_slr_dws AS (
    SELECT ds,
        hh,
        seller_id,
        COUNT(DISTINCT buyer_id) AS buyer_cnt,
        COUNT(DISTINCT order_id) AS order_cnt,
        SUM(gmv_usd) AS gmv_usd,
        COUNT(
            DISTINCT CASE
                WHEN TOLOWER(funding_bucket) IN ('lazada om')
                AND TOLOWER(funding_type) IN ('om', 'ams')
                AND TOLOWER(sub_channel) IN ('cps affiliate') THEN buyer_id
                ELSE NULL
            END
        ) AS aff_buyer_cnt,
        COUNT(
            DISTINCT CASE
                WHEN TOLOWER(funding_bucket) IN ('lazada om')
                AND TOLOWER(funding_type) IN ('om', 'ams')
                AND TOLOWER(sub_channel) IN ('cps affiliate') THEN order_id
                ELSE NULL
            END
        ) AS aff_order_cnt,
        SUM(
            CASE
                WHEN TOLOWER(funding_bucket) IN ('lazada om')
                AND TOLOWER(funding_type) IN ('om', 'ams')
                AND TOLOWER(sub_channel) IN ('cps affiliate') THEN gmv_usd
                ELSE 0
            END
        ) AS aff_gmv_usd,
        COUNT(
            DISTINCT CASE
                WHEN TOLOWER(funding_bucket) IN ('lazada om')
                AND TOLOWER(funding_type) IN ('om', 'ams')
                AND TOLOWER(sub_channel) IN ('cps affiliate')
                AND TOLOWER(group_segment) IN ('b2c') THEN buyer_id
                ELSE NULL
            END
        ) AS aff_b2c_buyer_cnt,
        COUNT(
            DISTINCT CASE
                WHEN TOLOWER(funding_bucket) IN ('lazada om')
                AND TOLOWER(funding_type) IN ('om', 'ams')
                AND TOLOWER(sub_channel) IN ('cps affiliate')
                AND TOLOWER(group_segment) IN ('b2c') THEN order_id
                ELSE NULL
            END
        ) AS aff_b2c_order_cnt,
        SUM(
            CASE
                WHEN TOLOWER(funding_bucket) IN ('lazada om')
                AND TOLOWER(funding_type) IN ('om', 'ams')
                AND TOLOWER(sub_channel) IN ('cps affiliate')
                AND TOLOWER(group_segment) IN ('b2c') THEN gmv_usd
                ELSE 0
            END
        ) AS aff_b2c_gmv_usd
    FROM t_supper_trd_core_dwd
    GROUP BY ds,
        hh,
        seller_id
),
t_prd_ads AS (
    SELECT t1.ds AS ds,
        t1.hh AS hh,
        t1.product_id AS product_id,
        t3.product_name AS product_name,
        t3.product_url AS product_url,
        COALESCE(t4.current_price, 0) AS product_current_price,
        COALESCE(t4.stock_available, 0) AS stock_available,
        t3.seller_id AS seller_id,
        t3.seller_short_code AS seller_short_code,
        t3.seller_name AS seller_name,
        t6.shop_url AS seller_link,
        t5.new_seller_segment AS seller_segment_v3,
        t5.commercial_team AS seller_commercial_team,
        t5.pic_lead_name AS pic_lead_name,
        t3.business_unit_1 AS bu_lv1,
        t3.business_unit_2 AS bu_lv2,
        t3.industry_name AS cluster,
        t3.regional_category_1 AS cat_lv1,
        COALESCE(t2.ssc_commission, 0) AS ssc_commission,
        COALESCE(t2.aff_commission, 0) AS aff_commission,
        t1.buyer_cnt AS buyer_cnt,
        t1.order_cnt AS order_cnt,
        t1.gmv_usd AS gmv_usd,
        t1.aff_buyer_cnt AS aff_buyer_cnt,
        t1.aff_order_cnt AS aff_order_cnt,
        t1.aff_gmv_usd AS aff_gmv_usd,
        t1.aff_b2c_buyer_cnt AS aff_b2c_buyer_cnt,
        t1.aff_b2c_order_cnt AS aff_b2c_order_cnt,
        t1.aff_b2c_gmv_usd AS aff_b2c_gmv_usd,
        COALESCE(t4.l1d_order_cnt, 0) AS l1d_order_cnt,
        COALESCE(t4.l1d_gmv, 0) AS l1d_gmv,
        COALESCE(t4.l1d_cancelled_gmv, 0) AS l1d_cancelled_gmv,
        COALESCE(t4.l2d_order_cnt, 0) AS l2d_order_cnt,
        COALESCE(t4.l2d_gmv, 0) AS l2d_gmv,
        COALESCE(t4.l2d_cancelled_gmv, 0) AS l2d_cancelled_gmv,
        ROW_NUMBER() OVER (
            PARTITION BY t1.ds,
            t1.hh
            ORDER BY ROUND(t1.gmv_usd, 0) DESC,
                t1.product_id ASC
        ) AS gmv_usd_ranking,
        CASE
            WHEN t1.aff_gmv_usd > 0 THEN ROW_NUMBER() OVER (
                PARTITION BY t1.ds,
                t1.hh,
                t1.aff_gmv_usd > 0
                ORDER BY ROUND(t1.aff_gmv_usd, 0) DESC,
                    t1.product_id ASC
            )
            ELSE 0
        END AS aff_gmv_usd_ranking,
        CASE
            WHEN t1.aff_b2c_gmv_usd > 0 THEN ROW_NUMBER() OVER (
                PARTITION BY t1.ds,
                t1.hh,
                t1.aff_b2c_gmv_usd > 0
                ORDER BY ROUND(t1.aff_b2c_gmv_usd, 0) DESC,
                    t1.product_id ASC
            )
            ELSE 0
        END AS aff_b2c_gmv_usd_ranking,
        IF(t1.gmv_usd = 0, 0, t1.aff_gmv_usd / t1.gmv_usd) AS aff_gmv_usd_share,
        IF(
            t1.gmv_usd = 0,
            0,
            t1.aff_b2c_gmv_usd / t1.gmv_usd
        ) AS aff_b2c_gmv_usd_share
    FROM (
            SELECT ds,
                hh,
                product_id,
                SUM(SUM(buyer_cnt)) OVER (
                    PARTITION BY ds,
                    product_id
                    ORDER BY hh ASC
                ) AS buyer_cnt,
                SUM(SUM(order_cnt)) OVER (
                    PARTITION BY ds,
                    product_id
                    ORDER BY hh ASC
                ) AS order_cnt,
                SUM(SUM(gmv_usd)) OVER (
                    PARTITION BY ds,
                    product_id
                    ORDER BY hh ASC
                ) AS gmv_usd,
                SUM(SUM(aff_buyer_cnt)) OVER (
                    PARTITION BY ds,
                    product_id
                    ORDER BY hh ASC
                ) AS aff_buyer_cnt,
                SUM(SUM(aff_order_cnt)) OVER (
                    PARTITION BY ds,
                    product_id
                    ORDER BY hh ASC
                ) AS aff_order_cnt,
                SUM(SUM(aff_gmv_usd)) OVER (
                    PARTITION BY ds,
                    product_id
                    ORDER BY hh ASC
                ) AS aff_gmv_usd,
                SUM(SUM(aff_b2c_buyer_cnt)) OVER (
                    PARTITION BY ds,
                    product_id
                    ORDER BY hh ASC
                ) AS aff_b2c_buyer_cnt,
                SUM(SUM(aff_b2c_order_cnt)) OVER (
                    PARTITION BY ds,
                    product_id
                    ORDER BY hh ASC
                ) AS aff_b2c_order_cnt,
                SUM(SUM(aff_b2c_gmv_usd)) OVER (
                    PARTITION BY ds,
                    product_id
                    ORDER BY hh ASC
                ) AS aff_b2c_gmv_usd
            FROM (
                    SELECT ds,
                        hh,
                        product_id,
                        buyer_cnt,
                        order_cnt,
                        gmv_usd,
                        aff_buyer_cnt,
                        aff_order_cnt,
                        aff_gmv_usd,
                        aff_b2c_buyer_cnt,
                        aff_b2c_order_cnt,
                        aff_b2c_gmv_usd
                    FROM t_prd_dws
                    UNION ALL
                    SELECT t1.ds AS ds,
                        t2.hh AS hh,
                        t1.product_id AS product_id,
                        0 AS buyer_cnt,
                        0 AS order_cnt,
                        0 AS gmv_usd,
                        0 AS aff_buyer_cnt,
                        0 AS aff_order_cnt,
                        0 AS aff_gmv_usd,
                        0 AS aff_b2c_buyer_cnt,
                        0 AS aff_b2c_order_cnt,
                        0 AS aff_b2c_gmv_usd
                    FROM (
                            SELECT DISTINCT ds,
                                product_id,
                                1 AS map_key
                            FROM t_prd_dws
                        ) AS t1
                        INNER JOIN (
                            SELECT hh,
                                1 AS map_key
                            FROM lazada_analyst_dev.loutruong_dim_hh
                        ) AS t2 ON t1.map_key = t2.map_key LEFT ANTI
                        JOIN (
                            SELECT ds,
                                hh,
                                product_id
                            FROM t_prd_dws
                        ) AS t3 ON t1.ds = t3.ds
                        AND t2.hh = t3.hh
                        AND t1.product_id = t3.product_id
                )
            GROUP BY ds,
                hh,
                product_id
        ) AS t1
        LEFT JOIN (
            SELECT product_id,
                MIN(sku_ssc_commission) AS ssc_commission,
                MIN(sku_aff_commission) AS aff_commission
            FROM lazada_analyst_dev.loutruong_dim_slr_brd_sku_cms
            WHERE 1 = 1
                AND ds = MAX_PT(
                    'lazada_analyst_dev.loutruong_dim_slr_brd_sku_cms'
                )
            GROUP BY product_id
        ) AS t2 ON t1.product_id = t2.product_id
        LEFT JOIN (
            SELECT product_id,
                product_name,
                product_url,
                seller_id,
                seller_short_code,
                seller_name,
                business_unit_1,
                business_unit_2,
                industry_name,
                regional_category_1
            FROM lazada_cdm.dim_lzd_prd_product_extra_info
            WHERE 1 = 1
                AND ds = MAX_PT('lazada_cdm.dim_lzd_prd_product_extra_info')
                AND venture = 'VN'
        ) AS t3 ON t1.product_id = t3.product_id
        LEFT JOIN (
            SELECT updated_time AS updated_time_1,
                TO_CHAR(
                    TO_DATE(updated_time, 'yyyymmddhh'),
                    'yyyy-mm-dd hh'
                ) AS updated_time_2,
                TO_CHAR(TO_DATE(updated_time, 'yyyymmddhh'), 'yyyymmdd') AS ds,
                TO_CHAR(TO_DATE(updated_time, 'yyyymmddhh'), 'hh') AS hh,
                product_id,
                MAX(current_price) AS current_price,
                SUM(stock_available) AS stock_available,
                SUM(l1d_order_cnt) AS l1d_order_cnt,
                SUM(l1d_gmv) AS l1d_gmv,
                SUM(l1d_cancelled_gmv) AS l1d_cancelled_gmv,
                SUM(l2d_order_cnt) AS l2d_order_cnt,
                SUM(l2d_gmv) AS l2d_gmv,
                SUM(l2d_cancelled_gmv) AS l2d_cancelled_gmv
            FROM lazada_analyst_dev.datx_mega_intraday_sku_performance
            GROUP BY updated_time,
                TO_CHAR(
                    TO_DATE(updated_time, 'yyyymmddhh'),
                    'yyyy-mm-dd hh'
                ),
                TO_CHAR(TO_DATE(updated_time, 'yyyymmddhh'), 'yyyymmdd'),
                TO_CHAR(TO_DATE(updated_time, 'yyyymmddhh'), 'hh'),
                product_id
        ) AS t4 ON t1.ds = t4.ds
        AND t1.product_id = t4.product_id
        LEFT JOIN (
            SELECT ext_num_id AS seller_id,
                new_seller_segment,
                commercial_team,
                pic_lead_name
            FROM lazada_analyst_dev.vn_map_memid_slrid
            WHERE 1 = 1
                AND date_ = MAX_PT('lazada_analyst_dev.vn_map_memid_slrid')
        ) AS t5 ON t3.seller_id = t5.seller_id
        LEFT JOIN (
            SELECT seller_id,
                shop_url
            FROM lazada_cdm.dim_lzd_slr_seller_vn
            WHERE 1 = 1
                AND ds = MAX_PT('lazada_cdm.dim_lzd_slr_seller')
                AND venture = 'VN'
        ) AS t6 ON t3.seller_id = t6.seller_id
    WHERE 1 = 1
        AND (
            t1.ds = TO_CHAR(DATEADD(GETDATE(), 0, 'dd'), 'yyyymmdd')
            AND t1.hh <= (
                SELECT hh
                FROM t_intra_dim_hh
            )
        )
        OR (
            t1.ds BETWEEN TO_CHAR(DATEADD(GETDATE(), -2, 'dd'), 'yyyymmdd') AND TO_CHAR(DATEADD(GETDATE(), -1, 'dd'), 'yyyymmdd')
            AND t1.hh <= 23
        )
),
t_slr_ads AS (
    SELECT t1.ds AS ds,
        t1.hh AS hh,
        NULL AS product_id,
        NULL AS product_name,
        NULL AS product_url,
        COALESCE(t3.current_price, 0) AS current_price,
        COALESCE(t3.stock_available, 0) AS stock_available,
        CAST(t1.seller_id AS BIGINT) AS seller_id,
        t6.seller_short_code,
        t6.seller_name AS seller_name,
        t5.shop_url AS seller_url,
        t4.new_seller_segment AS new_seller_segment,
        t4.commercial_team AS commercial_team,
        t4.pic_lead_name AS pic_lead_name,
        t6.business_type AS bu_lv1,
        t6.business_type_level2 AS bu_lv2,
        t6.industry_name AS cluster,
        t6.regional_category1_name AS cat_lv1,
        COALESCE(t2.ssc_commission, 0) AS ssc_commission,
        COALESCE(t2.aff_commission, 0) AS aff_commission,
        t1.buyer_cnt AS buyer_cnt,
        t1.order_cnt AS order_cnt,
        t1.gmv_usd AS gmv_usd,
        t1.aff_buyer_cnt AS aff_buyer_cnt,
        t1.aff_order_cnt AS aff_order_cnt,
        t1.aff_gmv_usd AS aff_gmv_usd,
        t1.aff_b2c_buyer_cnt AS aff_b2c_buyer_cnt,
        t1.aff_b2c_order_cnt AS aff_b2c_order_cnt,
        t1.aff_b2c_gmv_usd AS aff_b2c_gmv_usd,
        COALESCE(t3.l1d_order_cnt, 0) AS l1d_order_cnt,
        COALESCE(t3.l1d_gmv, 0) AS l1d_gmv,
        COALESCE(t3.l1d_cancelled_gmv, 0) AS l1d_cancelled_gmv,
        COALESCE(t3.l2d_order_cnt, 0) AS l2d_order_cnt,
        COALESCE(t3.l2d_gmv, 0) AS l2d_gmv,
        COALESCE(t3.l2d_cancelled_gmv, 0) AS l2d_cancelled_gmv,
        ROW_NUMBER() OVER (
            PARTITION BY t1.ds,
            t1.hh
            ORDER BY ROUND(t1.gmv_usd, 0) DESC,
                t1.seller_id ASC
        ) AS gmv_usd_ranking,
        CASE
            WHEN t1.aff_gmv_usd > 0 THEN ROW_NUMBER() OVER (
                PARTITION BY t1.ds,
                t1.hh,
                t1.aff_gmv_usd > 0
                ORDER BY ROUND(t1.aff_gmv_usd, 0) DESC,
                    t1.seller_id ASC
            )
            ELSE 0
        END AS aff_gmv_usd_ranking,
        CASE
            WHEN t1.aff_b2c_gmv_usd > 0 THEN ROW_NUMBER() OVER (
                PARTITION BY t1.ds,
                t1.hh,
                t1.aff_b2c_gmv_usd > 0
                ORDER BY ROUND(t1.aff_b2c_gmv_usd, 0) DESC,
                    t1.seller_id ASC
            )
            ELSE 0
        END AS aff_b2c_gmv_usd_ranking,
        IF(t1.gmv_usd = 0, 0, t1.aff_gmv_usd / t1.gmv_usd) AS aff_gmv_usd_share,
        IF(
            t1.gmv_usd = 0,
            0,
            t1.aff_b2c_gmv_usd / t1.gmv_usd
        ) AS aff_b2c_gmv_usd_share
    FROM (
            SELECT ds,
                hh,
                seller_id,
                SUM(SUM(buyer_cnt)) OVER (
                    PARTITION BY ds,
                    seller_id
                    ORDER BY hh ASC
                ) AS buyer_cnt,
                SUM(SUM(order_cnt)) OVER (
                    PARTITION BY ds,
                    seller_id
                    ORDER BY hh ASC
                ) AS order_cnt,
                SUM(SUM(gmv_usd)) OVER (
                    PARTITION BY ds,
                    seller_id
                    ORDER BY hh ASC
                ) AS gmv_usd,
                SUM(SUM(aff_buyer_cnt)) OVER (
                    PARTITION BY ds,
                    seller_id
                    ORDER BY hh ASC
                ) AS aff_buyer_cnt,
                SUM(SUM(aff_order_cnt)) OVER (
                    PARTITION BY ds,
                    seller_id
                    ORDER BY hh ASC
                ) AS aff_order_cnt,
                SUM(SUM(aff_gmv_usd)) OVER (
                    PARTITION BY ds,
                    seller_id
                    ORDER BY hh ASC
                ) AS aff_gmv_usd,
                SUM(SUM(aff_b2c_buyer_cnt)) OVER (
                    PARTITION BY ds,
                    seller_id
                    ORDER BY hh ASC
                ) AS aff_b2c_buyer_cnt,
                SUM(SUM(aff_b2c_order_cnt)) OVER (
                    PARTITION BY ds,
                    seller_id
                    ORDER BY hh ASC
                ) AS aff_b2c_order_cnt,
                SUM(SUM(aff_b2c_gmv_usd)) OVER (
                    PARTITION BY ds,
                    seller_id
                    ORDER BY hh ASC
                ) AS aff_b2c_gmv_usd
            FROM (
                    SELECT ds,
                        hh,
                        seller_id,
                        buyer_cnt,
                        order_cnt,
                        gmv_usd,
                        aff_buyer_cnt,
                        aff_order_cnt,
                        aff_gmv_usd,
                        aff_b2c_buyer_cnt,
                        aff_b2c_order_cnt,
                        aff_b2c_gmv_usd
                    FROM t_slr_dws
                    UNION ALL
                    SELECT t1.ds AS ds,
                        t2.hh AS hh,
                        t1.seller_id AS seller_id,
                        0 AS buyer_cnt,
                        0 AS order_cnt,
                        0 AS gmv_usd,
                        0 AS aff_buyer_cnt,
                        0 AS aff_order_cnt,
                        0 AS aff_gmv_usd,
                        0 AS aff_b2c_buyer_cnt,
                        0 AS aff_b2c_order_cnt,
                        0 AS aff_b2c_gmv_usd
                    FROM (
                            SELECT DISTINCT ds,
                                seller_id,
                                1 AS map_key
                            FROM t_slr_dws
                        ) AS t1
                        INNER JOIN (
                            SELECT hh,
                                1 AS map_key
                            FROM lazada_analyst_dev.loutruong_dim_hh
                        ) AS t2 ON t1.map_key = t2.map_key LEFT ANTI
                        JOIN (
                            SELECT ds,
                                hh,
                                seller_id
                            FROM t_slr_dws
                        ) AS t3 ON t1.ds = t3.ds
                        AND t2.hh = t3.hh
                        AND t1.seller_id = t3.seller_id
                )
            GROUP BY ds,
                hh,
                seller_id
        ) AS t1
        LEFT JOIN (
            SELECT seller_id,
                MIN(seller_ssc_commission) AS ssc_commission,
                MIN(seller_aff_commission) AS aff_commission
            FROM lazada_analyst_dev.loutruong_dim_slr_brd_sku_cms
            WHERE 1 = 1
                AND ds = MAX_PT(
                    'lazada_analyst_dev.loutruong_dim_slr_brd_sku_cms'
                )
            GROUP BY seller_id
        ) AS t2 ON t1.seller_id = t2.seller_id
        LEFT JOIN (
            SELECT updated_time AS updated_time_1,
                TO_CHAR(
                    TO_DATE(updated_time, 'yyyymmddhh'),
                    'yyyy-mm-dd hh'
                ) AS updated_time_2,
                TO_CHAR(TO_DATE(updated_time, 'yyyymmddhh'), 'yyyymmdd') AS ds,
                TO_CHAR(TO_DATE(updated_time, 'yyyymmddhh'), 'hh') AS hh,
                seller_id,
                MAX(current_price) AS current_price,
                SUM(stock_available) AS stock_available,
                SUM(l1d_order_cnt) AS l1d_order_cnt,
                SUM(l1d_gmv) AS l1d_gmv,
                SUM(l1d_cancelled_gmv) AS l1d_cancelled_gmv,
                SUM(l2d_order_cnt) AS l2d_order_cnt,
                SUM(l2d_gmv) AS l2d_gmv,
                SUM(l2d_cancelled_gmv) AS l2d_cancelled_gmv
            FROM lazada_analyst_dev.datx_mega_intraday_sku_performance
            GROUP BY updated_time,
                TO_CHAR(
                    TO_DATE(updated_time, 'yyyymmddhh'),
                    'yyyy-mm-dd hh'
                ),
                TO_CHAR(TO_DATE(updated_time, 'yyyymmddhh'), 'yyyymmdd'),
                TO_CHAR(TO_DATE(updated_time, 'yyyymmddhh'), 'hh'),
                seller_id
        ) AS t3 ON t1.ds = t3.ds
        AND t1.seller_id = t3.seller_id
        LEFT JOIN (
            SELECT ext_num_id AS seller_id,
                new_seller_segment,
                commercial_team,
                pic_lead_name
            FROM lazada_analyst_dev.vn_map_memid_slrid
            WHERE 1 = 1
                AND date_ = MAX_PT('lazada_analyst_dev.vn_map_memid_slrid')
        ) AS t4 ON t1.seller_id = t4.seller_id
        LEFT JOIN (
            SELECT seller_id,
                shop_url
            FROM lazada_cdm.dim_lzd_slr_seller_vn
            WHERE 1 = 1
                AND ds = MAX_PT('lazada_cdm.dim_lzd_slr_seller')
                AND venture = 'VN'
        ) AS t5 ON t1.seller_id = t5.seller_id
        LEFT JOIN (
            SELECT seller_id,
                seller_short_code,
                seller_name,
                business_type,
                business_type_level2,
                industry_name,
                regional_category1_name
            FROM lazada_cdm.dim_lzd_slr_seller_extra_vn
            WHERE 1 = 1
                AND ds = MAX_PT('lazada_cdm.dim_lzd_slr_seller_extra')
        ) AS t6 ON t1.seller_id = t6.seller_id
    WHERE 1 = 1
        AND (
            t1.ds = TO_CHAR(DATEADD(GETDATE(), 0, 'dd'), 'yyyymmdd')
            AND t1.hh <= (
                SELECT hh
                FROM t_intra_dim_hh
            )
        )
        OR (
            t1.ds BETWEEN TO_CHAR(DATEADD(GETDATE(), -2, 'dd'), 'yyyymmdd') AND TO_CHAR(DATEADD(GETDATE(), -1, 'dd'), 'yyyymmdd')
            AND t1.hh <= 23
        )
) --
SELECT 'product' AS data_set_label,
    '500_product' AS data_ranking_label,
    *
FROM t_prd_ads
WHERE 1 = 1
    AND gmv_usd_ranking BETWEEN 1 AND 500
UNION ALL
SELECT 'product' AS data_set_label,
    '500_product_aff' AS data_ranking_label,
    *
FROM t_prd_ads
WHERE 1 = 1
    AND aff_gmv_usd_ranking BETWEEN 1 AND 500
UNION ALL
SELECT 'product' AS data_set_label,
    '500_product_aff_b2c' AS data_ranking_label,
    *
FROM t_prd_ads
WHERE 1 = 1
    AND aff_b2c_gmv_usd_ranking BETWEEN 1 AND 500
UNION ALL
SELECT 'seller' AS data_set_label,
    '500_seller' AS data_ranking_label,
    *
FROM t_slr_ads
WHERE 1 = 1
    AND gmv_usd_ranking BETWEEN 1 AND 500
UNION ALL
SELECT 'seller' AS data_set_label,
    '500_seller_aff' AS data_ranking_label,
    *
FROM t_slr_ads
WHERE 1 = 1
    AND aff_gmv_usd_ranking BETWEEN 1 AND 500
UNION ALL
SELECT 'seller' AS data_set_label,
    '500_seller_aff_b2c' AS data_ranking_label,
    *
FROM t_slr_ads
WHERE 1 = 1
    AND aff_b2c_gmv_usd_ranking BETWEEN 1 AND 500;
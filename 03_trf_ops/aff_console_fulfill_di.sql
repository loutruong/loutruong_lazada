-- MaxCompute SQL 
-- ********************************************************************--
-- author:Truong, Van Thanh
-- create time:2024-01-31 17:13:09
-- ********************************************************************--
--@@ Input = lazada_ads.ads_lzd_marketing_cps_conversion_report_mi
--@@ Input = lazada_ads.dim_lzd_mkt_affiliate_biz_type_df
--@@ Input = lazada_cdm.dwd_lzd_trd_core_fulfill_di
--@@ Input = lazada_cdm.dwd_lzd_trd_flash_sale_di
--@@ Input = lazada_cdm.dim_lzd_pro_flash_sale_sku
--@@ Input = lazada_cdm.dwd_lzd_trd_elp_ord_di
--@@ Input = lazada_cdm.dwd_lzd_pro_promotion_item_di
--@@ Input = lazada_cdm.dim_lzd_pro_collectibles
--@@ Input = lazada_analyst_dev.loutruong_g_seg_uid_1d
--@@ Input = lazada_analyst_dev.loutruong_m_tag_uid_1d
--@@ Input = lazada_analyst_dev.hg_aff_plm_mixed
--@@ Output = lazada_analyst_dev.tmp_loutruong_aff_console_fulfill_di
--@@ Output = lazada_analyst_dev.loutruong_aff_console_fulfill_di
--@@ Output = lazada_analyst_dev.loutruong_aff_console_fulfill_df
----------------------------------------------------------------------------------------------------------------------------
-- Build up raw at the lowest level of partner is pid and transaction is sales_order_item_id
----------------------------------------------------------------------------------------------------------------------------
-- DROP TABLE IF EXISTS lazada_analyst_dev.tmp_loutruong_aff_console_fulfill_di
-- ;
-- CREATE TABLE IF NOT EXISTS lazada_analyst_dev.tmp_loutruong_aff_console_fulfill_di
-- (
--     sales_order_item_id           STRING
--     ,order_id                     STRING
--     ,order_number                 STRING
--     ,mord_id                      STRING
--     ,check_out_id                 STRING
--     ,platform                     STRING COMMENT 'app / web'
--     ,status                       STRING
--     ,adjust_type                  STRING
--     ,is_fraud                     BIGINT COMMENT '1=Yes, 0=No'
--     ,is_sa                        BIGINT COMMENT '1=Yes, 0=No'
--     ,is_flashsale                 BIGINT COMMENT '1=Yes, 0=No'
--     ,is_every_day_lowprice        BIGINT COMMENT '1=Yes, 0=No'
--     ,is_coin                      BIGINT COMMENT '1=Yes, 0=No'
--     ,is_freeship_max              BIGINT COMMENT '1=Yes, 0=No'
--     ,is_lazada_purchase_incentive BIGINT COMMENT '1=Yes, 0=No'
--     ,is_choice                    BIGINT COMMENT '1=Yes, 0=No'
--     ,seller_id                    STRING
--     ,seller_name                  STRING
--     ,offer_id                     STRING COMMENT 'seller sa / affiliate console offer id'
--     ,product_id                   STRING
--     ,product_name                 STRING
--     ,sku_id                       STRING
--     ,lazada_sku                   STRING COMMENT 'product_id_VNAMZ-sku_id'
--     ,buyer_id                     STRING
--     ,is_new_buyer                 BIGINT COMMENT '1=Yes, 0=No'
--     ,g_segment_original           STRING COMMENT 'fulfillment_create_date logic mapping avaliable data from 20220701'
--     ,m_tag_original               STRING COMMENT 'fulfillment_create_date logic mapping avaliable data from 20220701'
--     ,profitable_value_original    DOUBLE COMMENT 'fulfillment_create_date logic mapping avaliable data from 20220701'
--     ,g_segment_fixed              STRING COMMENT 'set null value to G4.1 - Aligned with CLM team'
--     ,g_segment_fixed_level_1      STRING COMMENT 'Reacquired buyer / Retained buyer / Other buyer'
--     ,g_segment_fixed_level_2      STRING COMMENT 'G1 / NB G2 / RB G2 / G3 / G4 / G5 / G6 / G7 / G8'
--     ,m_tag_fixed                  STRING COMMENT 'set null value to Undefined - Aligned with CLM team'
--     ,profitable_value_fixed       STRING COMMENT 'set null value to 0 - Aligned with CLM team'
--     ,user_segment                 STRING COMMENT 'Profitable buyer / Non-profitable buyer / Other buyer'
--     ,cluster                      STRING
--     ,cat_lv1                      STRING
--     ,bu_lv1                       STRING
--     ,bu_lv2                       STRING
--     ,delivery_location            STRING
--     ,delivery_company             STRING COMMENT 'seller_own_fleet/lex_vn/etc'
--     ,payment_method               STRING
--     ,exchange_rate                DOUBLE
--     ,gmv_vnd                      DOUBLE COMMENT 'Platform GMV logic, match exactly across all dashboard internal'
--     ,gmv_usd                      DOUBLE COMMENT 'Platform GMV logic, match exactly across all dashboard internal'
--     ,order_amt_vnd                DOUBLE COMMENT 'Partner GMV logic, match exactly across partner adsense, affiliate console'
--     ,order_amt_usd                DOUBLE COMMENT 'Partner GMV logic, match exactly across partner adsense, affiliate console'
--     ,group_segment_1              STRING COMMENT 'pid logic, 1 member_id can has multiple group_segment'
--     ,segment_1                    STRING COMMENT 'pid logic, 1 member_id can has multiple group_segment'
--     ,pid                          STRING COMMENT 'partner identify mm_memberid_siteid_adzoneid'
--     ,member_id                    STRING
--     ,site_id                      STRING
--     ,adzone_id                    STRING
--     ,sub_aff_id                   STRING
--     ,sub_id1                      STRING
--     ,sub_id2                      STRING
--     ,sub_id3                      STRING
--     ,sub_id4                      STRING
--     ,sub_id5                      STRING
--     ,sub_id6                      STRING
--     ,total_commission_rate        DOUBLE COMMENT 'platform_commission_rate + brand_commission_rate'
--     ,platform_commission_rate     DOUBLE
--     ,brand_commission_rate        DOUBLE
--     ,total_payout_vnd             DOUBLE COMMENT 'platform_payout_vnd + brand_payout_vnd'
--     ,platform_payout_vnd          DOUBLE
--     ,brand_payout_vnd             DOUBLE
--     ,total_payout_usd             DOUBLE COMMENT 'platform_payout_usd + brand_payout_usd'
--     ,platform_payout_usd          DOUBLE
--     ,brand_payout_usd             DOUBLE
--     ,hh                           BIGINT COMMENT 'fulfillment_create_hh from 00 to 23'
-- )
-- COMMENT 'Adsense / Console data view. Attr_model = ltpdp_settlement_voucher_p - Last touch 7D for app / 30D for web and paid prio'
-- PARTITIONED BY 
-- (
--     ds                            STRING COMMENT 'fulfillment_create_date'
-- )
-- LIFECYCLE 3600
-- ;
INSERT OVERWRITE TABLE lazada_analyst_dev.tmp_loutruong_aff_console_fulfill_di PARTITION (ds)
SELECT t1.sales_order_item_id AS sales_order_item_id,
    t2.order_id AS order_id,
    t2.sales_order_id AS order_number,
    t2.mord_id AS mord_id,
    t2.check_out_id AS check_out_id,
CASE
        WHEN TOLOWER(t1.platform) IN ('app') THEN 'app'
        ELSE 'web'
    END AS platform,
    t1.status AS status,
    t1.adjust_type AS adjust_type,
    COALESCE(t1.is_fraud, 0) AS is_fraud,
CASE
        WHEN COALESCE(t1.brand_commission_fee, 0) > 0 THEN 1
        ELSE 0
    END AS is_sa,
CASE
        WHEN t6.sales_order_item_id IS NOT NULL
        AND t7.sku_id IS NOT NULL THEN 1
        ELSE 0
    END AS is_flashsale,
CASE
        WHEN t8.sales_order_item_id IS NOT NULL THEN 1
        ELSE 0
    END AS is_every_day_lowprice,
CASE
        WHEN COALESCE(KEYVALUE(t2.extra_info, "flexiCoin_seller"), 0) + COALESCE(
            KEYVALUE(t2.extra_info, 'coinsSubsidy_platform'),
            0
        ) + COALESCE(KEYVALUE(t2.extra_info, 'flexiCoin_platform'), 0) > 0 THEN 1
        ELSE 0
    END AS is_coin,
CASE
        WHEN t9.sales_order_item_id IS NOT NULL
        AND t10.promotion_id IS NOT NULL THEN 1
        ELSE 0
    END AS is_freeship_max,
CASE
        WHEN COALESCE(KEYVALUE(t2.extra_info, 'lpiCoupon_platform'), 0) + COALESCE(KEYVALUE(t2.extra_info, 'lpiCoupon_seller'), 0) + COALESCE(
            KEYVALUE(t2.extra_info, 'purchaseIncentive_platform'),
            0
        ) + COALESCE(
            KEYVALUE(t2.extra_info, 'purchaseIncentive_seller'),
            0
        ) > 0 THEN 1
        ELSE 0
    END AS is_lazada_purchase_incentive,
CASE
        WHEN TOLOWER(t2.business_type_level2) LIKE '%choice%' THEN 1
        ELSE 0
    END AS is_choice,
    t1.seller_id AS seller_id,
    t1.seller_name AS seller_name,
    t1.offer_id AS offer_id,
    t1.product_id AS product_id,
    t1.product_name AS product_name,
    t1.sku_id AS sku_id,
    t1.sku AS lazada_sku,
    t2.buyer_id AS buyer_id,
    COALESCE(t1.new_user, 0) is_new_buyer,
    t4.g_level AS g_segment_original,
    t5.monetary_tag AS m_tag_original,
    t5.pc275 AS profitable_value_original,
CASE
        WHEN t1.fulfillment_create_date < '20220701' THEN 'data_unavailable'
        WHEN t1.fulfillment_create_date >= '20220701' THEN COALESCE(t4.g_level, 'G4.1')
    END AS g_segment_fixed,
CASE
        WHEN t1.fulfillment_create_date < '20220701' THEN 'data_unavailable'
        WHEN t1.fulfillment_create_date >= '20220701'
        AND COALESCE(t4.g_level, 'G4.1') IN ('G2.3', 'G3') THEN 'Reacquired buyer'
        WHEN t1.fulfillment_create_date >= '20220701'
        AND COALESCE(t4.g_level, 'G4.1') IN ('G6.1', 'G6.2', 'G7', 'G8') THEN 'Retained buyer'
        ELSE 'Other buyer'
    END AS g_segment_fixed_level_1,
CASE
        WHEN t1.fulfillment_create_date < '20220701' THEN 'data_unavailable'
        WHEN t1.fulfillment_create_date >= '20220701'
        AND COALESCE(t4.g_level, 'G4.1') LIKE 'G1%' THEN 'G1'
        WHEN t1.fulfillment_create_date >= '20220701'
        AND COALESCE(t4.g_level, 'G4.1') IN ('G2.1', 'G2.2') THEN 'NB G2'
        WHEN t1.fulfillment_create_date >= '20220701'
        AND COALESCE(t4.g_level, 'G4.1') IN ('G2.3') THEN 'RB G2'
        WHEN t1.fulfillment_create_date >= '20220701'
        AND COALESCE(t4.g_level, 'G4.1') LIKE 'G3%' THEN 'G3'
        WHEN t1.fulfillment_create_date >= '20220701'
        AND COALESCE(t4.g_level, 'G4.1') LIKE 'G4%' THEN 'G4'
        WHEN t1.fulfillment_create_date >= '20220701'
        AND COALESCE(t4.g_level, 'G4.1') LIKE 'G5%' THEN 'G5'
        WHEN t1.fulfillment_create_date >= '20220701'
        AND COALESCE(t4.g_level, 'G4.1') LIKE 'G6%' THEN 'G6'
        WHEN t1.fulfillment_create_date >= '20220701'
        AND COALESCE(t4.g_level, 'G4.1') LIKE 'G7%' THEN 'G7'
        WHEN t1.fulfillment_create_date >= '20220701'
        AND COALESCE(t4.g_level, 'G4.1') LIKE 'G8%' THEN 'G8'
        ELSE 'G4'
    END AS g_segment_fixed_level_2,
CASE
        WHEN t1.fulfillment_create_date < '20220701' THEN 'data_unavailable'
        WHEN t1.fulfillment_create_date >= '20220701' THEN COALESCE(t5.monetary_tag, 'Undefined')
    END AS m_tag_fixed,
CASE
        WHEN t1.fulfillment_create_date < '20220701' THEN 'data_unavailable'
        WHEN t1.fulfillment_create_date >= '20220701' THEN COALESCE(t5.pc275, 0)
    END AS profitable_value_fixed,
CASE
        WHEN t1.fulfillment_create_date < '20220701' THEN 'data_unavailable'
        WHEN t1.fulfillment_create_date >= '20220701'
        AND COALESCE(t4.g_level, 'G4.1') IN ('G6.1', 'G6.2', 'G7', 'G8')
        AND COALESCE(t5.monetary_tag, 'Undefined') IN ('Undefined', 'A', 'B') THEN 'Profitable buyer'
        WHEN t1.fulfillment_create_date >= '20220701'
        AND COALESCE(t4.g_level, 'G4.1') IN ('G6.1', 'G6.2', 'G7', 'G8')
        AND COALESCE(t5.monetary_tag, 'Undefined') IN ('C', 'D') THEN 'Non-profitable buyer'
        WHEN t1.fulfillment_create_date >= '20231022'
        AND COALESCE(t4.g_level, 'G4.1') IN ('G2.3', 'G3')
        AND COALESCE(t5.monetary_tag, 'Undefined') IN ('Undefined', 'A', 'B') THEN 'Profitable buyer'
        WHEN t1.fulfillment_create_date >= '20231022'
        AND COALESCE(t4.g_level, 'G4.1') IN ('G2.3', 'G3')
        AND COALESCE(t5.monetary_tag, 'Undefined') IN ('C', 'D') THEN 'Non-profitable buyer'
        ELSE 'Other buyer'
    END AS user_segment_fixed,
    COALESCE(t2.industry_name, 'Unknown_industry_name') AS cluster,
    COALESCE(t2.regional_category1_name, 'Uncategorized') AS cat_lv1,
    COALESCE(t2.business_type, 'Unknown_bu_lv1') AS bu_lv1,
    COALESCE(t2.business_type_level2, 'Unknown_bu_lv2') AS bu_lv2,
    COALESCE(
        t2.shipping_region1_name,
        'Unknown_delivery_location'
    ) AS delivery_location,
    COALESCE(t2.delivery_company, 'Unknown_delivery_company') AS delivery_company,
    COALESCE(t2.payment_method, 'Unknown_payment_method') AS payment_method,
    COALESCE(t2.exchange_rate, 0) AS exchange_rate,
    COALESCE(t2.actual_gmv, 0) AS gmv_vnd,
    COALESCE(t2.actual_gmv, 0) * COALESCE(t2.exchange_rate, 0) AS gmv_usd,
    COALESCE(t1.order_amt, 0) AS order_amt_vnd,
    COALESCE(t1.order_amt, 0) * COALESCE(t2.exchange_rate, 0) AS order_amt_usd,
    COALESCE(t3.biz_type, 'Unknown_group_segment_1') AS group_segment_1,
    COALESCE(t1.segmentation, 'Unknown_segment_1') AS segment_1,
    t1.pid AS pid,
    t1.member_id AS member_id,
    t1.site_id AS site_id,
    t1.adzone_id AS adzone_id,
    t1.affiliate_sub_id AS sub_aff_id,
    t1.sub_id1 AS sub_id1,
    t1.sub_id2 AS sub_id2,
    t1.sub_id3 AS sub_id3,
    t1.sub_id4 AS sub_id4,
    t1.sub_id5 AS sub_id5,
    t1.sub_id6 AS sub_id6,
    COALESCE(t1.total_commission_rate, 0) AS total_commission_rate,
    COALESCE(t1.platform_commission_rate, 0) AS platform_commission_rate,
    COALESCE(t1.brand_commission_rate, 0) AS brand_commission_rate,
    COALESCE(t1.total_commission_fee, 0) AS total_payout_vnd,
    COALESCE(t1.platform_commission_fee, 0) AS platform_payout_vnd,
    COALESCE(t1.brand_commission_fee, 0) AS brand_payout_vnd,
    COALESCE(t1.total_commission_fee, 0) * COALESCE(t2.exchange_rate, 0) AS total_payout_usd,
    COALESCE(t1.platform_commission_fee, 0) * COALESCE(t2.exchange_rate, 0) AS platform_payout_usd,
    COALESCE(t1.brand_commission_fee, 0) * COALESCE(t2.exchange_rate, 0) AS brand_payout_usd,
    t1.fulfillment_create_hh AS hh,
    t1.fulfillment_create_date AS ds
FROM (
        SELECT TO_CHAR(
                TO_DATE(conversion_time, 'yyyy-mm-ddTHH:mi'),
                'yyyymmdd'
            ) AS order_create_date,
            TO_CHAR(TO_DATE(conversion_time, 'yyyy-mm-ddTHH:mi'), 'hh') AS order_create_hh,
            TO_CHAR(
                TO_DATE(fulfilled_time, 'yyyy-mm-ddTHH:mi'),
                'yyyymmdd'
            ) AS fulfillment_create_date,
            TO_CHAR(TO_DATE(fulfilled_time, 'yyyy-mm-ddTHH:mi'), 'hh') AS fulfillment_create_hh,
            segmentation,
            pid,
            member_id,
            site_id,
            adzone_id,
            platform,
            affiliate_sub_id,
            sub_id1,
            sub_id2,
            sub_id3,
            sub_id4,
            sub_id5,
            sub_id6,
            sub_order_id AS sales_order_item_id,
            status,
            adjust_type,
            is_fraud,
            new_user,
            seller_id,
            seller_name,
            offer_id,
            product_id,
            product_name,
            sku_id,
            sku,
            order_amt,
            commission_rate AS total_commission_rate,
            platform_commission_rate,
            brand_commission_rate,
            est_payout AS total_commission_fee,
            platform_commission_fee,
            brand_commission_fee
        FROM lazada_ads.ads_lzd_marketing_cps_conversion_report_mi
        WHERE 1 = 1 --
            -- AND     mm >= '202112' --<< Start
            AND mm >= TO_CHAR(
                DATEADD(TO_DATE('${bizdate}', 'yyyymmdd'), -4, 'mm'),
                'yyyymm'
            ) --<< Daily
            AND venture = 'VN' --
            -- AND     TO_CHAR(TO_DATE(fulfilled_time,'yyyy-mm-ddTHH:mi'),'yyyymmdd') >= '20220101' --<< Start
            AND TO_CHAR(
                TO_DATE(fulfilled_time, 'yyyy-mm-ddTHH:mi'),
                'yyyymmdd'
            ) >= TO_CHAR(
                DATEADD(TO_DATE('${bizdate}', 'yyyymmdd'), -100, 'dd'),
                'yyyymmdd'
            ) --<< Daily
            AND source IS NOT NULL
    ) AS t1
    LEFT JOIN (
        SELECT TO_CHAR(order_create_date, 'yyyymmdd') AS order_create_date,
            TO_CHAR(order_create_date, 'hh') AS order_create_hh,
            TO_CHAR(fulfillment_create_date, 'yyyymmdd') AS fulfillment_create_date,
            TO_CHAR(fulfillment_create_date, 'hh') AS fulfillment_create_hh,
            extra_info,
            check_out_id,
            mord_id,
            sales_order_id,
            order_id,
            sales_order_item_id,
            shipping_region1_name,
            delivery_company,
            industry_name,
            regional_category1_name,
            business_type,
            business_type_level2,
            payment_method,
            buyer_id,
            exchange_rate,
            actual_gmv
        FROM lazada_cdm.dwd_lzd_trd_core_fulfill_di
        WHERE 1 = 1 --
            -- AND     ds >= '20211220' --<< Start
            AND ds >= TO_CHAR(
                DATEADD(TO_DATE('${bizdate}', 'yyyymmdd'), -110, 'dd'),
                'yyyymmdd'
            ) --<< Daily
            AND venture = 'VN'
    ) AS t2 ON t1.sales_order_item_id = t2.sales_order_item_id
    LEFT JOIN (
        SELECT biz_type,
            segmentation
        FROM lazada_ads.dim_lzd_mkt_affiliate_biz_type_df
    ) AS t3 ON t1.segmentation = t3.segmentation
    LEFT JOIN (
        SELECT ds,
            user_id,
            g_level
        FROM lazada_analyst_dev.loutruong_g_seg_uid_1d
        WHERE 1 = 1 --
            -- AND     ds >= '20211220' --<< Start
            AND ds >= TO_CHAR(
                DATEADD(TO_DATE('${bizdate}', 'yyyymmdd'), -110, 'dd'),
                'yyyymmdd'
            ) --<< Daily
        GROUP BY ds,
            user_id,
            g_level
    ) AS t4 ON t2.fulfillment_create_date = t4.ds
    AND t2.buyer_id = t4.user_id
    LEFT JOIN (
        SELECT ds,
            user_id,
            monetary_tag,
            pc275
        FROM lazada_analyst_dev.loutruong_m_tag_uid_1d
        WHERE 1 = 1 --
            -- AND     ds >= '20211220' --<< Start
            AND ds >= TO_CHAR(
                DATEADD(TO_DATE('${bizdate}', 'yyyymmdd'), -110, 'dd'),
                'yyyymmdd'
            ) --<< Daily
        GROUP BY ds,
            user_id,
            monetary_tag,
            pc275
    ) AS t5 ON t2.fulfillment_create_date = t5.ds
    AND t2.buyer_id = t5.user_id
    LEFT JOIN (
        SELECT ds,
            sales_order_item_id,
            sku_id
        FROM lazada_cdm.dwd_lzd_trd_flash_sale_di
        WHERE 1 = 1 --
            -- AND     ds >= '20211220' --<< Start
            AND ds >= TO_CHAR(
                DATEADD(TO_DATE('${bizdate}', 'yyyymmdd'), -110, 'dd'),
                'yyyymmdd'
            ) --<< Daily
            AND venture = 'VN'
            AND is_flash_sale = 1
        GROUP BY ds,
            sales_order_item_id,
            sku_id
    ) AS t6 ON t1.sales_order_item_id = t6.sales_order_item_id
    LEFT JOIN (
        SELECT ds,
            sku_id
        FROM lazada_cdm.dim_lzd_pro_flash_sale_sku
        WHERE 1 = 1 --
            -- AND     ds >= '20211220' --<< Start
            AND ds >= TO_CHAR(
                DATEADD(TO_DATE('${bizdate}', 'yyyymmdd'), -110, 'dd'),
                'yyyymmdd'
            ) --<< Daily
            AND ds BETWEEN TO_CHAR(fs_start_time, 'yyyymmdd') AND TO_CHAR(fs_end_time, 'yyyymmdd')
            AND CONCAT(ds, ' 00') != TO_CHAR(fs_end_time, 'yyyymmdd hh')
            AND venture = 'VN'
            AND campaign_sku_status = 2
        GROUP BY ds,
            sku_id
    ) AS t7 ON t6.ds = t7.ds
    AND t6.sku_id = t7.sku_id
    LEFT JOIN (
        SELECT sales_order_item_id
        FROM lazada_cdm.dwd_lzd_trd_elp_ord_di
        WHERE 1 = 1 --
            -- AND     ds >= '20211220' --<< Start
            AND ds >= TO_CHAR(
                DATEADD(TO_DATE('${bizdate}', 'yyyymmdd'), -110, 'dd'),
                'yyyymmdd'
            ) --<< Daily
            AND venture = 'VN'
            AND is_revenue = 1
        GROUP BY sales_order_item_id
    ) AS t8 ON t1.sales_order_item_id = t8.sales_order_item_id
    LEFT JOIN (
        SELECT sales_order_item_id,
            promotion_id
        FROM lazada_cdm.dwd_lzd_pro_promotion_item_di
        WHERE 1 = 1 --
            -- AND     ds >= '20211220' --<< Start
            AND ds >= TO_CHAR(
                DATEADD(TO_DATE('${bizdate}', 'yyyymmdd'), -110, 'dd'),
                'yyyymmdd'
            ) --<< Daily
            AND venture = 'VN'
            AND promotion_type IN ('shippingFeeCoupon')
            AND TOLOWER(promotion_role) IN ('platform')
        GROUP BY sales_order_item_id,
            promotion_id
    ) AS t9 ON t1.sales_order_item_id = t9.sales_order_item_id
    LEFT JOIN (
        SELECT promotion_id
        FROM lazada_cdm.dim_lzd_pro_collectibles
        WHERE 1 = 1
            AND ds = MAX_PT('lazada_cdm.dim_lzd_pro_collectibles')
            AND venture = 'VN'
            AND (
                TOLOWER(voucher_name) LIKE '%platform_clm_seller boost program%'
                OR TOLOWER(description) LIKE '%platform_clm_seller boost program%'
            )
        GROUP BY promotion_id
    ) AS t10 ON t9.promotion_id = t10.promotion_id;
----------------------------------------------------------------------------------------------------------------------------
-- Map latest tag to each of member_id
----------------------------------------------------------------------------------------------------------------------------
-- DROP TABLE IF EXISTS lazada_analyst_dev.loutruong_aff_console_fulfill_di
-- ;
-- CREATE TABLE IF NOT EXISTS lazada_analyst_dev.loutruong_aff_console_fulfill_di
-- (
--     sales_order_item_id           STRING
--     ,order_id                     STRING
--     ,order_number                 STRING
--     ,mord_id                      STRING
--     ,check_out_id                 STRING
--     ,platform                     STRING COMMENT 'app / web'
--     ,status                       STRING
--     ,adjust_type                  STRING
--     ,is_fraud                     BIGINT COMMENT '1=Yes, 0=No'
--     ,is_sa                        BIGINT COMMENT '1=Yes, 0=No'
--     ,is_flashsale                 BIGINT COMMENT '1=Yes, 0=No'
--     ,is_every_day_lowprice        BIGINT COMMENT '1=Yes, 0=No'
--     ,is_coin                      BIGINT COMMENT '1=Yes, 0=No'
--     ,is_freeship_max              BIGINT COMMENT '1=Yes, 0=No'
--     ,is_lazada_purchase_incentive BIGINT COMMENT '1=Yes, 0=No'
--     ,is_choice                    BIGINT COMMENT '1=Yes, 0=No'
--     ,seller_id                    STRING
--     ,seller_name                  STRING
--     ,offer_id                     STRING COMMENT 'seller sa / affiliate console offer id'
--     ,product_id                   STRING
--     ,product_name                 STRING
--     ,sku_id                       STRING
--     ,lazada_sku                   STRING COMMENT 'product_id_VNAMZ-sku_id'
--     ,buyer_id                     STRING
--     ,is_new_buyer                 BIGINT COMMENT '1=Yes, 0=No'
--     ,g_segment_original           STRING COMMENT 'fulfillment_create_date logic mapping avaliable data from 20220701'
--     ,m_tag_original               STRING COMMENT 'fulfillment_create_date logic mapping avaliable data from 20220701'
--     ,profitable_value_original    DOUBLE COMMENT 'fulfillment_create_date logic mapping avaliable data from 20220701'
--     ,g_segment_fixed              STRING COMMENT 'set null value to G4.1 - Aligned with CLM team'
--     ,g_segment_fixed_level_1      STRING COMMENT 'Reacquired buyer / Retained buyer / Other buyer'
--     ,g_segment_fixed_level_2      STRING COMMENT 'G1 / NB G2 / RB G2 / G3 / G4 / G5 / G6 / G7 / G8'
--     ,m_tag_fixed                  STRING COMMENT 'set null value to Undefined - Aligned with CLM team'
--     ,profitable_value_fixed       STRING COMMENT 'set null value to 0 - Aligned with CLM team'
--     ,user_segment                 STRING COMMENT 'Profitable buyer / Non-profitable buyer / Other buyer'
--     ,cluster                      STRING
--     ,cat_lv1                      STRING
--     ,bu_lv1                       STRING
--     ,bu_lv2                       STRING
--     ,delivery_location            STRING
--     ,delivery_company             STRING COMMENT 'seller_own_fleet/lex_vn/etc'
--     ,payment_method               STRING
--     ,exchange_rate                DOUBLE
--     ,gmv_vnd                      DOUBLE COMMENT 'Platform GMV logic, match exactly across all dashboard internal'
--     ,gmv_usd                      DOUBLE COMMENT 'Platform GMV logic, match exactly across all dashboard internal'
--     ,order_amt_vnd                DOUBLE COMMENT 'Partner GMV logic, match exactly across partner adsense, affiliate console'
--     ,order_amt_usd                DOUBLE COMMENT 'Partner GMV logic, match exactly across partner adsense, affiliate console'
--     ,group_segment_1              STRING COMMENT 'pid logic, 1 member_id can has multiple group_segment'
--     ,segment_1                    STRING COMMENT 'pid logic, 1 member_id can has multiple group_segment'
--     ,group_segment_2              STRING COMMENT 'lastest logic, 1 member_id = 1 group_segment, src = lazada_analyst_dev.hg_aff_plm_mixed'
--     ,team_ops                     STRING COMMENT 'lastest logic, 1 member_id = 1 team_ops, src = lazada_analyst_dev.hg_aff_plm_mixed'
--     ,plm_l30d_lv1                 STRING COMMENT 'lastest logic, 1 member_id = 1 plm_l30d_lv1, src = lazada_analyst_dev.hg_aff_plm_mixed'
--     ,plm_l30d_lv2                 STRING COMMENT 'lastest logic, 1 member_id = 1 plm_l30d_lv2, src = lazada_analyst_dev.hg_aff_plm_mixed'
--     ,segment_2                    STRING COMMENT 'lastest logic, 1 member_id = 1 segment, src = lazada_analyst_dev.hg_aff_plm_mixed'
--     ,register_ds                  STRING COMMENT 'lastest logic, 1 member_id = 1 register_ds, src = lazada_analyst_dev.hg_aff_plm_mixed'
--     ,pid                          STRING
--     ,member_id                    STRING COMMENT 'partner identify mm_memberid_siteid_adzoneid'
--     ,member_name                  STRING COMMENT 'lastest logic, 1 member_id = 1 name'
--     ,site_id                      STRING
--     ,adzone_id                    STRING
--     ,sub_aff_id                   STRING
--     ,sub_id1                      STRING
--     ,sub_id2                      STRING
--     ,sub_id3                      STRING
--     ,sub_id4                      STRING
--     ,sub_id5                      STRING
--     ,sub_id6                      STRING
--     ,total_commission_rate        DOUBLE COMMENT 'platform_commission_rate + brand_commission_rate'
--     ,platform_commission_rate     DOUBLE
--     ,brand_commission_rate        DOUBLE
--     ,total_payout_vnd             DOUBLE COMMENT 'platform_payout_vnd + brand_payout_vnd'
--     ,platform_payout_vnd          DOUBLE
--     ,brand_payout_vnd             DOUBLE
--     ,total_payout_usd             DOUBLE COMMENT 'platform_payout_usd + brand_payout_usd'
--     ,platform_payout_usd          DOUBLE
--     ,brand_payout_usd             DOUBLE
--     ,hh                           BIGINT COMMENT 'fulfillment_create_hh from 00 to 23'
-- )
-- COMMENT 'Adsense / Console data view. Attr_model = ltpdp_settlement_voucher_p - Last touch 7D for app / 30D for web and paid prio'
-- PARTITIONED BY 
-- (
--     ds                            STRING COMMENT 'fulfillment_create_date'
-- )
-- LIFECYCLE 3600
-- ;
INSERT OVERWRITE TABLE lazada_analyst_dev.loutruong_aff_console_fulfill_di PARTITION (ds)
SELECT t1.sales_order_item_id AS sales_order_item_id,
    t1.order_id AS order_id,
    t1.order_number AS order_number,
    t1.mord_id AS mord_id,
    t1.check_out_id AS check_out_id,
    t1.platform AS platform,
    t1.status AS status,
    t1.adjust_type AS adjust_type,
    t1.is_fraud AS is_fraud,
    t1.is_sa AS is_sa,
    t1.is_flashsale AS is_flashsale,
    t1.is_every_day_lowprice AS is_every_day_lowprice,
    t1.is_coin AS is_coin,
    t1.is_freeship_max AS is_freeship_max,
    t1.is_lazada_purchase_incentive AS is_lazada_purchase_incentive,
    t1.is_choice AS is_choice,
    t1.seller_id AS seller_id,
    t1.seller_name AS seller_name,
    t1.offer_id AS offer_id,
    t1.product_id AS product_id,
    t1.product_name AS product_name,
    t1.sku_id AS sku_id,
    t1.lazada_sku AS lazada_sku,
    t1.buyer_id AS buyer_id,
    t1.is_new_buyer AS is_new_buyer,
    t1.g_segment_original AS g_segment_original,
    t1.m_tag_original AS m_tag_original,
    t1.profitable_value_original AS profitable_value_original,
    t1.g_segment_fixed AS g_segment_fixed,
    t1.g_segment_fixed_level_1 AS g_segment_fixed_level_1,
    t1.g_segment_fixed_level_2 AS g_segment_fixed_level_2,
    t1.m_tag_fixed AS m_tag_fixed,
    t1.profitable_value_fixed AS profitable_value_fixed,
    t1.user_segment AS user_segment,
    t1.cluster AS cluster,
    t1.cat_lv1 AS cat_lv1,
    t1.bu_lv1 AS bu_lv1,
    t1.bu_lv2 AS bu_lv2,
    t1.delivery_location AS delivery_location,
    t1.delivery_company AS delivery_company,
    t1.payment_method AS payment_method,
    t1.exchange_rate AS exchange_rate,
    t1.gmv_vnd AS gmv_vnd,
    t1.gmv_usd AS gmv_usd,
    t1.order_amt_vnd AS order_amt_vnd,
    t1.order_amt_usd AS order_amt_usd,
    t1.group_segment_1 AS group_segment_1,
    t1.segment_1 AS segment_1,
    COALESCE(t2.group_segment, 'Unknow_group_segment_2') AS group_segment_2,
    COALESCE(t2.team_ops, 'Unknow_group_segment_2') AS team_ops,
    COALESCE(t2.plm_l30d_lv1, 'Unknow_group_segment_2') AS plm_l30d_lv1,
    COALESCE(t2.plm_l30d_lv2, 'Unknow_group_segment_2') AS plm_l30d_lv2,
    COALESCE(t2.segment, 'Unknow_segment_2') AS segment_2,
    t2.register_ds AS register_ds,
    t1.pid AS pid,
    t1.member_id AS member_id,
    t2.member_name AS member_name,
    t1.site_id AS site_id,
    t1.adzone_id AS adzone_id,
    t1.sub_aff_id AS sub_aff_id,
    t1.sub_id1 AS sub_id1,
    t1.sub_id2 AS sub_id2,
    t1.sub_id3 AS sub_id3,
    t1.sub_id4 AS sub_id4,
    t1.sub_id5 AS sub_id5,
    t1.sub_id6 AS sub_id6,
    t1.total_commission_rate AS total_commission_rate,
    t1.platform_commission_rate AS platform_commission_rate,
    t1.brand_commission_rate AS brand_commission_rate,
    t1.total_payout_vnd AS total_payout_vnd,
    t1.platform_payout_vnd AS platform_payout_vnd,
    t1.brand_payout_vnd AS brand_payout_vnd,
    t1.total_payout_usd AS total_payout_usd,
    t1.platform_payout_usd AS platform_payout_usd,
    t1.brand_payout_usd AS brand_payout_usd,
    t1.hh AS hh,
    t1.ds AS ds
FROM (
        SELECT sales_order_item_id,
            order_id,
            order_number,
            mord_id,
            check_out_id,
            platform,
            status,
            adjust_type,
            is_fraud,
            is_sa,
            is_flashsale,
            is_every_day_lowprice,
            is_coin,
            is_freeship_max,
            is_lazada_purchase_incentive,
            is_choice,
            seller_id,
            seller_name,
            offer_id,
            product_id,
            product_name,
            sku_id,
            lazada_sku,
            buyer_id,
            is_new_buyer,
            g_segment_original,
            m_tag_original,
            profitable_value_original,
            g_segment_fixed,
            g_segment_fixed_level_1,
            g_segment_fixed_level_2,
            m_tag_fixed,
            profitable_value_fixed,
            user_segment,
            cluster,
            cat_lv1,
            bu_lv1,
            bu_lv2,
            delivery_location,
            delivery_company,
            payment_method,
            exchange_rate,
            gmv_vnd,
            gmv_usd,
            order_amt_vnd,
            order_amt_usd,
            group_segment_1,
            segment_1,
            pid,
            member_id,
            site_id,
            adzone_id,
            sub_aff_id,
            sub_id1,
            sub_id2,
            sub_id3,
            sub_id4,
            sub_id5,
            sub_id6,
            total_commission_rate,
            platform_commission_rate,
            brand_commission_rate,
            total_payout_vnd,
            platform_payout_vnd,
            brand_payout_vnd,
            total_payout_usd,
            platform_payout_usd,
            brand_payout_usd,
            hh,
            ds
        FROM lazada_analyst_dev.tmp_loutruong_aff_console_fulfill_di
    ) AS t1
    LEFT JOIN (
        SELECT group_segment,
            team_ops,
            plm_l30d_lv1,
            plm_l30d_lv2,
            segment,
            member_id,
            member_name,
            register_ds
        FROM lazada_analyst_dev.hg_aff_plm_mixed
        WHERE 1 = 1
            AND ds = MAX_PT('lazada_analyst_dev.hg_aff_plm_mixed')
    ) AS t2 ON t1.member_id = t2.member_id;
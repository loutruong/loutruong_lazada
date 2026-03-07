-- MaxCompute SQL 
-- ********************************************************************--
-- author:Truong, Van Thanh
-- create time:2024-06-18 12:18:01
-- ********************************************************************--
--@@ Input = lazada_cdm.dim_lzd_prd_product
--@@ Input = lazada_cdm.dim_lzd_slr_seller_vn
--@@ Input = lazada_mkt.ods_lzd_mkt_ad_offer_index_info
--@@ Input = lazada_mkt.ods_lzd_mkt_ad_sku_index_info
--@@ Output = lazada_analyst_dev.loutruong_dim_slr_brd_sku_cms
-- DROP TABLE IF EXISTS lazada_analyst_dev.loutruong_dim_slr_brd_sku_cms
-- ;
-- CREATE TABLE IF NOT EXISTS lazada_analyst_dev.loutruong_dim_slr_brd_sku_cms
-- (
--     seller_id                   STRING
--     ,brand_id                   STRING
--     ,seller_offer_id            STRING
--     ,seller_offer_name          STRING
--     ,seller_offer_is_ams        BIGINT
--     ,seller_offer_link          STRING
--     ,seller_offer_start         STRING
--     ,seller_offer_end           STRING
--     ,seller_ssc_commission      DOUBLE
--     ,seller_aff_commission      DOUBLE
--     ,is_slr_sku_offer_id_match  BIGINT
--     ,is_sku_uplift_purchasable  BIGINT
--     ,product_id                 STRING
--     ,sku_id                     STRING
--     ,sku_offer_id               STRING
--     ,sku_offer_is_ams           BIGINT
--     ,sku_offer_last_modify_date DATETIME
--     ,sku_ssc_commission         DOUBLE
--     ,sku_aff_commission         DOUBLE
-- )
-- PARTITIONED BY 
-- (
--     ds                          STRING
-- )
-- LIFECYCLE 3600
-- ;
WITH t_slr_brd_retails AS (
    SELECT parent_seller_id,
        seller_id,
        brand_id
    FROM lazada_cdm.dim_lzd_slr_seller_vn LATERAL VIEW EXPLODE(SPLIT(shop_associated_brands, ',')) brand_id AS brand_id
    WHERE 1 = 1
        AND ds = MAX_PT('lazada_cdm.dim_lzd_slr_seller')
        AND venture = 'VN'
        AND parent_seller_id IN (100101649)
),
t_sa_slr_master AS (
    SELECT ds,
        seller_id,
        brand_id,
        offer_id AS seller_offer_id,
        offer_name AS seller_offer_name,
CASE
            WHEN TOLOWER(offer_id) LIKE '%vn%' THEN 1
            ELSE 0
        END AS seller_offer_is_ams,
        link AS seller_offer_link,
        TO_CHAR(
            CASE
                WHEN venture IN ('VN', 'TH', 'ID') THEN sg_udf :bi_changetimezone(
                    FROM_UNIXTIME(CAST(starts AS BIGINT) / 1000),
                    'GMT+8',
                    'GMT+7'
                )
                ELSE FROM_UNIXTIME(CAST(starts AS BIGINT) / 1000)
            END,
            'yyyymmdd'
        ) AS seller_offer_start,
        TO_CHAR(
            CASE
                WHEN venture IN ('VN', 'TH', 'ID') THEN sg_udf :bi_changetimezone(
                    FROM_UNIXTIME(CAST(expires AS BIGINT) / 1000),
                    'GMT+8',
                    'GMT+7'
                )
                ELSE FROM_UNIXTIME(CAST(expires AS BIGINT) / 1000)
            END,
            'yyyymmdd'
        ) AS seller_offer_end,
        commission / 0.9 AS seller_ssc_commission,
        commission AS seller_aff_commission
    FROM lazada_mkt.ods_lzd_mkt_ad_offer_index_info
    WHERE 1 = 1 --
        -- AND     ds >= 20240101 --<< Start
        AND ds >= TO_CHAR(
            DATEADD(TO_DATE('${bizdate}', 'yyyymmdd'), -2, 'dd'),
            'yyyymmdd'
        ) --<< Daily
        AND TO_DATE(ds, 'yyyymmdd') BETWEEN CASE
            WHEN venture IN ('VN', 'TH', 'ID') THEN sg_udf :bi_changetimezone(
                FROM_UNIXTIME(CAST(starts AS BIGINT) / 1000),
                'GMT+8',
                'GMT+7'
            )
            ELSE FROM_UNIXTIME(CAST(starts AS BIGINT) / 1000)
        END
        AND CASE
            WHEN venture IN ('VN', 'TH', 'ID') THEN sg_udf :bi_changetimezone(
                FROM_UNIXTIME(CAST(expires AS BIGINT) / 1000),
                'GMT+8',
                'GMT+7'
            )
            ELSE FROM_UNIXTIME(CAST(expires AS BIGINT) / 1000)
        END
        AND venture = 'VN'
        AND is_test = 0
        AND is_ams = 1
        AND final_status = 1
        AND visibility = 1
        AND bus_type != 2
),
t_sa_sku_master AS (
    SELECT ds,
        seller_id,
        brand_id,
        product_id,
        sku_id,
        offer_id AS sku_offer_id,
CASE
            WHEN TOLOWER(offer_id) LIKE '%vn%' THEN 1
            ELSE 0
        END AS sku_offer_is_ams,
        last_modify_date AS sku_offer_last_modify_date,
        commission / 0.9 AS sku_ssc_commission,
        commission AS sku_aff_commission
    FROM lazada_mkt.ods_lzd_mkt_ad_sku_index_info
    WHERE 1 = 1 --
        -- AND     ds >= 20240101 --<< Start
        AND ds >= TO_CHAR(
            DATEADD(TO_DATE('${bizdate}', 'yyyymmdd'), -2, 'dd'),
            'yyyymmdd'
        ) --<< Daily
        AND venture = 'VN'
        AND is_test = 0
)
INSERT OVERWRITE TABLE lazada_analyst_dev.loutruong_dim_slr_brd_sku_cms PARTITION (ds)
SELECT t1.seller_id AS seller_id,
    t1.brand_id AS brand_id,
    t1.seller_offer_id AS seller_offer_id,
    t1.seller_offer_name AS seller_offer_name,
    t1.seller_offer_is_ams AS seller_offer_is_ams,
    t1.seller_offer_link AS seller_offer_link,
    t1.seller_offer_start AS seller_offer_start,
    t1.seller_offer_end AS seller_offer_end,
    t1.seller_ssc_commission AS seller_ssc_commission,
    t1.seller_aff_commission AS seller_aff_commission,
CASE
        WHEN t1.seller_offer_id = t2.sku_offer_id THEN 1
        ELSE 0
    END AS is_slr_sku_offer_id_match,
CASE
        WHEN t2.sku_id IS NOT NULL THEN 1
        ELSE 0
    END AS is_sku_uplift_purchasable,
    t2.product_id AS product_id,
    t2.sku_id AS sku_id,
    t2.sku_offer_id AS sku_offer_id,
    t2.sku_offer_is_ams AS sku_offer_is_ams,
    t2.sku_offer_last_modify_date AS sku_offer_last_modify_date,
    t2.sku_ssc_commission AS sku_ssc_commission,
    t2.sku_aff_commission AS sku_aff_commission,
    t1.ds AS ds
FROM (
        SELECT t1.ds AS ds,
            t2.seller_id AS seller_id,
            t1.brand_id AS brand_id,
            t1.seller_offer_id AS seller_offer_id,
            t1.seller_offer_name AS seller_offer_name,
            t1.seller_offer_is_ams AS seller_offer_is_ams,
            t1.seller_offer_link AS seller_offer_link,
            t1.seller_offer_start AS seller_offer_start,
            t1.seller_offer_end AS seller_offer_end,
            t1.seller_ssc_commission AS seller_ssc_commission,
            t1.seller_aff_commission AS seller_aff_commission
        FROM (
                SELECT ds,
                    seller_id,
                    brand_id,
                    seller_offer_id,
                    seller_offer_name,
                    seller_offer_is_ams,
                    seller_offer_link,
                    seller_offer_start,
                    seller_offer_end,
                    seller_ssc_commission,
                    seller_aff_commission
                FROM t_sa_slr_master
                WHERE 1 = 1
                    AND seller_id IN (100101649)
            ) AS t1
            INNER JOIN (
                SELECT parent_seller_id,
                    seller_id,
                    brand_id
                FROM t_slr_brd_retails
            ) AS t2 ON t1.seller_id = t2.parent_seller_id
            AND t1.brand_id = t2.brand_id
        UNION ALL
        --<< BREAK POINT
        SELECT t1.ds AS ds,
            t1.seller_id AS seller_id,
            t2.brand_id AS brand_id,
            t1.seller_offer_id AS seller_offer_id,
            t1.seller_offer_name AS seller_offer_name,
            t1.seller_offer_is_ams AS seller_offer_is_ams,
            t1.seller_offer_link AS seller_offer_link,
            t1.seller_offer_start AS seller_offer_start,
            t1.seller_offer_end AS seller_offer_end,
            t1.seller_ssc_commission AS seller_ssc_commission,
            t1.seller_aff_commission AS seller_aff_commission
        FROM (
                SELECT ds,
                    seller_id,
                    brand_id,
                    seller_offer_id,
                    seller_offer_name,
                    seller_offer_is_ams,
                    seller_offer_link,
                    seller_offer_start,
                    seller_offer_end,
                    seller_ssc_commission,
                    seller_aff_commission
                FROM t_sa_slr_master
                WHERE 1 = 1
                    AND seller_id NOT IN (100101649)
                    AND brand_id IS NULL
            ) AS t1
            INNER JOIN (
                SELECT DISTINCT seller_id,
                    brand_id
                FROM lazada_cdm.dim_lzd_prd_product
                WHERE 1 = 1
                    AND ds = MAX_PT('lazada_cdm.dim_lzd_prd_product')
                    AND venture = 'VN'
            ) AS t2 ON t1.seller_id = t2.seller_id
        UNION ALL
        --<< BREAK POINT
        SELECT ds,
            seller_id,
            brand_id,
            seller_offer_id,
            seller_offer_name,
            seller_offer_is_ams,
            seller_offer_link,
            seller_offer_start,
            seller_offer_end,
            seller_ssc_commission,
            seller_aff_commission
        FROM t_sa_slr_master
        WHERE 1 = 1
            AND seller_id NOT IN (100101649)
            AND brand_id IS NOT NULL
    ) AS t1
    LEFT JOIN (
        SELECT t1.ds AS ds,
            t2.seller_id AS seller_id,
            t1.brand_id AS brand_id,
            t1.product_id AS product_id,
            t1.sku_id AS sku_id,
            t1.sku_offer_id AS sku_offer_id,
            t1.sku_offer_is_ams AS sku_offer_is_ams,
            t1.sku_offer_last_modify_date AS sku_offer_last_modify_date,
            t1.sku_ssc_commission AS sku_ssc_commission,
            t1.sku_aff_commission AS sku_aff_commission
        FROM (
                SELECT ds,
                    seller_id,
                    brand_id,
                    product_id,
                    sku_id,
                    sku_offer_id,
                    sku_offer_is_ams,
                    sku_offer_last_modify_date,
                    sku_ssc_commission,
                    sku_aff_commission
                FROM t_sa_sku_master
                WHERE 1 = 1
                    AND seller_id IN (100101649)
            ) AS t1
            INNER JOIN (
                SELECT parent_seller_id,
                    seller_id,
                    brand_id
                FROM t_slr_brd_retails
            ) AS t2 ON t1.seller_id = t2.parent_seller_id
            AND t1.brand_id = t2.brand_id
        UNION ALL
        --<< BREAK POINT
        SELECT ds,
            seller_id,
            brand_id,
            product_id,
            sku_id,
            sku_offer_id,
            sku_offer_is_ams,
            sku_offer_last_modify_date,
            sku_ssc_commission,
            sku_aff_commission
        FROM t_sa_sku_master
        WHERE 1 = 1
            AND seller_id NOT IN (100101649)
    ) AS t2 ON t1.ds = t2.ds
    AND t1.seller_id = t2.seller_id
    AND t1.brand_id = t2.brand_id;
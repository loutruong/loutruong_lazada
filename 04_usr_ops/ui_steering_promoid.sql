-- MaxCompute SQL 
-- ********************************************************************--
-- author:Truong, Van Thanh
-- create time:2024-07-06 11:51:31
-- ********************************************************************--
SELECT t1.promotion_id AS promotion_id,
    t3.voucher_name AS promotion_name,
    t3.description AS promotion_desc,
CASE
        WHEN t2.promotion_id IS NOT NULL THEN 1
        ELSE 0
    END AS is_offline_storage,
CASE
        WHEN t3.product_code IN ('categoryCoupon', 'collectibleCoupon')
        AND (
            TOLOWER(t3.voucher_name) LIKE '%voucher%bonus%'
            OR TOLOWER(t3.description) LIKE '%voucher%bonus%'
        ) THEN '02_lpi_all_ops'
        ELSE '01_fsm_all_ops'
    END AS mechanic,
    t3.promotion_start_date AS promotion_start_date,
    t3.promotion_end_date AS promotion_end_date,
    t3.teasing_start_date AS teasing_start_date,
    t3.teasing_end_date AS teasing_end_date,
    t3.status AS status
FROM (
        SELECT promotion_id
        FROM lazada_analyst.loutruong_upo_ui_fsm_local_ops
        UNION
        SELECT promotion_id
        FROM lazada_analyst.loutruong_upo_ui_lpi_local_ops
        UNION
        SELECT CAST(promotion_id AS STRING) AS promotion_id
        FROM lazada_cdm.dim_lzd_pro_collectibles
        WHERE 1 = 1
            AND ds = MAX_PT("lazada_cdm.dim_lzd_pro_collectibles")
            AND venture = 'VN'
            AND TOLOWER(sponsor) IN ('platform')
            AND (
                TOLOWER(retail_sponsor) IN ('platform')
                OR retail_sponsor IS NULL
            )
            AND prom_sub_type IN ('FS Max')
        UNION
        SELECT CAST(promotion_id AS STRING) AS promotion_id
        FROM lazada_cdm.dim_lzd_pro_collectibles
        WHERE 1 = 1
            AND ds = MAX_PT("lazada_cdm.dim_lzd_pro_collectibles")
            AND venture = 'VN'
            AND TOLOWER(sponsor) IN ('platform')
            AND (
                TOLOWER(retail_sponsor) IN ('platform')
                OR retail_sponsor IS NULL
            )
            AND product_code IN ('categoryCoupon', 'collectibleCoupon')
            AND (
                TOLOWER(voucher_name) LIKE '%voucher%bonus%'
                OR TOLOWER(description) LIKE '%voucher%bonus%'
            )
    ) AS t1
    LEFT JOIN (
        SELECT promotion_id
        FROM lazada_analyst.loutruong_upo_ui_fsm_local_ops
        UNION
        SELECT promotion_id
        FROM lazada_analyst.loutruong_upo_ui_lpi_local_ops
    ) AS t2 ON t1.promotion_id = t2.promotion_id
    LEFT JOIN (
        SELECT *
        FROM lazada_cdm.dim_lzd_pro_collectibles
        WHERE 1 = 1
            AND ds = MAX_PT("lazada_cdm.dim_lzd_pro_collectibles")
            AND venture = 'VN'
    ) AS t3 ON t1.promotion_id = t3.promotion_id;
SELECT t2.ds AS ds,
    t1.promotion_id AS promotion_id,
    t3.voucher_name AS promotion_name,
    t3.description AS promotion_description,
    t1.mechanics,
CASE
        WHEN t1.mechanics IN ('fsm_local_ops') THEN CONCAT(
            REPLACE(t3.coupon_value / 1000, '.0', ''),
            'k',
            '/',
            REPLACE(t3.min_order_value / 1000, '.0', ''),
            'k'
        )
        WHEN t1.mechanics IN ('lpi_local_ops') THEN CONCAT(
            REPLACE(t3.coupon_value, '.0', ''),
            '%',
            '/',
            REPLACE(t3.min_order_value / 1000, '.0', ''),
            'k',
            ' ',
            'Cap',
            ' ',
            REPLACE(t3.promotion_cap_value / 1000, '.0', ''),
            'k'
        )
        ELSE 0
    END AS promotion_scheme,
    TO_CHAR(t3.promotion_start_date, 'yyyymmdd') AS promotion_start_date,
    TO_CHAR(t3.promotion_end_date, 'yyyymmdd') AS promotion_end_date,
    COUNT(DISTINCT t4.buyer_id) AS redeem_uv,
    COUNT(DISTINCT t4.check_out_id) AS redeem,
    SUM(
        COALESCE(t4.promotion_amount, 0) * COALESCE(t4.exchange_rate, 0)
    ) AS promo_amt_gross_total,
    SUM(
        CASE
            WHEN t4.item_status_esm IN ('shipped', 'exportable', 'delivered') THEN COALESCE(t4.promotion_amount, 0) * COALESCE(t4.exchange_rate, 0)
            ELSE 0
        END
    ) AS promo_amt_net_total,
    SUM(
        COALESCE(t4.actual_gmv, 0) * COALESCE(t4.exchange_rate, 0)
    ) AS gmv
FROM (
        SELECT *,
            1 AS map_key
        FROM lazada_analyst.loutruong_upo_ui_fsm_local_ops
        UNION
        SELECT *,
            1 AS map_key
        FROM lazada_analyst.loutruong_upo_ui_lpi_local_ops
    ) t1
    INNER JOIN (
        SELECT ds_day AS ds,
            1 AS map_key
        FROM lazada_cdm.dim_lzd_date
        WHERE 1 = 1
            AND ds_day BETWEEN 20240401 AND TO_CHAR(GETDATE(), 'yyyymmdd')
    ) t2 ON t1.map_key = t2.map_key
    LEFT JOIN (
        SELECT promotion_id,
            voucher_name,
            description,
            coupon_value,
            min_order_value,
            promotion_cap_value,
            promotion_start_date,
            promotion_end_date
        FROM lazada_cdm.dwd_lzd_pro_collectibles_hh
        WHERE 1 = 1
            AND ds = MAX_PT('lazada_cdm.dwd_lzd_pro_collectibles_hh')
            AND hh = (
                SELECT MAX(hh)
                FROM lazada_cdm.dwd_lzd_pro_collectibles_hh
                WHERE 1 = 1
                    AND ds = MAX_PT('lazada_cdm.dwd_lzd_pro_collectibles_hh')
                    AND venture = 'VN'
            )
            AND venture = 'VN'
        UNION
        SELECT promotion_id,
            voucher_name,
            description,
            coupon_value,
            min_order_value,
            promotion_cap_value,
            promotion_start_date,
            promotion_end_date
        FROM lazada_cdm.dim_lzd_pro_collectibles
        WHERE 1 = 1
            AND ds = MAX_PT('lazada_cdm.dim_lzd_pro_collectibles')
            AND venture = 'VN'
    ) t3 ON t1.promotion_id = t3.promotion_id
    LEFT JOIN (
        SELECT t1.ds AS ds,
            t1.hh AS hh,
            t2.promotion_id AS promotion_id,
            t1.check_out_id AS check_out_id,
            t1.buyer_id AS buyer_id,
            t1.item_status_esm AS item_status_esm,
            t2.promotion_amount AS promotion_amount,
            t1.actual_gmv AS actual_gmv,
            t1.exchange_rate AS exchange_rate
        FROM (
                SELECT ds,
                    TO_CHAR(fulfillment_create_date, 'hh') AS hh,
                    sales_order_item_id,
                    order_id,
                    order_number,
                    check_out_id,
                    buyer_id,
                    item_status_esm,
                    business_type,
                    business_type_level2,
                    industry_name,
                    regional_category1_name,
                    delivery_company,
                    shipping_region1_name,
                    actual_gmv,
                    exchange_rate
                FROM lazada_cdm.dwd_lzd_trd_core_hh
                WHERE 1 = 1
                    AND ds = TO_CHAR(GETDATE(), 'yyyymmdd')
                    AND TO_CHAR(fulfillment_create_date, 'yyyymmdd') = TO_CHAR(GETDATE(), 'yyyymmdd')
                    AND venture = 'VN'
                    AND is_revenue = 1
                    AND COALESCE(business_application, 'LZD') IN ('LZD,ZAL', 'LZD')
                UNION
                SELECT ds,
                    TO_CHAR(fulfillment_create_date, 'hh') AS hh,
                    sales_order_item_id,
                    order_id,
                    order_number,
                    check_out_id,
                    buyer_id,
                    item_status_esm,
                    business_type,
                    business_type_level2,
                    industry_name,
                    regional_category1_name,
                    delivery_company,
                    shipping_region1_name,
                    actual_gmv,
                    exchange_rate
                FROM lazada_cdm.dwd_lzd_trd_core_fulfill_di
                WHERE 1 = 1
                    AND ds >= 20240401
                    AND venture = 'VN'
                    AND is_revenue = 1
                    AND COALESCE(business_application, 'LZD') IN ('LZD,ZAL', 'LZD')
            ) t1
            LEFT JOIN (
                SELECT sales_order_item_id,
                    promotion_id,
                    promotion_amount
                FROM lazada_cdm.dwd_lzd_pro_promotion_item_hh_vn
                WHERE 1 = 1
                    AND ds = TO_CHAR(GETDATE(), 'yyyymmdd')
                    AND venture = 'VN'
                    AND is_fulfilled = 1
                UNION
                SELECT sales_order_item_id,
                    promotion_id,
                    promotion_amount
                FROM lazada_cdm.dwd_lzd_pro_promotion_item_di
                WHERE 1 = 1
                    AND ds >= 20240101
                    AND venture = 'VN'
                    AND is_fulfilled = 1
            ) t2 ON t1.sales_order_item_id = t2.sales_order_item_id
    ) t4 ON t2.ds = t4.ds
    AND t1.promotion_id = t4.promotion_id
GROUP BY t2.ds,
    t1.promotion_id,
    t3.voucher_name,
    t3.description,
    t1.mechanics,
CASE
        WHEN t1.mechanics IN ('fsm_local_ops') THEN CONCAT(
            REPLACE(t3.coupon_value / 1000, '.0', ''),
            'k',
            '/',
            REPLACE(t3.min_order_value / 1000, '.0', ''),
            'k'
        )
        WHEN t1.mechanics IN ('lpi_local_ops') THEN CONCAT(
            REPLACE(t3.coupon_value, '.0', ''),
            '%',
            '/',
            REPLACE(t3.min_order_value / 1000, '.0', ''),
            'k',
            ' ',
            'Cap',
            ' ',
            REPLACE(t3.promotion_cap_value / 1000, '.0', ''),
            'k'
        )
        ELSE 0
    END,
    TO_CHAR(t3.promotion_start_date, 'yyyymmdd'),
    TO_CHAR(t3.promotion_end_date, 'yyyymmdd');
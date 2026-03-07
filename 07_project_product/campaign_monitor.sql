-- MaxCompute SQL 
-- ********************************************************************--
-- author:Truong, Van Thanh
-- create time:2024-11-05 16:56:59
-- ********************************************************************--
-- ********************************************************************--
-- Change controller to grab correct data
-- ********************************************************************--
WITH t_dim_date AS (
    SELECT ds_day AS ds,
CASE
            WHEN ds_day BETWEEN 20240905 AND 20240908 THEN 'D09'
            WHEN ds_day BETWEEN 20241107 AND 20241110 THEN 'D11'
        END AS campaign_name --<< Controller
    FROM lazada_cdm.dim_lzd_date
    WHERE 1 = 1
        AND (
            ds_day BETWEEN 20240905 AND 20240908
            OR ds_day BETWEEN 20241107 AND 20241110
        ) --<< Controller
),
t_dim_promo AS (
    SELECT status,
        promotion_id,
        voucher_name,
        description,
        teasing_start_date,
        teasing_end_date,
        promotion_start_date,
        promotion_end_date,
        product_code,
        product_sub_code,
        prom_type,
        prom_sub_type,
        features,
CASE
            WHEN prom_sub_type IN ('FS Max') THEN '01_fsm_all_ops'
            WHEN product_code IN ('categoryCoupon', 'collectibleCoupon')
            AND (
                TOLOWER(voucher_name) LIKE '%voucher%bonus%'
                OR TOLOWER(description) LIKE '%voucher%bonus%'
            ) THEN '02_lpi_all_ops'
            WHEN (
                CAST(
                    REGEXP_EXTRACT(
                        KEYVALUE(features, ',', ':', '"voucherSubType"'),
                        '\"([0-9]*)\"'
                    ) AS BIGINT
                ) IN (7) --<< Effected after 20241018 00:00:00
                OR (
                    product_code IN ('categoryCoupon') --<< ,'collectibleCoupon' to get vcm 1.0
                    AND (
                        TOLOWER(voucher_name) LIKE '%platform_voucher max%'
                        OR TOLOWER(voucher_name) LIKE '%voucher%max%'
                        OR TOLOWER(description) LIKE '%platform_voucher max%'
                        OR TOLOWER(description) LIKE '%voucher%max%'
                    )
                ) --<< Effected 20241001 00:00:00 ~~ 20241017 23:59:59
            )
            AND TOLOWER(voucher_name) NOT LIKE '%day%' --<< Only open to get all vcm all ops since campaign voucher use the same setting template 
            THEN '03_vcm_all_ops'
            ELSE 'Others'
        END AS promo_mechanics,
        CAST(
            REGEXP_EXTRACT(
                KEYVALUE(features, ',', ':', '"voucherSubType"'),
                '\"([0-9]*)\"'
            ) AS BIGINT
        ) AS voucher_sub_type_id,
CASE
            WHEN CAST(
                REGEXP_EXTRACT(
                    KEYVALUE(features, ',', ':', '"voucherSubType"'),
                    '\"([0-9]*)\"'
                ) AS BIGINT
            ) = 0 THEN 'All_sub_type'
            WHEN CAST(
                REGEXP_EXTRACT(
                    KEYVALUE(features, ',', ':', '"voucherSubType"'),
                    '\"([0-9]*)\"'
                ) AS BIGINT
            ) = 1 THEN 'PLATFORM_WIDE'
            WHEN CAST(
                REGEXP_EXTRACT(
                    KEYVALUE(features, ',', ':', '"voucherSubType"'),
                    '\"([0-9]*)\"'
                ) AS BIGINT
            ) = 2 THEN 'CATEGORY_VOUCHER'
            WHEN CAST(
                REGEXP_EXTRACT(
                    KEYVALUE(features, ',', ':', '"voucherSubType"'),
                    '\"([0-9]*)\"'
                ) AS BIGINT
            ) = 3 THEN 'Co_sub_pdv'
            WHEN CAST(
                REGEXP_EXTRACT(
                    KEYVALUE(features, ',', ':', '"voucherSubType"'),
                    '\"([0-9]*)\"'
                ) AS BIGINT
            ) = 4 THEN 'FLASH_VOUCHER'
            WHEN CAST(
                REGEXP_EXTRACT(
                    KEYVALUE(features, ',', ':', '"voucherSubType"'),
                    '\"([0-9]*)\"'
                ) AS BIGINT
            ) = 5 THEN 'CAMPAIGN_PDV'
            WHEN CAST(
                REGEXP_EXTRACT(
                    KEYVALUE(features, ',', ':', '"voucherSubType"'),
                    '\"([0-9]*)\"'
                ) AS BIGINT
            ) = 6 THEN 'Free_shipping_voucher'
            WHEN CAST(
                REGEXP_EXTRACT(
                    KEYVALUE(features, ',', ':', '"voucherSubType"'),
                    '\"([0-9]*)\"'
                ) AS BIGINT
            ) = 7 THEN 'VOUCHER_MAX'
            WHEN CAST(
                REGEXP_EXTRACT(
                    KEYVALUE(features, ',', ':', '"voucherSubType"'),
                    '\"([0-9]*)\"'
                ) AS BIGINT
            ) = 8 THEN 'Laz_mall_pdv'
            ELSE 'Others'
        END voucher_sub_type_name,
CASE
            WHEN voucher_type IN ('fixed_amount') THEN CONCAT(
                CAST(COALESCE(coupon_value, 0) / 1000 AS BIGINT),
                'k',
                '/',
                CAST(COALESCE(min_order_value, 0) / 1000 AS BIGINT),
                'k'
            )
            WHEN voucher_type IN ('percent_amount') THEN CONCAT(
                CAST(COALESCE(coupon_value, 0) AS BIGINT),
                '%',
                '/',
                CAST(COALESCE(min_order_value, 0) / 1000 AS BIGINT),
                'k',
                ' ',
                'Cap',
                ' ',
                CAST(COALESCE(promotion_cap_value, 0) / 1000 AS BIGINT),
                'k'
            )
        END AS voucher_scheme,
CASE
            WHEN voucher_type IN ('percent_amount') THEN CASE
                WHEN CAST(COALESCE(promotion_cap_value, 0) / 1000 AS BIGINT) IN (1000) THEN 'Campaign bundle - Ultra high' --<< Campaign bundle section logic
                WHEN CAST(COALESCE(promotion_cap_value, 0) / 1000 AS BIGINT) IN (400) THEN 'Campaign bundle - High'
                WHEN CAST(COALESCE(promotion_cap_value, 0) / 1000 AS BIGINT) IN (100) THEN 'Campaign bundle - Mid'
                WHEN CAST(COALESCE(promotion_cap_value, 0) / 1000 AS BIGINT) IN (30) THEN 'Campaign bundle - Low'
                WHEN CAST(COALESCE(promotion_cap_value, 0) / 1000 AS BIGINT) IN (75) THEN 'Daily bundle - High' --<< Daily bundle section logic
                WHEN CAST(COALESCE(promotion_cap_value, 0) / 1000 AS BIGINT) IN (15) THEN 'Daily bundle - Mid'
                WHEN CAST(COALESCE(promotion_cap_value, 0) / 1000 AS BIGINT) IN (10) THEN 'Daily bundle - Low'
                ELSE 'Other'
            END
            ELSE 'Other'
        END AS voucher_tier
    FROM lazada_cdm.dim_lzd_pro_collectibles
    WHERE 1 = 1
        AND ds = MAX_PT('lazada_cdm.dim_lzd_pro_collectibles')
        AND venture = 'VN'
        AND REGEXP_INSTR(voucher_name, '^.*(TEST|Test|test)+.*') = 0 --<< Clear data
        AND sponsor = 'platform'
        AND (
            TOLOWER(retail_sponsor) IN ('platform')
            OR retail_sponsor IS NULL
        )
        AND (
            prom_sub_type IN ('FS Max')
            OR (
                product_code IN ('categoryCoupon', 'collectibleCoupon')
                AND (
                    TOLOWER(voucher_name) LIKE '%voucher%bonus%'
                    OR TOLOWER(description) LIKE '%voucher%bonus%'
                )
            )
            OR (
                (
                    CAST(
                        REGEXP_EXTRACT(
                            KEYVALUE(features, ',', ':', '"voucherSubType"'),
                            '\"([0-9]*)\"'
                        ) AS BIGINT
                    ) IN (7) --<< Effected after 20241018 00:00:00
                    OR (
                        product_code IN ('categoryCoupon') --<< ,'collectibleCoupon' to get vcm 1.0
                        AND (
                            TOLOWER(voucher_name) LIKE '%platform_voucher max%'
                            OR TOLOWER(voucher_name) LIKE '%voucher%max%'
                            OR TOLOWER(description) LIKE '%platform_voucher max%'
                            OR TOLOWER(description) LIKE '%voucher%max%'
                        )
                    ) --<< Effected 20241001 00:00:00 ~~ 20241017 23:59:59
                )
                AND TOLOWER(voucher_name) NOT LIKE '%day%' --<< Only open to get all vcm all ops since campaign voucher use the same setting template
            )
        )
)
SELECT t1.campaign_name AS campaign_name,
    t2.promo_mechanics AS promo_mechanics,
    t1.plt_dau AS plt_dau,
    COALESCE(t2.collect_uv, 0) AS collect_uv,
    COALESCE(t2.app_dau_voh, 0) AS app_dau_voh,
    COALESCE(t2.redeem_uv, 0) AS redeem_uv
FROM (
        SELECT t2.campaign_name AS campaign_name,
            COUNT(DISTINCT t1.utdid) AS plt_dau
        FROM (
                SELECT *
                FROM lazada_cdm.dws_lzd_mkt_utdid_source_pv_1d
                WHERE 1 = 1
                    AND ds IN (
                        SELECT ds
                        FROM t_dim_date
                    )
                    AND venture = 'VN'
                    AND is_first_launch = 1
                    AND is_active_utdid = 1
                    AND is_paid = "ALL"
                    AND sub_source != "ALL"
                    AND utdid IS NOT NULL
            ) AS t1
            INNER JOIN (
                SELECT *
                FROM t_dim_date
            ) AS t2 ON t1.ds = t2.ds
        GROUP BY t2.campaign_name
    ) AS t1
    LEFT JOIN (
        SELECT campaign_name,
            promo_mechanics,
            SUM(collect_uv) AS collect_uv,
            SUM(app_dau_voh) AS app_dau_voh,
            SUM(redeem_uv) AS redeem_uv
        FROM (
                SELECT t4.campaign_name AS campaign_name,
                    t2.promo_mechanics AS promo_mechanics,
                    COUNT(DISTINCT t1.buyer_id) AS collect_uv,
                    0 AS app_dau_voh,
                    0 AS redeem_uv
                FROM (
                        SELECT *
                        FROM lazada_cdm.dwd_lzd_pro_voucher_collect_di
                        WHERE 1 = 1 --
                            AND ds IN (
                                SELECT DISTINCT ds
                                FROM t_dim_date
                            )
                            AND venture = 'VN'
                    ) AS t1
                    INNER JOIN (
                        SELECT *
                        FROM t_dim_promo
                    ) AS t2 ON t1.promotion_id = t2.promotion_id
                    INNER JOIN (
                        SELECT *
                        FROM t_dim_date
                    ) AS t4 ON t1.ds = t4.ds
                GROUP BY t4.campaign_name,
                    t2.promo_mechanics
                UNION ALL
                --<< Break point
                SELECT t4.campaign_name AS campaign_name,
                    t3.promo_mechanics AS promo_mechanics,
                    0 AS collect_uv,
                    COUNT(DISTINCT t1.utdid) AS app_dau_voh,
                    0 AS redeem_uv
                FROM (
                        SELECT *
                        FROM lazada_cdm.dws_lzd_mkt_app_ad_mp_di
                        WHERE 1 = 1
                            AND ds IN (
                                SELECT DISTINCT ds
                                FROM t_dim_date
                            )
                            AND venture = 'VN'
                            AND attr_model IN ('ft_1d_np')
                            AND utdid IS NOT NULL
                            AND is_active_utdid = 1
                    ) AS t1
                    INNER JOIN (
                        SELECT *
                        FROM lazada_cdm.dwd_lzd_pro_voucher_collect_di
                        WHERE 1 = 1 --
                            AND ds >= TO_CHAR(
                                DATEADD(
                                    TO_DATE(
                                        (
                                            SELECT MIN(ds)
                                            FROM t_dim_date
                                        ),
                                        'yyyymmdd'
                                    ),
                                    -35,
                                    'dd'
                                ),
                                'yyyymmdd'
                            )
                            AND venture = 'VN'
                            AND start_local_date >= 20240301 --<< Controller
                    ) AS t2 ON t1.ds >= t2.ds --<< Collect ==>> Has voucher on hand ==>> Open app
                    AND t1.user_id = t2.buyer_id
                    AND t1.ds BETWEEN t2.start_local_date AND t2.end_local_date
                    INNER JOIN (
                        SELECT *
                        FROM t_dim_promo
                    ) AS t3 ON t2.promotion_id = t3.promotion_id
                    INNER JOIN (
                        SELECT *
                        FROM t_dim_date
                    ) AS t4 ON t1.ds = t4.ds
                GROUP BY t4.campaign_name,
                    t3.promo_mechanics
                UNION ALL
                --<< Break point
                SELECT t4.campaign_name AS campaign_name,
                    t3.promo_mechanics AS promo_mechanics,
                    0 AS collect_uv,
                    0 AS app_dau_voh,
                    COUNT(DISTINCT t1.buyer_id) AS redeem_uv
                FROM (
                        SELECT *
                        FROM lazada_cdm.dwd_lzd_trd_core_fulfill_di
                        WHERE 1 = 1
                            AND ds IN (
                                SELECT DISTINCT ds
                                FROM t_dim_date
                            )
                            AND venture = 'VN'
                            AND is_revenue = 1
                            AND COALESCE(business_application, 'LZD') IN ('LZD,ZAL', 'LZD')
                    ) AS t1
                    LEFT JOIN (
                        SELECT *
                        FROM lazada_cdm.dwd_lzd_pro_promotion_item_di
                        WHERE 1 = 1 --
                            AND ds >= TO_CHAR(
                                DATEADD(
                                    TO_DATE(
                                        (
                                            SELECT MIN(ds)
                                            FROM t_dim_date
                                        ),
                                        'yyyymmdd'
                                    ),
                                    -35,
                                    'dd'
                                ),
                                'yyyymmdd'
                            )
                            AND venture = 'VN'
                            AND is_fulfilled = 1
                            AND TOLOWER(promotion_role) IN ('platform')
                            AND (
                                TOLOWER(retail_sponsor) IN ('platform')
                                OR retail_sponsor IS NULL
                            )
                    ) AS t2 ON t1.sales_order_item_id = t2.sales_order_item_id
                    INNER JOIN (
                        SELECT *
                        FROM t_dim_promo
                    ) AS t3 ON t2.promotion_id = t3.promotion_id
                    INNER JOIN (
                        SELECT *
                        FROM t_dim_date
                    ) AS t4 ON t1.ds = t4.ds
                GROUP BY t4.campaign_name,
                    t3.promo_mechanics
            )
        GROUP BY campaign_name,
            promo_mechanics
    ) AS t2 ON t1.campaign_name = t2.campaign_name;
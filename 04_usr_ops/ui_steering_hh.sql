-- MaxCompute SQL 
-- ********************************************************************--
-- author:Truong, Van Thanh
-- create time:2024-08-01 20:40:21
-- ********************************************************************--
-- DROP TABLE IF EXISTS lazada_analyst_dev.loutruong_upo_ui_steering_hh
-- ;
-- CREATE TABLE IF NOT EXISTS lazada_analyst_dev.loutruong_upo_ui_steering_hh
-- (
--     user_contribution_tag  STRING
--     ,promotion_mechanics   STRING
--     ,dau                   DOUBLE COMMENT 'dau total user_segment'
--     ,collect_uv            DOUBLE
--     ,redeem_uv             DOUBLE
--     ,usage                 DOUBLE
-- )
-- PARTITIONED BY 
-- (
--     ds                     STRING
--     ,hh                    STRING
-- )
-- LIFECYCLE 3600
-- ;
WITH t_user_segment AS (
    SELECT ds_month,
        user_id,
        user_contribution_tag AS user_contribution_tag_old,
        CASE
            WHEN user_contribution_tag IN ('high') THEN '01_high'
            WHEN user_contribution_tag IN ('middle') THEN '02_middle'
            WHEN user_contribution_tag IN ('low') THEN '03_low'
            WHEN user_contribution_tag IN ('lto 3-') THEN '04_lto 3-'
            WHEN user_contribution_tag IN ('undefined') THEN '05_undefined'
        END AS user_contribution_tag
    FROM lazada_ads.dws_lzd_cdp_usr_uid_contribute_value_v3
    WHERE 1 = 1 --
        AND ds_month = MAX_PT(
            'lazada_ads.dws_lzd_cdp_usr_uid_contribute_value_v3'
        ) --<< Daily
        -- AND     ds_month >= 202407 --<< Start
        AND venture = 'VN'
),
t_dim_promo AS (
    SELECT *,
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
                CAST(
                    COALESCE(promotion_cap_value, 0) / 1000 AS BIGINT
                ),
                'k'
            )
        END AS promotion_scheme
    FROM lazada_cdm.dim_lzd_pro_collectibles
    WHERE 1 = 1
        AND ds = MAX_PT('lazada_cdm.dim_lzd_pro_collectibles')
        AND venture = 'VN'
        AND sponsor = 'platform'
        AND (
            TOLOWER(retail_sponsor) IN ('platform')
            OR retail_sponsor IS NULL
        )
),
t_trf_dwd AS (
    SELECT t1.ds AS ds,
        t1.hh AS hh,
        t1.utdid AS utdid,
        t1.user_id AS user_id,
        COALESCE(t2.user_contribution_tag, '05_undefined') AS user_contribution_tag
    FROM (
            SELECT ds,
                TO_CHAR(TO_DATE(visit_time, 'yyyymmddhhmiss'), 'hh') AS hh,
                utdid,
                user_id
            FROM lazada_cdm.dwd_lzd_mkt_app_traffic_pv_di
            WHERE 1 = 1 --
                -- AND     ds = 20241008 --<< Test
                -- AND     ds BETWEEN '${start_date}' AND '${end_date}' --<< Start
                AND ds >= TO_CHAR(
                    DATEADD(TO_DATE('${bizdate}', 'yyyymmdd'), 0, 'dd'),
                    'yyyymmdd'
                ) --<< Daily
                AND venture = 'VN'
                AND utdid IS NOT NULL
                AND is_active_utdid = 1
                AND TOLOWER(sg_udf :bi_get_aplus_appinfo(app_id, 'code')) IN ('lazada')
        ) AS t1
        LEFT JOIN (
            SELECT *
            FROM t_user_segment
        ) AS t2 ON TO_CHAR(TO_DATE(t1.ds, 'yyyymmdd'), 'yyyymm') = TO_CHAR(
            DATEADD(TO_DATE(t2.ds_month, 'yyyymm'), + 1, 'mm'),
            'yyyymm'
        )
        AND t1.user_id = t2.user_id
),
t_clt_dwd AS (
    SELECT t1.collected_date AS collected_date,
        t1.ds AS ds,
        TO_CHAR(
            TO_DATE(t1.collected_date, 'yyyy-mm-dd hh:mi:ss'),
            'hh'
        ) AS hh,
        t1.buyer_id AS user_id,
        COALESCE(t3.user_contribution_tag, '05_undefined') AS user_contribution_tag,
        t1.promotion_id AS promotion_id,
        t2.voucher_name AS promotion_name,
        t2.description AS promotion_desc,
        CASE
            WHEN t2.prom_sub_type IN ('FS Max') THEN '01_fsm_all_ops'
            WHEN t2.product_code IN ('categoryCoupon', 'collectibleCoupon')
            AND (
                TOLOWER(t2.voucher_name) LIKE '%voucher%bonus%'
                OR TOLOWER(t2.description) LIKE '%voucher%bonus%'
            ) THEN '02_lpi_all_ops'
            WHEN t2.product_code IN ('categoryCoupon')
            AND (
                TOLOWER(t2.voucher_name) LIKE '%platform_voucher max%'
                OR TOLOWER(t2.voucher_name) LIKE '%voucher%max%'
                OR TOLOWER(t2.description) LIKE '%platform_voucher max%'
                OR TOLOWER(t2.description) LIKE '%voucher%max%'
            ) THEN '03_vcm_all_ops'
        END AS promotion_mechanics
    FROM (
            SELECT *
            FROM lazada_cdm.dwd_lzd_pro_voucher_collect_di
            WHERE 1 = 1 --
                -- AND     ds = 20241008 --<< Test
                -- AND     ds BETWEEN '${start_date}' AND '${end_date}' --<< Start
                AND ds >= TO_CHAR(
                    DATEADD(TO_DATE('${bizdate}', 'yyyymmdd'), 0, 'dd'),
                    'yyyymmdd'
                ) --<< Daily
                AND venture = 'VN'
        ) AS t1
        LEFT JOIN (
            SELECT *
            FROM t_dim_promo
        ) AS t2 ON t1.promotion_id = t2.promotion_id
        LEFT JOIN (
            SELECT *
            FROM t_user_segment
        ) AS t3 ON TO_CHAR(TO_DATE(t1.ds, 'yyyymmdd'), 'yyyymm') = TO_CHAR(
            DATEADD(TO_DATE(t3.ds_month, 'yyyymm'), + 1, 'mm'),
            'yyyymm'
        )
        AND t1.buyer_id = t3.user_id
),
t_trn_dwd AS (
    SELECT t1.fulfillment_create_date AS fulfillment_create_date,
        t1.ds AS ds,
        TO_CHAR(t1.fulfillment_create_date, 'hh') AS hh,
        t1.sales_order_item_id AS sales_order_item_id,
        t1.order_id AS order_id,
        t1.order_number AS order_number,
        t1.check_out_id AS check_out_id,
        t1.buyer_id AS buyer_id,
        COALESCE(t3.user_contribution_tag, '05_undefined') AS user_contribution_tag,
        t1.item_status_esm AS item_status_esm,
        t1.business_type AS bu_lv1,
        t1.business_type_level2 AS bu_lv2,
        t1.industry_name AS industry,
        t1.regional_category1_name AS cat_lv1,
        t1.delivery_company AS delivery_company,
        t1.shipping_region1_name AS delivery_location,
        CASE
            WHEN COALESCE(t2.is_fsm_promo, 0) = 1
            OR COALESCE(t2.is_lpi_promo, 0) = 1 THEN 1
            ELSE 0
        END AS is_any_promo,
        COALESCE(t2.is_fsm_promo, 0) AS is_fsm_promo,
        COALESCE(t2.is_lpi_promo, 0) AS is_lpi_promo,
        COALESCE(t2.is_vcm_promo, 0) AS is_vcm_promo
    FROM (
            SELECT *
            FROM lazada_cdm.dwd_lzd_trd_core_fulfill_di
            WHERE 1 = 1 --
                -- AND     ds = 20241008 --<< Test
                -- AND     ds BETWEEN '${start_date}' AND '${end_date}' --<< Start
                AND ds >= TO_CHAR(
                    DATEADD(TO_DATE('${bizdate}', 'yyyymmdd'), 0, 'dd'),
                    'yyyymmdd'
                ) --<< Daily
                AND venture = 'VN'
                AND is_revenue = 1
                AND COALESCE(business_application, 'LZD') IN ('LZD,ZAL', 'LZD')
        ) AS t1
        LEFT JOIN (
            SELECT t1.sales_order_item_id AS sales_order_item_id,
                MAX(
                    CASE
                        WHEN t2.prom_sub_type IN ('FS Max') THEN 1
                        ELSE 0
                    END
                ) AS is_fsm_promo,
                MAX(
                    CASE
                        WHEN t1.promotion_type IN ('purchaseIncentive', 'lpiCoupon')
                        OR (
                            t1.promotion_type IN ('categoryCoupon', 'collectibleCoupon')
                            AND (
                                TOLOWER(t1.promotion_name) LIKE '%voucher%bonus%'
                                OR TOLOWER(t2.voucher_name) LIKE '%voucher%bonus%'
                                OR TOLOWER(t2.description) LIKE '%voucher%bonus%'
                            )
                        ) THEN 1
                        ELSE 0
                    END
                ) AS is_lpi_promo,
                MAX(
                    CASE
                        WHEN t1.promotion_type IN ('categoryCoupon')
                        AND (
                            TOLOWER(t1.promotion_name) LIKE '%platform_voucher max%'
                            OR TOLOWER(t1.promotion_name) LIKE '%voucher%max%'
                            OR TOLOWER(t2.voucher_name) LIKE '%platform_voucher max%'
                            OR TOLOWER(t2.voucher_name) LIKE '%voucher%max%'
                            OR TOLOWER(t2.description) LIKE '%platform_voucher max%'
                            OR TOLOWER(t2.description) LIKE '%voucher%max%'
                        ) THEN 1
                        ELSE 0
                    END
                ) AS is_vcm_promo
            FROM (
                    SELECT *
                    FROM lazada_cdm.dwd_lzd_pro_promotion_item_di
                    WHERE 1 = 1 --
                        -- AND     ds = 20241008 --<< Test
                        -- AND     ds >= TO_CHAR(DATEADD(TO_DATE('${start_date}','yyyymmdd'),-33,'dd'),'yyyymmdd') --<< Start
                        AND ds >= TO_CHAR(
                            DATEADD(TO_DATE('${bizdate}', 'yyyymmdd'), -33, 'dd'),
                            'yyyymmdd'
                        ) --<< Daily
                        AND venture = 'VN'
                        AND is_fulfilled = 1
                        AND promotion_type IN (
                            'shippingFeeCoupon',
                            'purchaseIncentive',
                            'lpiCoupon',
                            'categoryCoupon',
                            'collectibleCoupon'
                        )
                        AND TOLOWER(promotion_role) IN ('platform')
                        AND (
                            TOLOWER(retail_sponsor) IN ('platform')
                            OR retail_sponsor IS NULL
                        )
                ) AS t1
                LEFT JOIN (
                    SELECT *
                    FROM t_dim_promo
                ) AS t2 ON t1.promotion_id = t2.promotion_id
            GROUP BY t1.sales_order_item_id
        ) AS t2 ON t1.sales_order_item_id = t2.sales_order_item_id
        LEFT JOIN (
            SELECT *
            FROM t_user_segment
        ) AS t3 ON TO_CHAR(TO_DATE(t1.ds, 'yyyymmdd'), 'yyyymm') = TO_CHAR(
            DATEADD(TO_DATE(t3.ds_month, 'yyyymm'), + 1, 'mm'),
            'yyyymm'
        )
        AND t1.buyer_id = t3.user_id
),
t_trf_ads AS (
    SELECT ds,
        hh,
        user_contribution_tag,
        promotion_mechanics,
        dau
    FROM (
            SELECT ds,
                hh,
                user_contribution_tag,
                SUM(SUM(dau)) OVER (
                    PARTITION BY ds,
                    user_contribution_tag
                    ORDER BY hh ASC
                ) AS dau
            FROM (
                    SELECT t1.ds AS ds,
                        t1.hh AS hh,
                        COALESCE(t2.user_contribution_tag, '00_all') AS user_contribution_tag,
                        COUNT(DISTINCT t1.utdid) AS dau
                    FROM (
                            SELECT ds,
                                MIN(hh) AS hh,
                                utdid
                            FROM t_trf_dwd
                            GROUP BY ds,
                                utdid
                        ) AS t1
                        LEFT JOIN (
                            SELECT DISTINCT ds,
                                user_contribution_tag,
                                utdid
                            FROM t_trf_dwd
                        ) AS t2 ON t1.ds = t2.ds
                        AND t1.utdid = t2.utdid
                    GROUP BY t1.ds,
                        t1.hh,
                        CUBE(t2.user_contribution_tag)
                )
            GROUP BY ds,
                hh,
                user_contribution_tag
        ) LATERAL VIEW EXPLODE(
            SPLIT(
                '01_fsm_all_ops,02_lpi_all_ops,03_vcm_all_ops',
                ','
            )
        ) promotion_mechanics AS promotion_mechanics
),
t_clt_ads_1 AS (
    SELECT ds,
        hh,
        COALESCE(user_contribution_tag, '00_all') AS user_contribution_tag,
        promotion_mechanics,
        COUNT(DISTINCT buyer_id_hh_promo) AS collect_uv
    FROM (
            SELECT *,
                CASE
                    WHEN is_1st_clt_promo = 1 THEN user_id
                    ELSE NULL
                END AS buyer_id_hh_promo
            FROM (
                    SELECT *,
                        CASE
                            WHEN promotion_mechanics IN ('01_fsm_all_ops') THEN ROW_NUMBER() OVER (
                                PARTITION BY ds,
                                user_id,
                                promotion_mechanics IN ('01_fsm_all_ops'),
                                user_contribution_tag
                                ORDER BY collected_date ASC
                            )
                            ELSE 0
                        END AS is_1st_clt_promo
                    FROM t_clt_dwd
                    WHERE 1 = 1
                        AND promotion_mechanics IN ('01_fsm_all_ops')
                )
        )
    GROUP BY ds,
        hh,
        CUBE(user_contribution_tag),
        promotion_mechanics
    UNION ALL
    SELECT ds,
        hh,
        COALESCE(user_contribution_tag, '00_all') AS user_contribution_tag,
        promotion_mechanics,
        COUNT(DISTINCT buyer_id_hh_promo) AS collect_uv
    FROM (
            SELECT *,
                CASE
                    WHEN is_1st_clt_promo = 1 THEN user_id
                    ELSE NULL
                END AS buyer_id_hh_promo
            FROM (
                    SELECT *,
                        CASE
                            WHEN promotion_mechanics IN ('02_lpi_all_ops') THEN ROW_NUMBER() OVER (
                                PARTITION BY ds,
                                user_id,
                                promotion_mechanics IN ('02_lpi_all_ops'),
                                user_contribution_tag
                                ORDER BY collected_date ASC
                            )
                            ELSE 0
                        END AS is_1st_clt_promo
                    FROM t_clt_dwd
                    WHERE 1 = 1
                        AND promotion_mechanics IN ('02_lpi_all_ops')
                )
        )
    GROUP BY ds,
        hh,
        CUBE(user_contribution_tag),
        promotion_mechanics
    UNION ALL
    SELECT ds,
        hh,
        COALESCE(user_contribution_tag, '00_all') AS user_contribution_tag,
        promotion_mechanics,
        COUNT(DISTINCT buyer_id_hh_promo) AS collect_uv
    FROM (
            SELECT *,
                CASE
                    WHEN is_1st_clt_promo = 1 THEN user_id
                    ELSE NULL
                END AS buyer_id_hh_promo
            FROM (
                    SELECT *,
                        CASE
                            WHEN promotion_mechanics IN ('03_vcm_all_ops') THEN ROW_NUMBER() OVER (
                                PARTITION BY ds,
                                user_id,
                                promotion_mechanics IN ('03_vcm_all_ops'),
                                user_contribution_tag
                                ORDER BY collected_date ASC
                            )
                            ELSE 0
                        END AS is_1st_clt_promo
                    FROM t_clt_dwd
                    WHERE 1 = 1
                        AND promotion_mechanics IN ('03_vcm_all_ops')
                )
        )
    GROUP BY ds,
        hh,
        CUBE(user_contribution_tag),
        promotion_mechanics
),
t_clt_ads AS (
    SELECT ds,
        hh,
        user_contribution_tag,
        promotion_mechanics,
        SUM(SUM(collect_uv)) OVER (
            PARTITION BY ds,
            user_contribution_tag,
            promotion_mechanics
            ORDER BY hh ASC
        ) AS collect_uv
    FROM (
            SELECT ds,
                hh,
                user_contribution_tag,
                promotion_mechanics,
                collect_uv
            FROM t_clt_ads_1
            UNION ALL
            SELECT t1.ds AS ds,
                t2.hh AS hh,
                t1.user_contribution_tag AS user_contribution_tag,
                t1.promotion_mechanics AS promotion_mechanics,
                0 AS collect_uv
            FROM (
                    SELECT ds,
                        user_contribution_tag,
                        promotion_mechanics,
                        1 AS map_key
                    FROM t_clt_ads_1
                ) AS t1
                LEFT JOIN (
                    SELECT hh,
                        1 AS map_key
                    FROM lazada_analyst_dev.loutruong_dim_hh
                ) AS t2 ON t1.map_key = t2.map_key LEFT ANTI
                JOIN (
                    SELECT ds,
                        hh,
                        promotion_mechanics,
                        user_contribution_tag
                    FROM t_clt_ads_1
                ) AS t3 ON t1.ds = t3.ds
                AND t2.hh = t3.hh
                AND t1.promotion_mechanics = t3.promotion_mechanics
                AND t1.user_contribution_tag = t3.user_contribution_tag
        )
    GROUP BY ds,
        hh,
        user_contribution_tag,
        promotion_mechanics
),
t_trn_ads_1 AS (
    SELECT ds,
        hh,
        COALESCE(user_contribution_tag, '00_all') AS user_contribution_tag,
        '01_fsm_all_ops' AS promotion_mechanics,
        COUNT(DISTINCT buyer_id_hh_promo) AS redeem_uv,
        COUNT(DISTINCT check_out_id) AS usage
    FROM (
            SELECT *,
                CASE
                    WHEN is_1st_pur_promo = 1 THEN buyer_id
                    ELSE NULL
                END buyer_id_hh_promo
            FROM (
                    SELECT *,
                        CASE
                            WHEN is_fsm_promo = 1 THEN ROW_NUMBER() OVER (
                                PARTITION BY ds,
                                buyer_id,
                                is_fsm_promo = 1,
                                user_contribution_tag
                                ORDER BY fulfillment_create_date ASC,
                                    check_out_id ASC
                            )
                            ELSE 0
                        END AS is_1st_pur_promo
                    FROM t_trn_dwd
                    WHERE 1 = 1
                        AND is_fsm_promo = 1
                )
        )
    GROUP BY ds,
        hh,
        CUBE(user_contribution_tag),
        promotion_mechanics
    UNION ALL
    SELECT ds,
        hh,
        COALESCE(user_contribution_tag, '00_all') AS user_contribution_tag,
        '02_lpi_all_ops' AS promotion_mechanics,
        COUNT(DISTINCT buyer_id_hh_promo) AS redeem_uv,
        COUNT(DISTINCT check_out_id) AS usage
    FROM (
            SELECT *,
                CASE
                    WHEN is_1st_pur_promo = 1 THEN buyer_id
                    ELSE NULL
                END buyer_id_hh_promo
            FROM (
                    SELECT *,
                        CASE
                            WHEN is_lpi_promo = 1 THEN ROW_NUMBER() OVER (
                                PARTITION BY ds,
                                buyer_id,
                                is_lpi_promo = 1,
                                user_contribution_tag
                                ORDER BY fulfillment_create_date ASC,
                                    check_out_id ASC
                            )
                            ELSE 0
                        END AS is_1st_pur_promo
                    FROM t_trn_dwd
                    WHERE 1 = 1
                        AND is_lpi_promo = 1
                )
        )
    GROUP BY ds,
        hh,
        CUBE(user_contribution_tag),
        promotion_mechanics
    UNION ALL
    SELECT ds,
        hh,
        COALESCE(user_contribution_tag, '00_all') AS user_contribution_tag,
        '03_vcm_all_ops' AS promotion_mechanics,
        COUNT(DISTINCT buyer_id_hh_promo) AS redeem_uv,
        COUNT(DISTINCT check_out_id) AS usage
    FROM (
            SELECT *,
                CASE
                    WHEN is_1st_pur_promo = 1 THEN buyer_id
                    ELSE NULL
                END buyer_id_hh_promo
            FROM (
                    SELECT *,
                        CASE
                            WHEN is_vcm_promo = 1 THEN ROW_NUMBER() OVER (
                                PARTITION BY ds,
                                buyer_id,
                                is_vcm_promo = 1,
                                user_contribution_tag
                                ORDER BY fulfillment_create_date ASC,
                                    check_out_id ASC
                            )
                            ELSE 0
                        END AS is_1st_pur_promo
                    FROM t_trn_dwd
                    WHERE 1 = 1
                        AND is_vcm_promo = 1
                )
        )
    GROUP BY ds,
        hh,
        CUBE(user_contribution_tag),
        promotion_mechanics
),
t_trn_ads AS (
    SELECT ds,
        hh,
        user_contribution_tag,
        promotion_mechanics,
        SUM(SUM(redeem_uv)) OVER (
            PARTITION BY ds,
            user_contribution_tag,
            promotion_mechanics
            ORDER BY hh ASC
        ) AS redeem_uv,
        SUM(SUM(usage)) OVER (
            PARTITION BY ds,
            user_contribution_tag,
            promotion_mechanics
            ORDER BY hh ASC
        ) AS usage
    FROM (
            SELECT ds,
                hh,
                user_contribution_tag,
                promotion_mechanics,
                redeem_uv,
                usage
            FROM t_trn_ads_1
            UNION ALL
            SELECT t1.ds AS ds,
                t2.hh AS hh,
                t1.user_contribution_tag AS user_contribution_tag,
                t1.promotion_mechanics AS promotion_mechanics,
                0 AS redeem_uv,
                0 AS usage
            FROM (
                    SELECT ds,
                        user_contribution_tag,
                        promotion_mechanics,
                        1 AS map_key
                    FROM t_trn_ads_1
                ) AS t1
                LEFT JOIN (
                    SELECT hh,
                        1 AS map_key
                    FROM lazada_analyst_dev.loutruong_dim_hh
                ) AS t2 ON t1.map_key = t2.map_key LEFT ANTI
                JOIN (
                    SELECT ds,
                        hh,
                        promotion_mechanics,
                        user_contribution_tag
                    FROM t_trn_ads_1
                ) AS t3 ON t1.ds = t3.ds
                AND t2.hh = t3.hh
                AND t1.promotion_mechanics = t3.promotion_mechanics
                AND t1.user_contribution_tag = t3.user_contribution_tag
        )
    GROUP BY ds,
        hh,
        user_contribution_tag,
        promotion_mechanics
) --
INSERT OVERWRITE TABLE lazada_analyst_dev.loutruong_upo_ui_steering_hh PARTITION (ds, hh)
SELECT t1.user_contribution_tag AS user_contribution_tag,
    t1.promotion_mechanics AS promotion_mechanics,
    t1.dau AS dau,
    COALESCE(t2.collect_uv, 0) AS collect_uv,
    COALESCE(t2.redeem_uv, 0) AS redeem_uv,
    COALESCE(t2.usage, 0) AS usage,
    t1.ds AS ds,
    t1.hh AS hh
FROM (
        SELECT ds,
            hh,
            user_contribution_tag,
            promotion_mechanics,
            dau
        FROM t_trf_ads
    ) AS t1
    LEFT JOIN (
        SELECT ds,
            hh,
            user_contribution_tag,
            promotion_mechanics,
            SUM(collect_uv) AS collect_uv,
            SUM(redeem_uv) AS redeem_uv,
            SUM(usage) AS usage
        FROM (
                SELECT ds,
                    hh,
                    user_contribution_tag,
                    promotion_mechanics,
                    collect_uv,
                    0 AS redeem_uv,
                    0 AS usage
                FROM t_clt_ads
                UNION ALL
                SELECT ds,
                    hh,
                    user_contribution_tag,
                    promotion_mechanics,
                    0 AS collect_uv,
                    redeem_uv,
                    usage
                FROM t_trn_ads
            )
        GROUP BY ds,
            hh,
            user_contribution_tag,
            promotion_mechanics
    ) AS t2 ON t1.ds = t2.ds
    AND t1.hh = t2.hh
    AND t1.user_contribution_tag = t2.user_contribution_tag
    AND t1.promotion_mechanics = t2.promotion_mechanics
ORDER BY t1.ds ASC,
    t1.hh ASC,
    t1.user_contribution_tag ASC,
    t1.promotion_mechanics;
-------------------------------------------------------------------------------------
-- WITH t_user_segment AS 
-- (
--     SELECT  ds_month
--             ,user_id
--             ,user_contribution_tag AS user_contribution_tag_old
--             ,CASE   WHEN user_contribution_tag IN ('high') THEN '01_high'
--                     WHEN user_contribution_tag IN ('middle') THEN '02_middle'
--                     WHEN user_contribution_tag IN ('low') THEN '03_low'
--                     WHEN user_contribution_tag IN ('lto 3-') THEN '04_lto 3-'
--                     WHEN user_contribution_tag IN ('undefined') THEN '05_undefined'
--             END AS user_contribution_tag
--     FROM    lazada_ads.dws_lzd_cdp_usr_uid_contribute_value_v3
--     WHERE   1 = 1 --
--     AND     ds_month = MAX_PT('lazada_ads.dws_lzd_cdp_usr_uid_contribute_value_v3')
--     AND     venture = 'VN'
-- )
-- ,t_dim_promo AS 
-- (
--     SELECT  promotion_id
--             ,name AS voucher_name
--             ,description AS description
--             ,product_code
--             ,CASE   WHEN product_code IN ('shippingFeeCoupon') THEN 'FS Max'
--                     ELSE product_code
--             END AS prom_sub_type
--     FROM    lazada_ods.s_promotion_delta_vn_hh
--     WHERE   1 = 1
--     AND     ds = MAX_PT('lazada_cdm.dwd_lzd_pro_collectibles_hh')
--     UNION
--     SELECT  CAST(promotion_id AS STRING) AS promotion_id
--             ,voucher_name
--             ,description
--             ,product_code
--             ,prom_sub_type
--     FROM    lazada_cdm.dim_lzd_pro_collectibles
--     WHERE   1 = 1
--     AND     ds = MAX_PT('lazada_cdm.dim_lzd_pro_collectibles')
--     AND     venture = 'VN'
--     AND     sponsor = 'platform'
--     AND     (
--                 TOLOWER(retail_sponsor) IN ('platform')
--                 OR      retail_sponsor IS NULL
--     )
-- )
-- ,t_trf_dwd AS 
-- (
--     SELECT  t1.ds AS ds
--             ,t1.hh AS hh
--             ,t1.utdid AS utdid
--             ,t1.user_id AS user_id
--             ,COALESCE(t2.user_contribution_tag,'05_undefined') AS user_contribution_tag
--     FROM    (
--                 SELECT  ds
--                         ,hh
--                         ,utdid
--                         ,user_id
--                 FROM    lazada_cdm.dwd_lzd_mkt_app_ft_1d_np_hi
--                 WHERE   1 = 1
--                 AND     ds = TO_CHAR(GETDATE(),'yyyymmdd')
--                 AND     venture = 'VN'
--             ) AS t1
--     LEFT JOIN   (
--                     SELECT  *
--                     FROM    t_user_segment
--                 ) AS t2
--     ON      TO_CHAR(TO_DATE(t1.ds,'yyyymmdd'),'yyyymm') = TO_CHAR(DATEADD(TO_DATE(t2.ds_month,'yyyymm'),+1,'mm'),'yyyymm')
--     AND     t1.user_id = t2.user_id
-- )
-- ,t_clt_dwd AS 
-- (
--     SELECT  t1.collected_date AS collected_date
--             ,t1.ds AS ds
--             ,TO_CHAR(TO_DATE(t1.collected_date,'yyyy-mm-dd hh:mi:ss'),'hh') AS hh
--             ,t1.buyer_id AS user_id
--             ,COALESCE(t3.user_contribution_tag,'05_undefined') AS user_contribution_tag
--             ,t1.promotion_id AS promotion_id
--             ,t2.voucher_name AS promotion_name
--             ,t2.description AS promotion_desc
--             ,CASE   WHEN t2.prom_sub_type IN ('FS Max') THEN '01_fsm_all_ops'
--                     WHEN t2.product_code IN ('categoryCoupon','collectibleCoupon')
--                         AND (TOLOWER(t2.voucher_name) LIKE '%voucher%bonus%'
--                         OR TOLOWER(t2.description) LIKE '%voucher%bonus%') THEN '02_lpi_all_ops'
--                     WHEN t2.product_code IN ('categoryCoupon')
--                         AND (
--                                 TOLOWER(t2.voucher_name) LIKE '%platform_voucher max%'
--                                     OR TOLOWER(t2.voucher_name) LIKE '%voucher%max%'
--                                     OR TOLOWER(t2.description) LIKE '%platform_voucher max%'
--                                     OR TOLOWER(t2.description) LIKE '%voucher%max%'
--                     ) THEN '03_vcm_all_ops'
--             END AS promotion_mechanics
--     FROM    (
--                 SELECT  *
--                 FROM    lazada_cdm.dwd_lzd_pro_voucher_collect_hh
--                 WHERE   1 = 1 --
--                 AND     ds = TO_CHAR(GETDATE(),'yyyymmdd')
--                 AND     hh = (
--                             SELECT  MAX(hh)
--                             FROM    lazada_cdm.dwd_lzd_pro_voucher_collect_hh
--                             WHERE   1 = 1 --
--                             AND     ds = TO_CHAR(GETDATE(),'yyyymmdd')
--                             AND     venture = 'VN'
--                         ) 
--                 AND     venture = 'VN'
--             ) AS t1
--     LEFT JOIN   (
--                     SELECT  *
--                     FROM    t_dim_promo
--                 ) AS t2
--     ON      t1.promotion_id = t2.promotion_id
--     LEFT JOIN   (
--                     SELECT  *
--                     FROM    t_user_segment
--                 ) AS t3
--     ON      TO_CHAR(TO_DATE(t1.ds,'yyyymmdd'),'yyyymm') = TO_CHAR(DATEADD(TO_DATE(t3.ds_month,'yyyymm'),+1,'mm'),'yyyymm')
--     AND     t1.buyer_id = t3.user_id
-- )
-- ,t_trn_dwd AS 
-- (
--     SELECT  t1.fulfillment_create_date AS fulfillment_create_date
--             ,t1.ds AS ds
--             ,TO_CHAR(t1.fulfillment_create_date,'hh') AS hh
--             ,t1.sales_order_item_id AS sales_order_item_id
--             ,t1.order_id AS order_id
--             ,t1.order_number AS order_number
--             ,t1.check_out_id AS check_out_id
--             ,t1.buyer_id AS buyer_id
--             ,COALESCE(t3.user_contribution_tag,'05_undefined') AS user_contribution_tag
--             ,t1.item_status_esm AS item_status_esm
--             ,t1.business_type AS bu_lv1
--             ,t1.business_type_level2 AS bu_lv2
--             ,t1.industry_name AS industry
--             ,t1.regional_category1_name AS cat_lv1
--             ,t1.delivery_company AS delivery_company
--             ,t1.shipping_region1_name AS delivery_location
--             ,CASE   WHEN COALESCE(t2.is_fsm_promo,0) = 1
--                         OR COALESCE(t2.is_lpi_promo,0) = 1
--                         OR COALESCE(t2.is_vcm_promo,0) = 1 THEN 1
--                     ELSE 0
--             END AS is_any_promo
--             ,COALESCE(t2.is_fsm_promo,0) AS is_fsm_promo
--             ,COALESCE(t2.is_lpi_promo,0) AS is_lpi_promo
--             ,COALESCE(t2.is_vcm_promo,0) AS is_vcm_promo
--             ,(COALESCE(t2.fsm_promo_amt,0) + COALESCE(t2.lpi_promo_amt,0) + COALESCE(t2.vcm_promo_amt,0)) * t1.exchange_rate AS any_promo_amt
--             ,COALESCE(t2.fsm_promo_amt,0) * t1.exchange_rate AS fsm_promo_amt
--             ,COALESCE(t2.lpi_promo_amt,0) * t1.exchange_rate AS lpi_promo_amt
--             ,COALESCE(t2.vcm_promo_amt,0) * t1.exchange_rate AS vcm_promo_amt
--             ,t1.actual_gmv * t1.exchange_rate AS gmv
--     FROM    (
--                 SELECT  *
--                 FROM    lazada_cdm.dwd_lzd_trd_core_hh
--                 WHERE   1 = 1
--                 AND     ds = TO_CHAR(GETDATE(),'yyyymmdd')
--                 AND     TO_CHAR(fulfillment_create_date,'yyyymmdd') = TO_CHAR(GETDATE(),'yyyymmdd')
--                 AND     venture = 'VN'
--                 AND     is_revenue = 1
--                 AND     COALESCE(business_application,'LZD') IN ('LZD,ZAL','LZD')
--             ) AS t1
--     LEFT JOIN   (
--                     SELECT  t1.sales_order_item_id AS sales_order_item_id
--                             ,MAX(CASE    WHEN t2.prom_sub_type IN ('FS Max') THEN 1 ELSE 0 END) AS is_fsm_promo
--                             ,MAX(
--                                 CASE    WHEN t1.promotion_type IN ('purchaseIncentive','lpiCoupon')
--                                             OR (t1.promotion_type IN ('categoryCoupon','collectibleCoupon')
--                                             AND (TOLOWER(t1.promotion_name) LIKE '%voucher%bonus%'
--                                             OR TOLOWER(t2.voucher_name) LIKE '%voucher%bonus%'
--                                             OR TOLOWER(t2.description) LIKE '%voucher%bonus%')) THEN 1 ELSE 0 END
--                             ) AS is_lpi_promo
--                             ,MAX(
--                                 CASE    WHEN t1.promotion_type IN ('categoryCoupon')
--                                             AND (TOLOWER(t1.promotion_name) LIKE '%platform_voucher max%'
--                                             OR TOLOWER(t1.promotion_name) LIKE '%voucher%max%'
--                                             OR TOLOWER(t2.voucher_name) LIKE '%platform_voucher max%'
--                                             OR TOLOWER(t2.voucher_name) LIKE '%voucher%max%'
--                                             OR TOLOWER(t2.description) LIKE '%platform_voucher max%'
--                                             OR TOLOWER(t2.description) LIKE '%voucher%max%') THEN 1 ELSE 0 END
--                             ) AS is_vcm_promo
--                             ,SUM(CASE    WHEN t2.prom_sub_type IN ('FS Max') THEN promotion_amount ELSE 0 END) AS fsm_promo_amt
--                             ,SUM(
--                                 CASE    WHEN t1.promotion_type IN ('purchaseIncentive','lpiCoupon')
--                                             OR (t1.promotion_type IN ('categoryCoupon','collectibleCoupon')
--                                             AND (TOLOWER(t1.promotion_name) LIKE '%voucher%bonus%'
--                                             OR TOLOWER(t2.voucher_name) LIKE '%voucher%bonus%'
--                                             OR TOLOWER(t2.description) LIKE '%voucher%bonus%')) THEN promotion_amount ELSE 0 END
--                             ) AS lpi_promo_amt
--                             ,SUM(
--                                 CASE    WHEN t1.promotion_type IN ('categoryCoupon')
--                                             AND (TOLOWER(t1.promotion_name) LIKE '%platform_voucher max%'
--                                             OR TOLOWER(t1.promotion_name) LIKE '%voucher%max%'
--                                             OR TOLOWER(t2.voucher_name) LIKE '%platform_voucher max%'
--                                             OR TOLOWER(t2.voucher_name) LIKE '%voucher%max%'
--                                             OR TOLOWER(t2.description) LIKE '%platform_voucher max%'
--                                             OR TOLOWER(t2.description) LIKE '%voucher%max%') THEN promotion_amount ELSE 0 END
--                             ) AS vcm_promo_amt
--                     FROM    (
--                                 SELECT  *
--                                 FROM    lazada_cdm.dwd_lzd_pro_promotion_item_hh_vn
--                                 WHERE   1 = 1
--                                 AND     ds >= TO_CHAR(DATEADD(GETDATE(),-8,'dd'),'yyyymmdd') --<< Daily
--                                 AND     venture = 'VN'
--                                 AND     is_fulfilled = 1
--                                 AND     promotion_type IN ('shippingFeeCoupon','purchaseIncentive','lpiCoupon','categoryCoupon','collectibleCoupon')
--                                 AND     TOLOWER(promotion_role) IN ('platform')
--                                 AND     (
--                                             TOLOWER(retail_sponsor) IN ('platform')
--                                             OR      retail_sponsor IS NULL
--                                 )
--                             ) AS t1
--                     LEFT JOIN   (
--                                     SELECT  *
--                                     FROM    t_dim_promo
--                                 ) AS t2
--                     ON      t1.promotion_id = t2.promotion_id
--                     GROUP BY t1.sales_order_item_id
--                 ) AS t2
--     ON      t1.sales_order_item_id = t2.sales_order_item_id
--     LEFT JOIN   (
--                     SELECT  *
--                     FROM    t_user_segment
--                 ) AS t3
--     ON      TO_CHAR(TO_DATE(t1.ds,'yyyymmdd'),'yyyymm') = TO_CHAR(DATEADD(TO_DATE(t3.ds_month,'yyyymm'),+1,'mm'),'yyyymm')
--     AND     t1.buyer_id = t3.user_id
-- )
-- ,t_trf_ads AS 
-- (
--     SELECT  ds
--             ,hh
--             ,user_contribution_tag
--             ,promotion_mechanics
--             ,dau
--     FROM    (
--                 SELECT  ds
--                         ,hh
--                         ,user_contribution_tag
--                         ,SUM(SUM(dau)) OVER (PARTITION BY ds,user_contribution_tag ORDER BY hh ASC ) AS dau
--                 FROM    (
--                             SELECT  t1.ds AS ds
--                                     ,t1.hh AS hh
--                                     ,COALESCE(t2.user_contribution_tag,'00_all') AS user_contribution_tag
--                                     ,COUNT(DISTINCT t1.utdid) AS dau
--                             FROM    (
--                                         SELECT  ds
--                                                 ,MIN(hh) AS hh
--                                                 ,utdid
--                                         FROM    t_trf_dwd
--                                         GROUP BY ds
--                                                  ,utdid
--                                     ) AS t1
--                             LEFT JOIN   (
--                                             SELECT  DISTINCT ds
--                                                     ,user_contribution_tag
--                                                     ,utdid
--                                             FROM    t_trf_dwd
--                                         ) AS t2
--                             ON      t1.ds = t2.ds
--                             AND     t1.utdid = t2.utdid
--                             GROUP BY t1.ds
--                                      ,t1.hh
--                                      ,CUBE(t2.user_contribution_tag)
--                         ) 
--                 GROUP BY ds
--                          ,hh
--                          ,user_contribution_tag
--             ) 
--     LATERAL VIEW EXPLODE(SPLIT('01_fsm_all_ops,02_lpi_all_ops,03_vcm_all_ops',',')) promotion_mechanics AS promotion_mechanics
-- )
-- ,t_clt_ads_1 AS 
-- (
--     SELECT  ds
--             ,hh
--             ,COALESCE(user_contribution_tag,'00_all') AS user_contribution_tag
--             ,promotion_mechanics
--             ,COUNT(DISTINCT buyer_id_hh_promo) AS collect_uv
--     FROM    (
--                 SELECT  *
--                         ,CASE   WHEN is_1st_clt_promo = 1 THEN user_id
--                                 ELSE NULL
--                         END AS buyer_id_hh_promo
--                 FROM    (
--                             SELECT  *
--                                     ,CASE   WHEN promotion_mechanics IN ('01_fsm_all_ops') THEN ROW_NUMBER() OVER (PARTITION BY ds,user_id,promotion_mechanics IN ('01_fsm_all_ops'),user_contribution_tag ORDER BY collected_date ASC )
--                                             ELSE 0
--                                     END AS is_1st_clt_promo
--                             FROM    t_clt_dwd
--                             WHERE   1 = 1
--                             AND     promotion_mechanics IN ('01_fsm_all_ops')
--                         ) 
--             ) 
--     GROUP BY ds
--              ,hh
--              ,CUBE(user_contribution_tag)
--              ,promotion_mechanics
--     UNION ALL
--     SELECT  ds
--             ,hh
--             ,COALESCE(user_contribution_tag,'00_all') AS user_contribution_tag
--             ,promotion_mechanics
--             ,COUNT(DISTINCT buyer_id_hh_promo) AS collect_uv
--     FROM    (
--                 SELECT  *
--                         ,CASE   WHEN is_1st_clt_promo = 1 THEN user_id
--                                 ELSE NULL
--                         END AS buyer_id_hh_promo
--                 FROM    (
--                             SELECT  *
--                                     ,CASE   WHEN promotion_mechanics IN ('02_lpi_all_ops') THEN ROW_NUMBER() OVER (PARTITION BY ds,user_id,promotion_mechanics IN ('02_lpi_all_ops'),user_contribution_tag ORDER BY collected_date ASC )
--                                             ELSE 0
--                                     END AS is_1st_clt_promo
--                             FROM    t_clt_dwd
--                             WHERE   1 = 1
--                             AND     promotion_mechanics IN ('02_lpi_all_ops')
--                         ) 
--             ) 
--     GROUP BY ds
--              ,hh
--              ,CUBE(user_contribution_tag)
--              ,promotion_mechanics
--     UNION ALL
--     SELECT  ds
--             ,hh
--             ,COALESCE(user_contribution_tag,'00_all') AS user_contribution_tag
--             ,promotion_mechanics
--             ,COUNT(DISTINCT buyer_id_hh_promo) AS collect_uv
--     FROM    (
--                 SELECT  *
--                         ,CASE   WHEN is_1st_clt_promo = 1 THEN user_id
--                                 ELSE NULL
--                         END AS buyer_id_hh_promo
--                 FROM    (
--                             SELECT  *
--                                     ,CASE   WHEN promotion_mechanics IN ('03_vcm_all_ops') THEN ROW_NUMBER() OVER (PARTITION BY ds,user_id,promotion_mechanics IN ('03_vcm_all_ops'),user_contribution_tag ORDER BY collected_date ASC )
--                                             ELSE 0
--                                     END AS is_1st_clt_promo
--                             FROM    t_clt_dwd
--                             WHERE   1 = 1
--                             AND     promotion_mechanics IN ('03_vcm_all_ops')
--                         ) 
--             ) 
--     GROUP BY ds
--              ,hh
--              ,CUBE(user_contribution_tag)
--              ,promotion_mechanics
-- )
-- ,t_clt_ads AS 
-- (
--     SELECT  ds
--             ,hh
--             ,user_contribution_tag
--             ,promotion_mechanics
--             ,SUM(SUM(collect_uv)) OVER (PARTITION BY ds,user_contribution_tag,promotion_mechanics ORDER BY hh ASC ) AS collect_uv
--     FROM    (
--                 SELECT  ds
--                         ,hh
--                         ,user_contribution_tag
--                         ,promotion_mechanics
--                         ,collect_uv
--                 FROM    t_clt_ads_1
--                 UNION ALL
--                 SELECT  t1.ds AS ds
--                         ,t2.hh AS hh
--                         ,t1.user_contribution_tag AS user_contribution_tag
--                         ,t1.promotion_mechanics AS promotion_mechanics
--                         ,0 AS collect_uv
--                 FROM    (
--                             SELECT  ds
--                                     ,user_contribution_tag
--                                     ,promotion_mechanics
--                                     ,1 AS map_key
--                             FROM    t_clt_ads_1
--                         ) AS t1
--                 LEFT JOIN   (
--                                 SELECT  hh
--                                         ,1 AS map_key
--                                 FROM    lazada_analyst_dev.loutruong_dim_hh
--                             ) AS t2
--                 ON      t1.map_key = t2.map_key
--                 LEFT ANTI JOIN  (
--                                     SELECT  ds
--                                             ,hh
--                                             ,promotion_mechanics
--                                             ,user_contribution_tag
--                                     FROM    t_clt_ads_1
--                                 ) AS t3
--                 ON      t1.ds = t3.ds
--                 AND     t2.hh = t3.hh
--                 AND     t1.promotion_mechanics = t3.promotion_mechanics
--                 AND     t1.user_contribution_tag = t3.user_contribution_tag
--             ) 
--     GROUP BY ds
--              ,hh
--              ,user_contribution_tag
--              ,promotion_mechanics
-- )
-- ,t_trn_ads_1 AS 
-- (
--     SELECT  t1.ds AS ds
--             ,t1.hh AS hh
--             ,t1.user_contribution_tag AS user_contribution_tag
--             ,t1.promotion_mechanics AS promotion_mechanics
--             ,t1.redeem_uv AS redeem_uv
--             ,t1.usage AS usage
--             ,t1.gmv AS gmv_promo_guided
--             ,t1.order_cnt AS order_promo_guided
--             ,t1.promo_amt_fulfill AS promo_amt_fulfill
--             ,t2.buyer_cnt AS buyer_plt
--             ,t2.gmv AS gmv_plt
--             ,t2.order AS order_plt
--     FROM    (
--                 SELECT  ds
--                         ,hh
--                         ,COALESCE(user_contribution_tag,'00_all') AS user_contribution_tag
--                         ,'01_fsm_all_ops' AS promotion_mechanics
--                         ,COUNT(DISTINCT buyer_id_hh_promo) AS redeem_uv
--                         ,COUNT(DISTINCT check_out_id) AS usage
--                         ,SUM(gmv) AS gmv
--                         ,COUNT(DISTINCT order_id) AS order_cnt
--                         ,SUM(fsm_promo_amt) AS promo_amt_fulfill
--                 FROM    (
--                             SELECT  *
--                                     ,CASE   WHEN is_1st_pur_promo = 1 THEN buyer_id
--                                             ELSE NULL
--                                     END buyer_id_hh_promo
--                             FROM    (
--                                         SELECT  *
--                                                 ,CASE   WHEN is_fsm_promo = 1 THEN ROW_NUMBER() OVER (PARTITION BY ds,buyer_id,is_fsm_promo = 1,user_contribution_tag ORDER BY fulfillment_create_date ASC,check_out_id ASC )
--                                                         ELSE 0
--                                                 END AS is_1st_pur_promo
--                                         FROM    t_trn_dwd
--                                         WHERE   1 = 1
--                                         AND     is_fsm_promo = 1
--                                     ) 
--                         ) 
--                 GROUP BY ds
--                          ,hh
--                          ,CUBE(user_contribution_tag)
--                          ,promotion_mechanics
--                 UNION ALL
--                 SELECT  ds
--                         ,hh
--                         ,COALESCE(user_contribution_tag,'00_all') AS user_contribution_tag
--                         ,'02_lpi_all_ops' AS promotion_mechanics
--                         ,COUNT(DISTINCT buyer_id_hh_promo) AS redeem_uv
--                         ,COUNT(DISTINCT check_out_id) AS usage
--                         ,SUM(gmv) AS gmv
--                         ,COUNT(DISTINCT order_id) AS order_cnt
--                         ,SUM(lpi_promo_amt) AS promo_amt_fulfill
--                 FROM    (
--                             SELECT  *
--                                     ,CASE   WHEN is_1st_pur_promo = 1 THEN buyer_id
--                                             ELSE NULL
--                                     END buyer_id_hh_promo
--                             FROM    (
--                                         SELECT  *
--                                                 ,CASE   WHEN is_lpi_promo = 1 THEN ROW_NUMBER() OVER (PARTITION BY ds,buyer_id,is_lpi_promo = 1,user_contribution_tag ORDER BY fulfillment_create_date ASC,check_out_id ASC )
--                                                         ELSE 0
--                                                 END AS is_1st_pur_promo
--                                         FROM    t_trn_dwd
--                                         WHERE   1 = 1
--                                         AND     is_lpi_promo = 1
--                                     ) 
--                         ) 
--                 GROUP BY ds
--                          ,hh
--                          ,CUBE(user_contribution_tag)
--                          ,promotion_mechanics
--                 UNION ALL
--                 SELECT  ds
--                         ,hh
--                         ,COALESCE(user_contribution_tag,'00_all') AS user_contribution_tag
--                         ,'03_vcm_all_ops' AS promotion_mechanics
--                         ,COUNT(DISTINCT buyer_id_hh_promo) AS redeem_uv
--                         ,COUNT(DISTINCT check_out_id) AS usage
--                         ,SUM(gmv) AS gmv
--                         ,COUNT(DISTINCT order_id) AS order_cnt
--                         ,SUM(vcm_promo_amt) AS promo_amt_fulfill
--                 FROM    (
--                             SELECT  *
--                                     ,CASE   WHEN is_1st_pur_promo = 1 THEN buyer_id
--                                             ELSE NULL
--                                     END buyer_id_hh_promo
--                             FROM    (
--                                         SELECT  *
--                                                 ,CASE   WHEN is_vcm_promo = 1 THEN ROW_NUMBER() OVER (PARTITION BY ds,buyer_id,is_vcm_promo = 1,user_contribution_tag ORDER BY fulfillment_create_date ASC,check_out_id ASC )
--                                                         ELSE 0
--                                                 END AS is_1st_pur_promo
--                                         FROM    t_trn_dwd
--                                         WHERE   1 = 1
--                                         AND     is_vcm_promo = 1
--                                     ) 
--                         ) 
--                 GROUP BY ds
--                          ,hh
--                          ,CUBE(user_contribution_tag)
--                          ,promotion_mechanics
--             ) AS t1
--     LEFT JOIN   (
--                     SELECT  ds
--                             ,hh
--                             ,user_contribution_tag
--                             ,promotion_mechanics
--                             ,buyer_cnt
--                             ,order_cnt
--                             ,gmv
--                     FROM    (
--                                 SELECT  ds
--                                         ,hh
--                                         ,COALESCE(user_contribution_tag,'00_all') AS user_contribution_tag
--                                         ,COUNT(DISTINCT buyer_id_hh) AS buyer_cnt
--                                         ,COUNT(DISTINCT order_id) AS order_cnt
--                                         ,SUM(gmv) AS gmv
--                                 FROM    (
--                                             SELECT  *
--                                                     ,CASE   WHEN is_1st_pur = 1 THEN buyer_id
--                                                             ELSE NULL
--                                                     END buyer_id_hh
--                                             FROM    (
--                                                         SELECT  *
--                                                                 ,ROW_NUMBER() OVER (PARTITION BY ds,buyer_id,user_contribution_tag ORDER BY fulfillment_create_date ASC,check_out_id ASC ) AS is_1st_pur
--                                                         FROM    t_trn_dwd
--                                                     ) 
--                                         ) 
--                                 GROUP BY ds
--                                          ,hh
--                                          ,CUBE(user_contribution_tag)
--                             ) 
--                     LATERAL VIEW EXPLODE(SPLIT('00_all,01_fsm_all_ops,02_lpi_all_ops,03_vcm_all_ops',',')) promotion_mechanics AS promotion_mechanics
--                 ) AS t2
--     ON      t1.ds = t2.ds
--     AND     t1.hh = t2.hh
--     AND     t1.user_contribution_tag = t2.user_contribution_tag
--     AND     t1.promotion_mechanics = t2.promotion_mechanics
-- )
-- ,t_trn_ads AS 
-- (
--     SELECT  ds
--             ,hh
--             ,user_contribution_tag
--             ,promotion_mechanics
--             ,SUM(SUM(redeem_uv)) OVER (PARTITION BY ds,user_contribution_tag,promotion_mechanics ORDER BY hh ASC ) AS redeem_uv
--             ,SUM(SUM(usage)) OVER (PARTITION BY ds,user_contribution_tag,promotion_mechanics ORDER BY hh ASC ) AS usage
--     FROM    (
--                 SELECT  ds
--                         ,hh
--                         ,user_contribution_tag
--                         ,promotion_mechanics
--                         ,redeem_uv
--                         ,usage
--                 FROM    t_trn_ads_1
--                 UNION ALL
--                 SELECT  t1.ds AS ds
--                         ,t2.hh AS hh
--                         ,t1.user_contribution_tag AS user_contribution_tag
--                         ,t1.promotion_mechanics AS promotion_mechanics
--                         ,0 AS redeem_uv
--                         ,0 AS usage
--                 FROM    (
--                             SELECT  ds
--                                     ,user_contribution_tag
--                                     ,promotion_mechanics
--                                     ,1 AS map_key
--                             FROM    t_trn_ads_1
--                         ) AS t1
--                 LEFT JOIN   (
--                                 SELECT  hh
--                                         ,1 AS map_key
--                                 FROM    lazada_analyst_dev.loutruong_dim_hh
--                             ) AS t2
--                 ON      t1.map_key = t2.map_key
--                 LEFT ANTI JOIN  (
--                                     SELECT  ds
--                                             ,hh
--                                             ,promotion_mechanics
--                                             ,user_contribution_tag
--                                     FROM    t_trn_ads_1
--                                 ) AS t3
--                 ON      t1.ds = t3.ds
--                 AND     t2.hh = t3.hh
--                 AND     t1.promotion_mechanics = t3.promotion_mechanics
--                 AND     t1.user_contribution_tag = t3.user_contribution_tag
--             ) 
--     GROUP BY ds
--              ,hh
--              ,user_contribution_tag
--              ,promotion_mechanics
-- )
-- SELECT  t1.ds AS ds
--         ,t1.hh AS hh
--         ,t1.user_contribution_tag AS user_contribution_tag
--         ,t1.promotion_mechanics AS promotion_mechanics
--         ,t1.dau AS dau
--         ,COALESCE(t2.collect_uv,0) AS collect_uv
--         ,COALESCE(t2.redeem_uv,0) AS redeem_uv
--         ,COALESCE(t2.usage,0) AS usage
-- FROM    (
--             SELECT  ds
--                     ,hh
--                     ,user_contribution_tag
--                     ,promotion_mechanics
--                     ,dau
--             FROM    t_trf_ads
--         ) AS t1
-- LEFT JOIN   (
--                 SELECT  ds
--                         ,hh
--                         ,user_contribution_tag
--                         ,promotion_mechanics
--                         ,SUM(collect_uv) AS collect_uv
--                         ,SUM(redeem_uv) AS redeem_uv
--                         ,SUM(usage) AS usage
--                 FROM    (
--                             SELECT  ds
--                                     ,hh
--                                     ,user_contribution_tag
--                                     ,promotion_mechanics
--                                     ,collect_uv
--                                     ,0 AS redeem_uv
--                                     ,0 AS usage
--                             FROM    t_clt_ads
--                             UNION ALL
--                             SELECT  ds
--                                     ,hh
--                                     ,user_contribution_tag
--                                     ,promotion_mechanics
--                                     ,0 AS collect_uv
--                                     ,redeem_uv
--                                     ,usage
--                             FROM    t_trn_ads
--                         ) 
--                 GROUP BY ds
--                          ,hh
--                          ,user_contribution_tag
--                          ,promotion_mechanics
--             ) AS t2
-- ON      t1.ds = t2.ds
-- AND     t1.hh = t2.hh
-- AND     t1.user_contribution_tag = t2.user_contribution_tag
-- AND     t1.promotion_mechanics = t2.promotion_mechanics
-- ;
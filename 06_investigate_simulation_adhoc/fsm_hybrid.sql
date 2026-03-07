-- MaxCompute SQL 
-- ********************************************************************--
-- author:Truong, Van Thanh
-- create time:2024-08-26 15:54:26
-- ********************************************************************--
--@@ Input = lazada_cdm.dim_lzd_usr
--@@ Input = lazada_ab.dim_lzd_ab_users_d (15/8 ==>> 31/8 (Missing 19 to 25): 1241607, 1251580 / 15/7 ==>> 31/7: 1234378)
--@@ Input = lazada_analyst_dev.loutruong_dim_ab_di
--@@ Input = lazada_cdm.dwd_lzd_pro_voucher_collect_di
--@@ Input = lazada_ads.dws_lzd_cdp_usr_uid_contribute_value_v3 (202406 ==>> 202407 / 202407 ==>> 202408)
--@@ Input = lazada_ads.ads_lzd_asset_usr_uid_prd_ipv_1d
--@@ Input = lazada_cdm.dwd_lzd_trd_core_fulfill_di
--@@ Input = lazada_cdm.dim_lzd_pro_collectibles
--@@ Input = lazada_cdm.dwd_lzd_pro_promotion_item_di
--@@ Input = lazada_ent_cdm.dwd_lzd_fin_trd_commission_di
-- DROP TABLE IF EXISTS lazada_analyst_dev.tmp_loutruong_fsm_hybrid
-- ;
-- CREATE TABLE IF NOT EXISTS lazada_analyst_dev.tmp_loutruong_fsm_hybrid
-- LIFECYCLE 180 AS
WITH t_dim_usr AS (
    SELECT *
    FROM lazada_cdm.dim_lzd_usr
    WHERE 1 = 1
        AND ds = 20241010 --<< Fix the list
        AND venture = 'VN'
),
t_dim_usr_seg AS (
    SELECT ds_month,
        user_id,
CASE
            WHEN user_contribution_tag IN ('high') THEN '01_high'
            WHEN user_contribution_tag IN ('middle') THEN '02_middle'
            WHEN user_contribution_tag IN ('low') THEN '03_low'
            WHEN user_contribution_tag IN ('lto 3-') THEN '04_lto 3-'
            WHEN user_contribution_tag IN ('undefined') THEN '05_undefined'
        END AS user_contribution_tag
    FROM lazada_ads.dws_lzd_cdp_usr_uid_contribute_value_v3
    WHERE 1 = 1
        AND ds_month = 202408
        AND venture = 'VN'
),
t_dim_promo AS (
    SELECT *
    FROM lazada_cdm.dim_lzd_pro_collectibles
    WHERE 1 = 1
        AND ds = MAX_PT('lazada_cdm.dim_lzd_pro_collectibles')
        AND venture = 'VN'
        AND prom_sub_type IN ('FS Max')
),
t_dim_usr_dis_type AS (
    SELECT user_id,
        MAX(
            CASE
                WHEN ds BETWEEN 20240715 AND 20240731
                AND bucket_id IN (1234378) THEN 'Normal'
                ELSE NULL
            END
        ) AS status_fsm_dis_jul,
        MAX(
            CASE
                WHEN ds BETWEEN 20240815 AND 20240831
                AND bucket_id IN (1241607, 1251580) THEN 'Hybrid'
                ELSE NULL
            END
        ) AS status_fsm_dis_aug
    FROM lazada_analyst_dev.loutruong_dim_ab_di
    WHERE 1 = 1
        AND (
            ds BETWEEN 20240815 AND 20240831
            OR ds BETWEEN 20240715 AND 20240731
        )
        AND venture = 'VN'
        AND bucket_id IN (1234378, 1241607, 1251580)
    GROUP BY user_id
),
t_usr_fsm_clt AS (
    SELECT t1.buyer_id AS user_id,
        MAX(
            CASE
                WHEN t1.ds BETWEEN 20240715 AND 20240731
                AND t2.promotion_id IS NOT NULL THEN 1
                ELSE 0
            END
        ) AS is_fsm_voh_jul,
        MAX(
            CASE
                WHEN t1.ds BETWEEN 20240815 AND 20240831
                AND t2.promotion_id IS NOT NULL THEN 1
                ELSE 0
            END
        ) AS is_fsm_voh_aug
    FROM (
            SELECT *
            FROM lazada_cdm.dwd_lzd_pro_voucher_collect_di
            WHERE 1 = 1
                AND (
                    ds BETWEEN 20240715 AND 20240731
                    OR ds BETWEEN 20240815 AND 20240831
                )
                AND venture = 'VN'
        ) AS t1
        LEFT JOIN (
            SELECT *
            FROM t_dim_promo
        ) AS t2 ON t1.promotion_id = t2.promotion_id
    GROUP BY t1.buyer_id
),
t_usr_ipv AS (
    SELECT CASE
            WHEN ds BETWEEN 20240715 AND 20240731 THEN 'Data range: 20240715 ==>> 20240731'
            WHEN ds BETWEEN 20240815 AND 20240831 THEN 'Data range: 20240815 ==>> 20240831'
        END AS duration,
        user_id,
        SUM(ipv) AS pdp_pv
    FROM lazada_ads.ads_lzd_asset_usr_uid_prd_ipv_1d
    WHERE 1 = 1
        AND (
            ds BETWEEN 20240715 AND 20240731
            OR ds BETWEEN 20240815 AND 20240831
        )
        AND venture = 'VN'
    GROUP BY CASE
            WHEN ds BETWEEN 20240715 AND 20240731 THEN 'Data range: 20240715 ==>> 20240731'
            WHEN ds BETWEEN 20240815 AND 20240831 THEN 'Data range: 20240815 ==>> 20240831'
        END,
        user_id
),
t_trd_core AS (
    SELECT CASE
            WHEN ds BETWEEN 20240715 AND 20240731 THEN 'Data range: 20240715 ==>> 20240731'
            WHEN ds BETWEEN 20240815 AND 20240831 THEN 'Data range: 20240815 ==>> 20240831'
        END AS duration,
        buyer_id AS user_id,
        COUNT(DISTINCT order_id) AS order_sku_cnt,
        COUNT(DISTINCT order_number) AS order_slr_cnt,
        SUM(actual_gmv * exchange_rate) AS gmv,
        COUNT(
            DISTINCT CASE
                WHEN TOLOWER(business_type_level2) NOT LIKE '%choice%'
                AND TOLOWER(business_type_level2) NOT LIKE '%ae%' THEN order_id
                ELSE NULL
            END
        ) AS order_sku_exc_ac_ae_cnt,
        COUNT(
            DISTINCT CASE
                WHEN TOLOWER(business_type_level2) NOT LIKE '%choice%'
                AND TOLOWER(business_type_level2) NOT LIKE '%ae%' THEN order_number
                ELSE NULL
            END
        ) AS order_slr_exc_ac_ae_cnt,
        SUM(
            DISTINCT CASE
                WHEN TOLOWER(business_type_level2) NOT LIKE '%choice%'
                AND TOLOWER(business_type_level2) NOT LIKE '%ae%' THEN actual_gmv * exchange_rate
                ELSE NULL
            END
        ) AS gmv_exc_ac_ae_cnt,
        COUNT(
            DISTINCT CASE
                WHEN COALESCE(is_fsm, 0) = 1 THEN order_id
                ELSE NULL
            END
        ) AS order_sku_fsm_gui_cnt,
        COUNT(
            DISTINCT CASE
                WHEN COALESCE(is_fsm, 0) = 1 THEN order_number
                ELSE NULL
            END
        ) AS order_slr_fsm_gui_cnt,
        SUM(
            CASE
                WHEN COALESCE(is_fsm, 0) = 1 THEN actual_gmv * exchange_rate
                ELSE 0
            END
        ) AS gmv_fsm_gui_cnt,
        SUM(COALESCE(total_promo_amt, 0)) AS total_promo_amt,
        SUM(COALESCE(fsm_promo_amt, 0)) AS fsm_promo_amt,
        SUM(COALESCE(lpi_promo_amt, 0)) AS lpi_promo_amt
    FROM (
            SELECT t1.*,
                COALESCE(t2.is_fsm, 0) AS is_fsm,
                COALESCE(t2.is_lpi, 0) AS is_lpi,
                COALESCE(t2.total_promo_amt, 0) * t1.exchange_rate AS total_promo_amt,
                COALESCE(t2.fsm_promo_amt, 0) * t1.exchange_rate AS fsm_promo_amt,
                COALESCE(t2.lpi_promo_amt, 0) * t1.exchange_rate AS lpi_promo_amt
            FROM (
                    SELECT *
                    FROM lazada_cdm.dwd_lzd_trd_core_fulfill_di
                    WHERE 1 = 1
                        AND (
                            ds BETWEEN 20240715 AND 20240731
                            OR ds BETWEEN 20240815 AND 20240831
                        )
                        AND venture = 'VN'
                        AND is_revenue = 1
                        AND COALESCE(business_application, 'LZD') IN ('LZD,ZAL', 'LZD')
                ) AS t1
                LEFT JOIN (
                    SELECT t1.sales_order_item_id AS sales_order_item_id,
                        MAX(
                            CASE
                                WHEN t2.prom_sub_type IN ('FS Max')
                                AND TOLOWER(t1.promotion_role) IN ('platform')
                                AND (
                                    TOLOWER(t1.retail_sponsor) IN ('platform')
                                    OR t1.retail_sponsor IS NULL
                                )
                                AND t1.promotion_type IN (
                                    'shippingFeeCoupon',
                                    'purchaseIncentive',
                                    'lpiCoupon',
                                    'categoryCoupon',
                                    'collectibleCoupon'
                                ) THEN 1
                                ELSE 0
                            END
                        ) AS is_fsm,
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
                                )
                                AND TOLOWER(t1.promotion_role) IN ('platform')
                                AND (
                                    TOLOWER(t1.retail_sponsor) IN ('platform')
                                    OR t1.retail_sponsor IS NULL
                                )
                                AND t1.promotion_type IN (
                                    'shippingFeeCoupon',
                                    'purchaseIncentive',
                                    'lpiCoupon',
                                    'categoryCoupon',
                                    'collectibleCoupon'
                                ) THEN 1
                                ELSE 0
                            END
                        ) AS is_lpi,
                        SUM(t1.promotion_amount) AS total_promo_amt,
                        SUM(
                            CASE
                                WHEN t2.prom_sub_type IN ('FS Max')
                                AND TOLOWER(t1.promotion_role) IN ('platform')
                                AND (
                                    TOLOWER(t1.retail_sponsor) IN ('platform')
                                    OR t1.retail_sponsor IS NULL
                                )
                                AND t1.promotion_type IN (
                                    'shippingFeeCoupon',
                                    'purchaseIncentive',
                                    'lpiCoupon',
                                    'categoryCoupon',
                                    'collectibleCoupon'
                                ) THEN promotion_amount
                                ELSE 0
                            END
                        ) AS fsm_promo_amt,
                        SUM(
                            CASE
                                WHEN t1.promotion_type IN ('purchaseIncentive', 'lpiCoupon')
                                OR (
                                    t1.promotion_type IN ('categoryCoupon', 'collectibleCoupon')
                                    AND (
                                        TOLOWER(t1.promotion_name) LIKE '%voucher%bonus%'
                                        OR TOLOWER(t2.voucher_name) LIKE '%voucher%bonus%'
                                        OR TOLOWER(t2.description) LIKE '%voucher%bonus%'
                                    )
                                )
                                AND TOLOWER(t1.promotion_role) IN ('platform')
                                AND (
                                    TOLOWER(t1.retail_sponsor) IN ('platform')
                                    OR t1.retail_sponsor IS NULL
                                )
                                AND t1.promotion_type IN (
                                    'shippingFeeCoupon',
                                    'purchaseIncentive',
                                    'lpiCoupon',
                                    'categoryCoupon',
                                    'collectibleCoupon'
                                ) THEN promotion_amount
                                ELSE 0
                            END
                        ) AS lpi_promo_amt
                    FROM (
                            SELECT *
                            FROM lazada_cdm.dwd_lzd_pro_promotion_item_di
                            WHERE 1 = 1
                                AND (
                                    ds BETWEEN 20240710 AND 20240807
                                    OR ds BETWEEN 20240810 AND 20240907
                                )
                                AND venture = 'VN'
                                AND is_fulfilled = 1
                        ) AS t1
                        LEFT JOIN (
                            SELECT *
                            FROM lazada_cdm.dim_lzd_pro_collectibles
                            WHERE 1 = 1
                                AND ds = MAX_PT('lazada_cdm.dim_lzd_pro_collectibles')
                                AND venture = 'VN'
                        ) AS t2 ON t1.promotion_id = t2.promotion_id
                    GROUP BY t1.sales_order_item_id
                ) AS t2 ON t1.sales_order_item_id = t2.sales_order_item_id
        )
    GROUP BY CASE
            WHEN ds BETWEEN 20240715 AND 20240731 THEN 'Data range: 20240715 ==>> 20240731'
            WHEN ds BETWEEN 20240815 AND 20240831 THEN 'Data range: 20240815 ==>> 20240831'
        END,
        buyer_id
)
SELECT t1.duration AS data_range,
    t1.user_id AS user_id,
CASE
        WHEN COALESCE(t3.order_sku_cnt, 0) > 0 THEN 'Buyer'
        ELSE 'Non-Buyer'
    END AS user_type_1,
    COALESCE(t1.user_type, 'Unknown') AS user_type_2,
    t1.status_fsm_dis_jul AS status_fsm_dis_jul,
    t1.status_fsm_dis_aug AS status_fsm_dis_aug,
    t1.is_fsm_voh_jul AS is_fsm_voh_jul,
    t1.is_fsm_voh_aug AS is_fsm_voh_aug,
    t1.user_contribution_tag AS user_contribution_tag,
    COALESCE(t2.pdp_pv, 0) AS pdp_pv,
    COALESCE(t3.order_sku_cnt, 0) AS order_sku_cnt,
    COALESCE(t3.order_slr_cnt, 0) AS order_slr_cnt,
    COALESCE(t3.gmv, 0) AS gmv,
    COALESCE(t3.order_sku_exc_ac_ae_cnt, 0) AS order_sku_exc_ac_ae_cnt,
    COALESCE(t3.order_slr_exc_ac_ae_cnt, 0) AS order_slr_exc_ac_ae_cnt,
    COALESCE(t3.gmv_exc_ac_ae_cnt, 0) AS gmv_exc_ac_ae_cnt,
    COALESCE(t3.order_sku_fsm_gui_cnt, 0) AS order_sku_fsm_gui_cnt,
    COALESCE(t3.order_slr_fsm_gui_cnt, 0) AS order_slr_fsm_gui_cnt,
    COALESCE(t3.gmv_fsm_gui_cnt, 0) AS gmv_fsm_gui_cnt,
    COALESCE(t3.total_promo_amt, 0) AS total_promo_amt,
    COALESCE(t3.fsm_promo_amt, 0) AS fsm_promo_amt,
    COALESCE(t3.lpi_promo_amt, 0) AS lpi_promo_amt,
    ROW_NUMBER() OVER (
        PARTITION BY t1.duration
        ORDER BY t1.user_id ASC
    ) AS r
FROM (
        SELECT duration,
            user_id,
            user_type,
            status_fsm_dis_jul,
            status_fsm_dis_aug,
            is_fsm_voh_jul,
            is_fsm_voh_aug,
            user_contribution_tag
        FROM (
                SELECT t1.user_id AS user_id,
CASE
                        WHEN t2.status_fsm_dis_jul = 'Normal'
                        AND t2.status_fsm_dis_aug = 'Hybrid'
                        AND COALESCE(t3.is_fsm_voh_jul, 0) = 0
                        AND COALESCE(t3.is_fsm_voh_aug, 0) = 1 THEN 'Group 1'
                        WHEN t2.status_fsm_dis_jul = 'Normal'
                        AND t2.status_fsm_dis_aug = 'Hybrid'
                        AND COALESCE(t3.is_fsm_voh_jul, 0) = 1
                        AND COALESCE(t3.is_fsm_voh_aug, 0) = 1 THEN 'Group 2'
                        ELSE NULL
                    END AS user_type,
                    t2.status_fsm_dis_jul AS status_fsm_dis_jul,
                    t2.status_fsm_dis_aug AS status_fsm_dis_aug,
                    COALESCE(t3.is_fsm_voh_jul, 0) AS is_fsm_voh_jul,
                    COALESCE(t3.is_fsm_voh_aug, 0) AS is_fsm_voh_aug,
                    COALESCE(t4.user_contribution_tag, '05_undefined') AS user_contribution_tag
                FROM (
                        SELECT *
                        FROM t_dim_usr
                    ) AS t1
                    LEFT JOIN (
                        SELECT *
                        FROM t_dim_usr_dis_type
                    ) AS t2 ON t1.user_id = t2.user_id
                    LEFT JOIN (
                        SELECT *
                        FROM t_usr_fsm_clt
                    ) AS t3 ON t1.user_id = t3.user_id
                    LEFT JOIN (
                        SELECT *
                        FROM t_dim_usr_seg
                    ) AS t4 ON t1.user_id = t4.user_id
            ) LATERAL VIEW EXPLODE(
                SPLIT(
                    'Data range: 20240715 ==>> 20240731,Data range: 20240815 ==>> 20240831',
                    ','
                )
            ) duration AS duration
    ) AS t1
    LEFT JOIN (
        SELECT *
        FROM t_usr_ipv
    ) AS t2 ON t1.duration = t2.duration
    AND t1.user_id = t2.user_id
    LEFT JOIN (
        SELECT *
        FROM t_trd_core
    ) AS t3 ON t1.duration = t3.duration
    AND t1.user_id = t3.user_id
ORDER BY t1.user_type DESC,
    t1.user_id DESC,
    t1.duration ASC;
WITH t_t_zero AS (
    SELECT *,
CASE
            WHEN z_score BETWEEN lower_bound_1 AND upper_bound_1 THEN 1
            ELSE 0
        END AS is_in_bound_1
    FROM (
            SELECT *,
(order_sku_cnt - AVG(order_sku_cnt) OVER ()) / STDDEV(order_sku_cnt) OVER () AS z_score,
                AVG(order_sku_cnt) OVER () AS avg_order_sku_cnt,
                STDDEV(order_sku_cnt) OVER () AS std_order_sku_cnt,
                AVG(order_sku_cnt) OVER () - STDDEV(order_sku_cnt) OVER () AS lower_bound_1,
                AVG(order_sku_cnt) OVER () + STDDEV(order_sku_cnt) OVER () AS upper_bound_1,
                AVG(order_sku_cnt) OVER () - (
                    STDDEV(order_sku_cnt) OVER ()
                ) * 2 AS lower_bound_2,
                AVG(order_sku_cnt) OVER () + (
                    STDDEV(order_sku_cnt) OVER ()
                ) * 2 AS upper_bound_2,
                AVG(order_sku_cnt) OVER () - (
                    STDDEV(order_sku_cnt) OVER ()
                ) * 3 AS lower_bound_3,
                AVG(order_sku_cnt) OVER () + (
                    STDDEV(order_sku_cnt) OVER ()
                ) * 3 AS upper_bound_3
            FROM lazada_analyst_dev.tmp_loutruong_fsm_hybrid
            WHERE 1 = 1
                AND user_type_2 <> 'Unknown'
                AND data_range IN ('Data range: 20240815 ==>> 20240831')
        )
),
t_t_minus_1 AS (
    SELECT *,
CASE
            WHEN z_score BETWEEN lower_bound_1 AND upper_bound_1 THEN 1
            ELSE 0
        END AS is_in_bound_1
    FROM (
            SELECT *,
(order_sku_cnt - AVG(order_sku_cnt) OVER ()) / STDDEV(order_sku_cnt) OVER () AS z_score,
                AVG(order_sku_cnt) OVER () AS avg_order_sku_cnt,
                STDDEV(order_sku_cnt) OVER () AS std_order_sku_cnt,
                AVG(order_sku_cnt) OVER () - STDDEV(order_sku_cnt) OVER () AS lower_bound_1,
                AVG(order_sku_cnt) OVER () + STDDEV(order_sku_cnt) OVER () AS upper_bound_1,
                AVG(order_sku_cnt) OVER () - (
                    STDDEV(order_sku_cnt) OVER ()
                ) * 2 AS lower_bound_2,
                AVG(order_sku_cnt) OVER () + (
                    STDDEV(order_sku_cnt) OVER ()
                ) * 2 AS upper_bound_2,
                AVG(order_sku_cnt) OVER () - (
                    STDDEV(order_sku_cnt) OVER ()
                ) * 3 AS lower_bound_3,
                AVG(order_sku_cnt) OVER () + (
                    STDDEV(order_sku_cnt) OVER ()
                ) * 3 AS upper_bound_3
            FROM lazada_analyst_dev.tmp_loutruong_fsm_hybrid
            WHERE 1 = 1
                AND user_type_2 <> 'Unknown'
                AND data_range IN ('Data range: 20240715 ==>> 20240731')
        )
)
SELECT *,
    ROW_NUMBER() OVER (
        PARTITION BY data_range
        ORDER BY user_id ASC
    ) AS rn
FROM (
        SELECT *,
            MAX(cum_in_bound_1) OVER (PARTITION BY user_id) AS number_data_set
        FROM (
                SELECT *,
                    SUM(is_in_bound_1) OVER (
                        PARTITION BY user_id
                        ORDER BY data_range ASC
                    ) AS cum_in_bound_1
                FROM (
                        SELECT *
                        FROM t_t_zero
                        UNION ALL
                        SELECT *
                        FROM t_t_minus_1
                    )
            )
    );
SELECT t1.duration AS duration,
CASE
        WHEN COALESCE(t2.order_sku_cnt, 0) > 0 THEN 'Buyer'
        ELSE 'Non-Buyer'
    END AS user_type,
    t1.user_contribution_tag AS user_contribution_tag,
    COUNT(DISTINCT t1.user_id) AS usr_cnt
FROM (
        SELECT CASE
                WHEN ds_month = 202407 THEN 'Data range: 20240715 ==>> 20240731'
                WHEN ds_month = 202408 THEN 'Data range: 20240815 ==>> 20240831'
            END AS duration,
            user_id,
CASE
                WHEN user_contribution_tag IN ('high') THEN '01_high'
                WHEN user_contribution_tag IN ('middle') THEN '02_middle'
                WHEN user_contribution_tag IN ('low') THEN '03_low'
                WHEN user_contribution_tag IN ('lto 3-') THEN '04_lto 3-'
                WHEN user_contribution_tag IN ('undefined') THEN '05_undefined'
            END AS user_contribution_tag
        FROM lazada_ads.dws_lzd_cdp_usr_uid_contribute_value_v3
        WHERE 1 = 1
            AND ds_month IN (202407, 202408)
            AND venture = 'VN'
    ) AS t1
    LEFT JOIN (
        SELECT CASE
                WHEN ds BETWEEN 20240715 AND 20240731 THEN 'Data range: 20240715 ==>> 20240731'
                WHEN ds BETWEEN 20240815 AND 20240831 THEN 'Data range: 20240815 ==>> 20240831'
            END AS duration,
            buyer_id,
            COUNT(DISTINCT order_id) AS order_sku_cnt
        FROM lazada_cdm.dwd_lzd_trd_core_fulfill_di
        WHERE 1 = 1
            AND (
                ds BETWEEN 20240715 AND 20240731
                OR ds BETWEEN 20240815 AND 20240831
            )
            AND venture = 'VN'
            AND is_revenue = 1
            AND COALESCE(business_application, 'LZD') IN ('LZD,ZAL', 'LZD')
        GROUP BY CASE
                WHEN ds BETWEEN 20240715 AND 20240731 THEN 'Data range: 20240715 ==>> 20240731'
                WHEN ds BETWEEN 20240815 AND 20240831 THEN 'Data range: 20240815 ==>> 20240831'
            END,
            buyer_id
    ) AS t2 ON t1.duration = t2.duration
    AND t1.user_id = t2.buyer_id
GROUP BY t1.duration,
CASE
        WHEN COALESCE(t2.order_sku_cnt, 0) > 0 THEN 'Buyer'
        ELSE 'Non-Buyer'
    END,
    t1.user_contribution_tag;
SELECT t1.duration AS duration,
CASE
        WHEN COALESCE(t3.order_sku_cnt, 0) > 0 THEN 'Buyer'
        ELSE 'Non-Buyer'
    END AS user_type,
    COALESCE(t2.user_contribution_tag, '05_undefined') AS user_contribution_tag,
    COUNT(DISTINCT t1.user_id) AS usr_cnt
FROM (
        SELECT DISTINCT CASE
                WHEN ds BETWEEN 20240715 AND 20240731 THEN 'Data range: 20240715 ==>> 20240731'
                WHEN ds BETWEEN 20240815 AND 20240831 THEN 'Data range: 20240815 ==>> 20240831'
            END AS duration,
            utdid,
            user_id
        FROM lazada_cdm.dws_lzd_mkt_app_ad_mp_di
        WHERE 1 = 1 --
            AND (
                ds BETWEEN 20240715 AND 20240731
                OR ds BETWEEN 20240815 AND 20240831
            )
            AND venture = 'VN'
            AND attr_model IN ('ft_1d_np')
            AND utdid IS NOT NULL
    ) AS t1
    LEFT JOIN (
        SELECT ds_month,
            user_id,
CASE
                WHEN user_contribution_tag IN ('high') THEN '01_high'
                WHEN user_contribution_tag IN ('middle') THEN '02_middle'
                WHEN user_contribution_tag IN ('low') THEN '03_low'
                WHEN user_contribution_tag IN ('lto 3-') THEN '04_lto 3-'
                WHEN user_contribution_tag IN ('undefined') THEN '05_undefined'
            END AS user_contribution_tag
        FROM lazada_ads.dws_lzd_cdp_usr_uid_contribute_value_v3
        WHERE 1 = 1
            AND ds_month = 202408
            AND venture = 'VN'
    ) AS t2 ON t1.user_id = t2.user_id
    LEFT JOIN (
        SELECT CASE
                WHEN ds BETWEEN 20240715 AND 20240731 THEN 'Data range: 20240715 ==>> 20240731'
                WHEN ds BETWEEN 20240815 AND 20240831 THEN 'Data range: 20240815 ==>> 20240831'
            END AS duration,
            buyer_id,
            COUNT(DISTINCT order_id) AS order_sku_cnt
        FROM lazada_cdm.dwd_lzd_trd_core_fulfill_di
        WHERE 1 = 1
            AND (
                ds BETWEEN 20240715 AND 20240731
                OR ds BETWEEN 20240815 AND 20240831
            )
            AND venture = 'VN'
            AND is_revenue = 1
            AND COALESCE(business_application, 'LZD') IN ('LZD,ZAL', 'LZD')
        GROUP BY CASE
                WHEN ds BETWEEN 20240715 AND 20240731 THEN 'Data range: 20240715 ==>> 20240731'
                WHEN ds BETWEEN 20240815 AND 20240831 THEN 'Data range: 20240815 ==>> 20240831'
            END,
            buyer_id
    ) AS t3 ON t1.user_id = t3.buyer_id
GROUP BY t1.duration,
CASE
        WHEN COALESCE(t3.order_sku_cnt, 0) > 0 THEN 'Buyer'
        ELSE 'Non-Buyer'
    END,
    COALESCE(t2.user_contribution_tag, '05_undefined');
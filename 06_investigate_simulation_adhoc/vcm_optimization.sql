-- MaxCompute SQL 
-- ********************************************************************--
-- author:Truong, Van Thanh
-- create time:2024-11-09 15:32:50
-- ********************************************************************--
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
    WHERE 1 = 1
        AND ds_month = 202409
        AND venture = 'VN'
),
t_dim_camp AS (
    SELECT ds,
        CASE
            WHEN camp_type_fixed = 'BAU' THEN '01_bau'
            ELSE '02_non_bau'
        END AS camp_type
    FROM (
            SELECT ds,
                day_type,
                camp_type AS camp_type_original,
                CASE
                    WHEN TOLOWER(camp_type) IN ('mega') THEN 'Mega'
                    WHEN TOLOWER(camp_type) IN ('pd') THEN 'A+'
                    WHEN TOLOWER(camp_type) IN ('holiday') THEN 'BAU'
                    ELSE camp_type
                END AS camp_type_fixed
            FROM lazada_analyst.cp_calendar
            WHERE 1 = 1
                AND TO_CHAR(TO_DATE(ds, 'yyyymmdd'), 'yyyymm') = 202410
        )
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
            THEN '03_vcm_ui_ops'
        END AS promo_mechanics,
        CAST(
            REGEXP_EXTRACT(
                KEYVALUE(features, ',', ':', '"voucherSubType"'),
                '\"([0-9]*)\"'
            ) AS BIGINT
        ) AS vouchersutype,
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
        END AS voucher_scheme,
        CASE
            WHEN voucher_type IN ('percent_amount') THEN CASE
                WHEN CAST(
                    COALESCE(promotion_cap_value, 0) / 1000 AS BIGINT
                ) IN (1000) THEN 'Campaign bundle - Ultra high' --<< Campaign bundle section logic
                WHEN CAST(
                    COALESCE(promotion_cap_value, 0) / 1000 AS BIGINT
                ) IN (400) THEN 'Campaign bundle - High'
                WHEN CAST(
                    COALESCE(promotion_cap_value, 0) / 1000 AS BIGINT
                ) IN (100) THEN 'Campaign bundle - Mid'
                WHEN CAST(
                    COALESCE(promotion_cap_value, 0) / 1000 AS BIGINT
                ) IN (30) THEN 'Campaign bundle - Low'
                WHEN CAST(
                    COALESCE(promotion_cap_value, 0) / 1000 AS BIGINT
                ) IN (75) THEN 'Daily bundle - High' --<< Daily bundle section logic
                WHEN CAST(
                    COALESCE(promotion_cap_value, 0) / 1000 AS BIGINT
                ) IN (15) THEN 'Daily bundle - Mid'
                WHEN CAST(
                    COALESCE(promotion_cap_value, 0) / 1000 AS BIGINT
                ) IN (10) THEN 'Daily bundle - Low'
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
),
t_trf AS (
    SELECT COALESCE(t3.camp_type, '00_full_month') AS camp_type,
        COALESCE(
            COALESCE(t2.user_contribution_tag, '05_undefined'),
            '00_all'
        ) AS user_contribution_tag,
        COUNT(DISTINCT t1.utdid) AS mau
    FROM (
            SELECT *
            FROM lazada_cdm.dws_lzd_mkt_app_ad_mp_di
            WHERE 1 = 1
                AND TO_CHAR(TO_DATE(ds, 'yyyymmdd'), 'yyyymm') = 202410
                AND venture = 'VN'
                AND attr_model IN ('ft_1d_np')
                AND utdid IS NOT NULL
                AND is_active_utdid = 1
        ) AS t1
        LEFT JOIN (
            SELECT *
            FROM t_user_segment
        ) AS t2 ON t1.user_id = t2.user_id
        LEFT JOIN (
            SELECT *
            FROM t_dim_camp
        ) AS t3 ON t1.ds = t3.ds
    GROUP BY CUBE(
            t3.camp_type,
            COALESCE(t2.user_contribution_tag, '05_undefined')
        )
),
t_trf_avg AS (
    SELECT camp_type,
        user_contribution_tag,
        IF(duration = 0, 0, dau / duration) AS dau
    FROM (
            SELECT COALESCE(camp_type, '00_full_month') AS camp_type,
                user_contribution_tag,
                COUNT(DISTINCT ds) AS duration,
                SUM(dau) AS dau
            FROM (
                    SELECT t1.ds,
                        t3.camp_type,
                        COALESCE(
                            COALESCE(t2.user_contribution_tag, '05_undefined'),
                            '00_all'
                        ) AS user_contribution_tag,
                        COUNT(DISTINCT t1.utdid) AS dau
                    FROM (
                            SELECT *
                            FROM lazada_cdm.dws_lzd_mkt_app_ad_mp_di
                            WHERE 1 = 1
                                AND TO_CHAR(TO_DATE(ds, 'yyyymmdd'), 'yyyymm') = 202410
                                AND venture = 'VN'
                                AND attr_model IN ('ft_1d_np')
                                AND utdid IS NOT NULL
                                AND is_active_utdid = 1
                        ) AS t1
                        LEFT JOIN (
                            SELECT *
                            FROM t_user_segment
                        ) AS t2 ON t1.user_id = t2.user_id
                        LEFT JOIN (
                            SELECT *
                            FROM t_dim_camp
                        ) AS t3 ON t1.ds = t3.ds
                    GROUP BY t1.ds,
                        t3.camp_type,
                        CUBE(
                            COALESCE(t2.user_contribution_tag, '05_undefined')
                        )
                )
            GROUP BY user_contribution_tag,
                CUBE(camp_type)
        )
),
t_collect AS (
    SELECT COALESCE(t5.camp_type, '00_full_month') AS camp_type,
        COALESCE(
            COALESCE(t4.user_contribution_tag, '05_undefined'),
            '00_all'
        ) AS user_contribution_tag,
        COUNT(DISTINCT t1.buyer_id) AS collect_uv,
        COUNT(*) AS collect_cnt
    FROM (
            SELECT *
            FROM lazada_cdm.dwd_lzd_pro_voucher_collect_di
            WHERE 1 = 1
                AND TO_CHAR(TO_DATE(ds, 'yyyymmdd'), 'yyyymm') = 202410
                AND venture = 'VN'
                AND start_local_date >= 20240925
        ) AS t1
        INNER JOIN (
            SELECT *
            FROM t_dim_promo
            WHERE 1 = 1
                AND promo_mechanics = '03_vcm_ui_ops'
        ) AS t3 ON t1.promotion_id = t3.promotion_id
        LEFT JOIN (
            SELECT *
            FROM t_user_segment
        ) AS t4 ON t1.buyer_id = t4.user_id
        LEFT JOIN (
            SELECT *
            FROM t_dim_camp
        ) AS t5 ON t1.ds = t5.ds
    GROUP BY CUBE(
            t5.camp_type,
            COALESCE(t4.user_contribution_tag, '05_undefined')
        )
),
t_trf_voh AS (
    SELECT COALESCE(t5.camp_type, '00_full_month') AS camp_type,
        COALESCE(
            COALESCE(t4.user_contribution_tag, '05_undefined'),
            '00_all'
        ) AS user_contribution_tag,
        COUNT(DISTINCT t1.utdid) AS mau_vcm_voh
    FROM (
            SELECT *
            FROM lazada_cdm.dws_lzd_mkt_app_ad_mp_di
            WHERE 1 = 1
                AND TO_CHAR(TO_DATE(ds, 'yyyymmdd'), 'yyyymm') = 202410
                AND venture = 'VN'
                AND attr_model IN ('ft_1d_np')
                AND utdid IS NOT NULL
                AND is_active_utdid = 1
        ) AS t1
        INNER JOIN (
            SELECT *
            FROM lazada_cdm.dwd_lzd_pro_voucher_collect_di
            WHERE 1 = 1
                AND ds >= 20240925
                AND venture = 'VN'
                AND start_local_date >= 20240925
        ) AS t2 ON t1.ds >= t2.ds --<< Collect ==>> Has voucher on hand ==>> Open app
        AND t1.user_id = t2.buyer_id
        AND t1.ds BETWEEN t2.start_local_date AND t2.end_local_date
        INNER JOIN (
            SELECT *
            FROM t_dim_promo
            WHERE 1 = 1
                AND promo_mechanics = '03_vcm_ui_ops'
        ) AS t3 ON t2.promotion_id = t3.promotion_id
        LEFT JOIN (
            SELECT *
            FROM t_user_segment
        ) AS t4 ON t1.user_id = t4.user_id
        LEFT JOIN (
            SELECT *
            FROM t_dim_camp
        ) AS t5 ON t1.ds = t5.ds
    GROUP BY CUBE(
            t5.camp_type,
            COALESCE(t4.user_contribution_tag, '05_undefined')
        )
),
t_trf_voh_avg AS (
    SELECT camp_type,
        user_contribution_tag,
        IF(duration = 0, 0, dau_vcm_voh / duration) AS dau_vcm_voh
    FROM (
            SELECT COALESCE(camp_type, '00_full_month') AS camp_type,
                user_contribution_tag,
                COUNT(DISTINCT ds) AS duration,
                SUM(dau_vcm_voh) AS dau_vcm_voh
            FROM (
                    SELECT t1.ds,
                        t5.camp_type,
                        COALESCE(
                            COALESCE(t4.user_contribution_tag, '05_undefined'),
                            '00_all'
                        ) AS user_contribution_tag,
                        COUNT(DISTINCT t1.utdid) AS dau_vcm_voh
                    FROM (
                            SELECT *
                            FROM lazada_cdm.dws_lzd_mkt_app_ad_mp_di
                            WHERE 1 = 1
                                AND TO_CHAR(TO_DATE(ds, 'yyyymmdd'), 'yyyymm') = 202410
                                AND venture = 'VN'
                                AND attr_model IN ('ft_1d_np')
                                AND utdid IS NOT NULL
                                AND is_active_utdid = 1
                        ) AS t1
                        INNER JOIN (
                            SELECT *
                            FROM lazada_cdm.dwd_lzd_pro_voucher_collect_di
                            WHERE 1 = 1
                                AND ds >= 20240925
                                AND venture = 'VN'
                                AND start_local_date >= 20240925
                        ) AS t2 ON t1.ds >= t2.ds --<< Collect ==>> Has voucher on hand ==>> Open app
                        AND t1.user_id = t2.buyer_id
                        AND t1.ds BETWEEN t2.start_local_date AND t2.end_local_date
                        INNER JOIN (
                            SELECT *
                            FROM t_dim_promo
                            WHERE 1 = 1
                                AND promo_mechanics = '03_vcm_ui_ops'
                        ) AS t3 ON t2.promotion_id = t3.promotion_id
                        LEFT JOIN (
                            SELECT *
                            FROM t_user_segment
                        ) AS t4 ON t1.user_id = t4.user_id
                        LEFT JOIN (
                            SELECT *
                            FROM t_dim_camp
                        ) AS t5 ON t1.ds = t5.ds
                    GROUP BY t1.ds,
                        t5.camp_type,
                        CUBE(
                            COALESCE(t4.user_contribution_tag, '05_undefined')
                        )
                )
            GROUP BY user_contribution_tag,
                CUBE(camp_type)
        )
),
t_trn AS (
    SELECT COALESCE(t5.camp_type, '00_full_month') AS camp_type,
        COALESCE(
            COALESCE(t4.user_contribution_tag, '05_undefined'),
            '00_all'
        ) AS user_contribution_tag,
        COUNT(DISTINCT t1.buyer_id) AS mab,
        COUNT(
            DISTINCT CASE
                WHEN min_r > 0 THEN t1.buyer_id
                ELSE NULL
            END
        ) AS mab_exc_ac_ae_full_month,
        COUNT(
            DISTINCT CASE
                WHEN t2.sales_order_item_id IS NOT NULL THEN t1.buyer_id
                ELSE NULL
            END
        ) AS mab_vcm_gui,
        COUNT(
            DISTINCT CASE
                WHEN t2.sales_order_item_id IS NOT NULL THEN t1.check_out_id
                ELSE NULL
            END
        ) AS number_redeem_vcm_gui,
        COUNT(DISTINCT t1.order_id) AS ord,
        COUNT(
            DISTINCT CASE
                WHEN TOLOWER(t1.business_type_level2) NOT LIKE '%choice%'
                AND TOLOWER(t1.business_type_level2) NOT LIKE '%ae%' THEN t1.order_id
                ELSE NULL
            END
        ) AS ord_exc_ac_ae,
        COUNT(
            DISTINCT CASE
                WHEN t6.seller_id IS NOT NULL THEN t1.order_id
                ELSE NULL
            END
        ) AS ord_vcm_eli,
        COUNT(
            DISTINCT CASE
                WHEN t6.seller_id IS NOT NULL
                AND t2.sales_order_item_id IS NOT NULL THEN t1.order_id
                ELSE NULL
            END
        ) AS ord_vcm_eli_gui,
        COUNT(
            DISTINCT CASE
                WHEN t6.seller_id IS NOT NULL
                AND t2.sales_order_item_id IS NULL THEN t1.order_id
                ELSE NULL
            END
        ) AS ord_vcm_eli_un_gui,
        SUM(t1.actual_gmv * t1.exchange_rate) AS gmv,
        SUM(
            CASE
                WHEN TOLOWER(t1.business_type_level2) NOT LIKE '%choice%'
                AND TOLOWER(t1.business_type_level2) NOT LIKE '%ae%' THEN t1.actual_gmv * t1.exchange_rate
                ELSE 0
            END
        ) AS gmv_exc_ac_ae,
        SUM(
            CASE
                WHEN t6.seller_id IS NOT NULL THEN t1.actual_gmv * t1.exchange_rate
                ELSE 0
            END
        ) AS gmv_vcm_eli,
        SUM(
            CASE
                WHEN t6.seller_id IS NOT NULL
                AND t2.sales_order_item_id IS NOT NULL THEN t1.actual_gmv * t1.exchange_rate
                ELSE 0
            END
        ) AS gmv_vcm_eli_gui,
        SUM(
            CASE
                WHEN t6.seller_id IS NOT NULL
                AND t2.sales_order_item_id IS NULL THEN t1.actual_gmv * t1.exchange_rate
                ELSE 0
            END
        ) AS gmv_vcm_eli_un_gui,
        SUM(
            COALESCE(t2.promotion_amount, 0) * t1.exchange_rate
        ) AS total_promo_amt_ff,
        SUM(COALESCE(t7.vcm_slr_amt, 0) * t1.exchange_rate) AS slr_fee_ff,
        SUM(
            COALESCE(t2.promotion_amount, 0) * t1.exchange_rate - COALESCE(t7.vcm_slr_amt, 0) * t1.exchange_rate
        ) AS plt_promo_amt_ff
    FROM (
            SELECT *,
                MIN(r) OVER (PARTITION BY buyer_id) AS min_r
            FROM (
                    SELECT *,
                        CASE
                            WHEN TOLOWER(business_type_level2) NOT LIKE '%choice%'
                            AND TOLOWER(business_type_level2) NOT LIKE '%ae%' THEN ROW_NUMBER() OVER (
                                PARTITION BY TOLOWER(business_type_level2) NOT LIKE '%choice%',
                                TOLOWER(business_type_level2) NOT LIKE '%ae%',
                                buyer_id,
                                TO_CHAR(TO_DATE(ds, 'yyyymmdd'), 'yyyymm')
                            )
                            ELSE 0
                        END AS r
                    FROM lazada_cdm.dwd_lzd_trd_core_fulfill_di
                    WHERE 1 = 1
                        AND TO_CHAR(TO_DATE(ds, 'yyyymmdd'), 'yyyymm') = 202410
                        AND venture = 'VN'
                        AND is_revenue = 1
                        AND COALESCE(business_application, 'LZD') IN ('LZD,ZAL', 'LZD')
                )
        ) AS t1
        LEFT JOIN (
            SELECT sales_order_item_id,
                SUM(promotion_amount) AS promotion_amount
            FROM lazada_cdm.dwd_lzd_pro_promotion_item_di
            WHERE 1 = 1
                AND ds >= 20240925
                AND venture = 'VN'
                AND is_fulfilled = 1
                AND TOLOWER(promotion_role) IN ('platform')
                AND (
                    TOLOWER(retail_sponsor) IN ('platform')
                    OR retail_sponsor IS NULL
                )
                AND promotion_id IN (
                    SELECT promotion_id
                    FROM t_dim_promo
                    WHERE 1 = 1
                        AND promo_mechanics = '03_vcm_ui_ops'
                )
            GROUP BY sales_order_item_id
        ) AS t2 ON t1.sales_order_item_id = t2.sales_order_item_id
        LEFT JOIN (
            SELECT sales_order_item_id,
                SUM(
                    ABS(
                        COALESCE(KEYVALUE(exp_comm_amt_detail, 'LPI'), 0)
                    )
                ) AS vcm_slr_amt
            FROM lazada_ent_cdm.dwd_lzd_fin_trd_commission_di
            WHERE 1 = 1
                AND TO_CHAR(TO_DATE(ds, 'yyyymmdd'), 'yyyymm') = 202410
                AND venture = 'VN'
            GROUP BY sales_order_item_id
        ) AS t7 ON t1.sales_order_item_id = t7.sales_order_item_id
        LEFT JOIN (
            SELECT *
            FROM t_user_segment
        ) AS t4 ON TO_CHAR(TO_DATE(t1.ds, 'yyyymmdd'), 'yyyymm') = TO_CHAR(
            DATEADD(TO_DATE(t4.ds_month, 'yyyymm'), + 1, 'mm'),
            'yyyymm'
        )
        AND t1.buyer_id = t4.user_id
        LEFT JOIN (
            SELECT *
            FROM t_dim_camp
        ) AS t5 ON t1.ds = t5.ds
        LEFT JOIN (
            SELECT DISTINCT seller_id
            FROM lazada_ds.ds_lzd_program_seller
            WHERE 1 = 1
                AND ds = 20241031
                AND program_id IN (1534, 1564)
                AND status = 1
                AND venture = 'VN'
        ) AS t6 ON t1.seller_id = t6.seller_id
    GROUP BY CUBE(
            t5.camp_type,
            COALESCE(t4.user_contribution_tag, '05_undefined')
        )
),
t_trn_byr AS (
    SELECT COALESCE(t5.camp_type, '00_full_month') AS camp_type,
        COALESCE(
            COALESCE(t4.user_contribution_tag, '05_undefined'),
            '00_all'
        ) AS user_contribution_tag,
        COUNT(DISTINCT t1.buyer_id) AS mab_any_promo_exc_fsm_full_month
    FROM (
            SELECT *
            FROM lazada_cdm.dwd_lzd_trd_core_fulfill_di
            WHERE 1 = 1
                AND TO_CHAR(TO_DATE(ds, 'yyyymmdd'), 'yyyymm') = 202410
                AND venture = 'VN'
                AND is_revenue = 1
                AND COALESCE(business_application, 'LZD') IN ('LZD,ZAL', 'LZD')
        ) AS t1 LEFT ANTI
        JOIN (
            SELECT t1.buyer_id AS buyer_id,
                MAX(
                    CASE
                        WHEN t2.sales_order_item_id IS NOT NULL THEN 1
                        ELSE 0
                    END
                ) AS is_fsm
            FROM (
                    SELECT *
                    FROM lazada_cdm.dwd_lzd_trd_core_fulfill_di
                    WHERE 1 = 1
                        AND TO_CHAR(TO_DATE(ds, 'yyyymmdd'), 'yyyymm') = 202410
                        AND venture = 'VN'
                        AND is_revenue = 1
                        AND COALESCE(business_application, 'LZD') IN ('LZD,ZAL', 'LZD')
                ) AS t1
                LEFT JOIN (
                    SELECT DISTINCT sales_order_item_id
                    FROM lazada_cdm.dwd_lzd_pro_promotion_item_di
                    WHERE 1 = 1
                        AND ds >= 20240925
                        AND venture = 'VN'
                        AND is_fulfilled = 1
                        AND TOLOWER(promotion_role) IN ('platform')
                        AND (
                            TOLOWER(retail_sponsor) IN ('platform')
                            OR retail_sponsor IS NULL
                        )
                        AND promotion_id IN (
                            SELECT promotion_id
                            FROM t_dim_promo
                            WHERE 1 = 1
                                AND promo_mechanics = '01_fsm_all_ops'
                        )
                ) AS t2 ON t1.sales_order_item_id = t2.sales_order_item_id
            GROUP BY t1.buyer_id
            HAVING MAX(
                    CASE
                        WHEN t2.sales_order_item_id IS NOT NULL THEN 1
                        ELSE 0
                    END
                ) = 1
        ) AS t2 ON t1.buyer_id = t2.buyer_id
        LEFT JOIN (
            SELECT *
            FROM t_user_segment
        ) AS t4 ON TO_CHAR(TO_DATE(t1.ds, 'yyyymmdd'), 'yyyymm') = TO_CHAR(
            DATEADD(TO_DATE(t4.ds_month, 'yyyymm'), + 1, 'mm'),
            'yyyymm'
        )
        AND t1.buyer_id = t4.user_id
        LEFT JOIN (
            SELECT *
            FROM t_dim_camp
        ) AS t5 ON t1.ds = t5.ds
    GROUP BY CUBE(
            t5.camp_type,
            COALESCE(t4.user_contribution_tag, '05_undefined')
        )
),
t_final AS (
    SELECT camp_type,
        user_contribution_tag,
        SUM(usr_pool) AS usr_pool,
        SUM(mau) AS mau,
        SUM(mau_vcm_voh) AS mau_vcm_voh --
        -- ,SUM(collect_uv) AS collect_uv
,
        SUM(collect_cnt) AS collect_cnt,
        SUM(mab) AS mab,
        SUM(mab_exc_ac_ae_full_month) AS mab_exc_ac_ae_full_month,
        SUM(mab_any_promo_exc_fsm_full_month) AS mab_any_promo_exc_fsm_full_month,
        SUM(mab_vcm_gui) AS mab_vcm_gui,
        SUM(number_redeem_vcm_gui) AS number_redeem_vcm_gui,
        SUM(ord) AS ord,
        SUM(ord_exc_ac_ae) AS ord_exc_ac_ae,
        SUM(ord_vcm_eli) AS ord_vcm_eli,
        SUM(ord_vcm_eli_gui) AS ord_vcm_eli_gui,
        SUM(ord_vcm_eli_un_gui) AS ord_vcm_eli_un_gui,
        SUM(gmv) AS gmv,
        SUM(gmv_exc_ac_ae) AS gmv_exc_ac_ae,
        SUM(gmv_vcm_eli) AS gmv_vcm_eli,
        SUM(gmv_vcm_eli_gui) AS gmv_vcm_eli_gui,
        SUM(gmv_vcm_eli_un_gui) AS gmv_vcm_eli_un_gui,
        SUM(total_promo_amt_ff) AS total_promo_amt_ff,
        SUM(slr_fee_ff) AS slr_fee_ff,
        SUM(plt_promo_amt_ff) AS plt_promo_amt_ff,
        SUM(dau) AS dau,
        SUM(dau_vcm_voh) AS dau_vcm_voh
    FROM (
            SELECT camp_type,
                user_contribution_tag,
                usr_pool,
                0 AS mau,
                0 AS mau_vcm_voh --
                -- ,0 AS collect_uv
,
                0 AS collect_cnt,
                0 AS mab,
                0 AS mab_exc_ac_ae_full_month,
                0 AS mab_any_promo_exc_fsm_full_month,
                0 AS mab_vcm_gui,
                0 AS number_redeem_vcm_gui,
                0 AS ord,
                0 AS ord_exc_ac_ae,
                0 AS ord_vcm_eli,
                0 AS ord_vcm_eli_gui,
                0 AS ord_vcm_eli_un_gui,
                0 AS gmv,
                0 AS gmv_exc_ac_ae,
                0 AS gmv_vcm_eli,
                0 AS gmv_vcm_eli_gui,
                0 AS gmv_vcm_eli_un_gui,
                0 AS total_promo_amt_ff,
                0 AS slr_fee_ff,
                0 AS plt_promo_amt_ff,
                0 AS dau,
                0 AS dau_vcm_voh
            FROM (
                    SELECT COALESCE(
                            COALESCE(user_contribution_tag, '05_undefined'),
                            '00_all'
                        ) AS user_contribution_tag,
                        COUNT(DISTINCT user_id) AS usr_pool
                    FROM t_user_segment
                    GROUP BY CUBE(COALESCE(user_contribution_tag, '05_undefined'))
                ) LATERAL VIEW EXPLODE(SPLIT('00_full_month,01_bau,02_non_bau', ',')) camp_type AS camp_type
            UNION ALL
            --<< BREAK POINT
            SELECT camp_type,
                user_contribution_tag,
                0 AS usr_pool,
                mau,
                0 AS mau_vcm_voh --
                -- ,0 AS collect_uv
,
                0 AS collect_cnt,
                0 AS mab,
                0 AS mab_exc_ac_ae_full_month,
                0 AS mab_any_promo_exc_fsm_full_month,
                0 AS mab_vcm_gui,
                0 AS number_redeem_vcm_gui,
                0 AS ord,
                0 AS ord_exc_ac_ae,
                0 AS ord_vcm_eli,
                0 AS ord_vcm_eli_gui,
                0 AS ord_vcm_eli_un_gui,
                0 AS gmv,
                0 AS gmv_exc_ac_ae,
                0 AS gmv_vcm_eli,
                0 AS gmv_vcm_eli_gui,
                0 AS gmv_vcm_eli_un_gui,
                0 AS total_promo_amt_ff,
                0 AS slr_fee_ff,
                0 AS plt_promo_amt_ff,
                0 AS dau,
                0 AS dau_vcm_voh
            FROM t_trf
            UNION ALL
            --<< BREAK POINT
            SELECT camp_type,
                user_contribution_tag,
                0 AS usr_pool,
                0 AS mau,
                mau_vcm_voh --
                -- ,0 AS collect_uv
,
                0 AS collect_cnt,
                0 AS mab,
                0 AS mab_exc_ac_ae_full_month,
                0 AS mab_any_promo_exc_fsm_full_month,
                0 AS mab_vcm_gui,
                0 AS number_redeem_vcm_gui,
                0 AS ord,
                0 AS ord_exc_ac_ae,
                0 AS ord_vcm_eli,
                0 AS ord_vcm_eli_gui,
                0 AS ord_vcm_eli_un_gui,
                0 AS gmv,
                0 AS gmv_exc_ac_ae,
                0 AS gmv_vcm_eli,
                0 AS gmv_vcm_eli_gui,
                0 AS gmv_vcm_eli_un_gui,
                0 AS total_promo_amt_ff,
                0 AS slr_fee_ff,
                0 AS plt_promo_amt_ff,
                0 AS dau,
                0 AS dau_vcm_voh
            FROM t_trf_voh
            UNION ALL
            --<< BREAK POINT
            SELECT camp_type,
                user_contribution_tag,
                0 AS usr_pool,
                0 AS mau,
                0 AS mau_vcm_voh --
                -- ,0 AS collect_uv
,
                0 AS collect_cnt,
                mab,
                mab_exc_ac_ae_full_month,
                0 AS mab_any_promo_exc_fsm_full_month,
                mab_vcm_gui,
                number_redeem_vcm_gui,
                ord,
                ord_exc_ac_ae,
                ord_vcm_eli,
                ord_vcm_eli_gui,
                ord_vcm_eli_un_gui,
                gmv,
                gmv_exc_ac_ae,
                gmv_vcm_eli,
                gmv_vcm_eli_gui,
                gmv_vcm_eli_un_gui,
                total_promo_amt_ff,
                slr_fee_ff,
                plt_promo_amt_ff,
                0 AS dau,
                0 AS dau_vcm_voh
            FROM t_trn
            UNION ALL
            --<< BREAK POINT
            SELECT camp_type,
                user_contribution_tag,
                0 AS usr_pool,
                0 AS mau,
                0 AS mau_vcm_voh --
                -- ,0 AS collect_uv
,
                0 AS collect_cnt,
                0 AS mab,
                0 AS mab_exc_ac_ae_full_month,
                mab_any_promo_exc_fsm_full_month,
                0 AS mab_vcm_gui,
                0 AS number_redeem_vcm_gui,
                0 AS ord,
                0 AS ord_exc_ac_ae,
                0 AS ord_vcm_eli,
                0 AS ord_vcm_eli_gui,
                0 AS ord_vcm_eli_un_gui,
                0 AS gmv,
                0 AS gmv_exc_ac_ae,
                0 AS gmv_vcm_eli,
                0 AS gmv_vcm_eli_gui,
                0 AS gmv_vcm_eli_un_gui,
                0 AS total_promo_amt_ff,
                0 AS slr_fee_ff,
                0 AS plt_promo_amt_ff,
                0 AS dau,
                0 AS dau_vcm_voh
            FROM t_trn_byr
            UNION ALL
            --<< BREAK POINT
            SELECT camp_type,
                user_contribution_tag,
                0 AS usr_pool,
                0 AS mau,
                0 AS mau_vcm_voh --
                -- ,collect_uv
,
                collect_cnt,
                0 AS mab,
                0 AS mab_exc_ac_ae_full_month,
                0 AS mab_any_promo_exc_fsm_full_month,
                0 AS mab_vcm_gui,
                0 AS number_redeem_vcm_gui,
                0 AS ord,
                0 AS ord_exc_ac_ae,
                0 AS ord_vcm_eli,
                0 AS ord_vcm_eli_gui,
                0 AS ord_vcm_eli_un_gui,
                0 AS gmv,
                0 AS gmv_exc_ac_ae,
                0 AS gmv_vcm_eli,
                0 AS gmv_vcm_eli_gui,
                0 AS gmv_vcm_eli_un_gui,
                0 AS total_promo_amt_ff,
                0 AS slr_fee_ff,
                0 AS plt_promo_amt_ff,
                0 AS dau,
                0 AS dau_vcm_voh
            FROM t_collect
            UNION ALL
            --<< BREAK POINT
            SELECT camp_type,
                user_contribution_tag,
                0 AS usr_pool,
                0 AS mau,
                0 AS mau_vcm_voh --
                -- ,collect_uv
,
                0 AS collect_cnt,
                0 AS mab,
                0 AS mab_exc_ac_ae_full_month,
                0 AS mab_any_promo_exc_fsm_full_month,
                0 AS mab_vcm_gui,
                0 AS number_redeem_vcm_gui,
                0 AS ord,
                0 AS ord_exc_ac_ae,
                0 AS ord_vcm_eli,
                0 AS ord_vcm_eli_gui,
                0 AS ord_vcm_eli_un_gui,
                0 AS gmv,
                0 AS gmv_exc_ac_ae,
                0 AS gmv_vcm_eli,
                0 AS gmv_vcm_eli_gui,
                0 AS gmv_vcm_eli_un_gui,
                0 AS total_promo_amt_ff,
                0 AS slr_fee_ff,
                0 AS plt_promo_amt_ff,
                dau,
                0 AS dau_vcm_voh
            FROM t_trf_avg
            UNION ALL
            --<< BREAK POINT
            SELECT camp_type,
                user_contribution_tag,
                0 AS usr_pool,
                0 AS mau,
                0 AS mau_vcm_voh --
                -- ,collect_uv
,
                0 AS collect_cnt,
                0 AS mab,
                0 AS mab_exc_ac_ae_full_month,
                0 AS mab_any_promo_exc_fsm_full_month,
                0 AS mab_vcm_gui,
                0 AS number_redeem_vcm_gui,
                0 AS ord,
                0 AS ord_exc_ac_ae,
                0 AS ord_vcm_eli,
                0 AS ord_vcm_eli_gui,
                0 AS ord_vcm_eli_un_gui,
                0 AS gmv,
                0 AS gmv_exc_ac_ae,
                0 AS gmv_vcm_eli,
                0 AS gmv_vcm_eli_gui,
                0 AS gmv_vcm_eli_un_gui,
                0 AS total_promo_amt_ff,
                0 AS slr_fee_ff,
                0 AS plt_promo_amt_ff,
                0 AS dau,
                dau_vcm_voh
            FROM t_trf_voh_avg
        )
    GROUP BY camp_type,
        user_contribution_tag
)
SELECT *
FROM t_final;
SELECT campaign_type,
    SUM(voucher_quantity) AS voucher_quantity
FROM (
        SELECT t1.promotion_id,
            CASE
                WHEN TOLOWER(voucher_tier) LIKE '%campaign%' THEN 'non Bau'
                WHEN TOLOWER(voucher_tier) LIKE '%daily%' THEN 'Bau'
            END AS campaign_type,
            MAX(COALESCE(t2.total_value, 0)) AS voucher_quantity
        FROM (
                SELECT ds,
                    status,
                    promotion_id,
                    voucher_name,
                    description,
                    promotion_start_date,
                    promotion_end_date,
                    product_code,
                    product_sub_code,
                    prom_type,
                    prom_sub_type,
                    features,
                    coupon_value,
                    min_order_value,
                    promotion_cap_value,
                    '03_vcm_ui_ops' AS promo_mechanics,
                    CAST(
                        REGEXP_EXTRACT(
                            KEYVALUE(features, ',', ':', '"voucherSubType"'),
                            '\"([0-9]*)\"'
                        ) AS BIGINT
                    ) AS vouchersubtype,
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
                    END AS voucher_scheme,
                    CASE
                        WHEN voucher_type IN ('percent_amount') THEN CASE
                            WHEN CAST(
                                COALESCE(promotion_cap_value, 0) / 1000 AS BIGINT
                            ) IN (1000) THEN 'Campaign bundle - Ultra high' --<< Campaign bundle section logic
                            WHEN CAST(
                                COALESCE(promotion_cap_value, 0) / 1000 AS BIGINT
                            ) IN (400) THEN 'Campaign bundle - High'
                            WHEN CAST(
                                COALESCE(promotion_cap_value, 0) / 1000 AS BIGINT
                            ) IN (100) THEN 'Campaign bundle - Mid'
                            WHEN CAST(
                                COALESCE(promotion_cap_value, 0) / 1000 AS BIGINT
                            ) IN (30) THEN 'Campaign bundle - Low'
                            WHEN CAST(
                                COALESCE(promotion_cap_value, 0) / 1000 AS BIGINT
                            ) IN (75) THEN 'Daily bundle - High' --<< Daily bundle section logic
                            WHEN CAST(
                                COALESCE(promotion_cap_value, 0) / 1000 AS BIGINT
                            ) IN (15) THEN 'Daily bundle - Mid'
                            WHEN CAST(
                                COALESCE(promotion_cap_value, 0) / 1000 AS BIGINT
                            ) IN (10) THEN 'Daily bundle - Low'
                            ELSE 'Other'
                        END
                        ELSE 'Other'
                    END AS voucher_tier
                FROM lazada_cdm.dim_lzd_pro_collectibles
                WHERE 1 = 1
                    AND ds = MAX_PT('lazada_cdm.dim_lzd_pro_collectibles')
                    AND TO_CHAR(promotion_start_date, 'yyyymmdd') >= 20241001
                    AND TO_CHAR(promotion_end_date, 'yyyymmdd') <= 20241031
                    AND status = 1
                    AND venture = 'VN'
                    AND REGEXP_INSTR(voucher_name, '^.*(TEST|Test|test)+.*') = 0 --<< Clear data
                    AND sponsor = 'platform'
                    AND TOLOWER(COALESCE(retail_sponsor, 'platform')) IN ('platform')
                    AND (
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
            ) AS t1
            LEFT JOIN (
                SELECT *
                FROM lazada_ods.s_promotion_budget_vn
                WHERE 1 = 1
                    AND ds = MAX_PT('lazada_ods.s_promotion_budget_vn')
                    AND budget_type = 1
            ) AS t2 ON t1.promotion_id = t2.promotion_id
        GROUP BY t1.promotion_id,
            CASE
                WHEN TOLOWER(voucher_tier) LIKE '%campaign%' THEN 'non Bau'
                WHEN TOLOWER(voucher_tier) LIKE '%daily%' THEN 'Bau'
            END
    )
GROUP BY campaign_type;
SELECT user_contribution_tag,
    camp_type,
    SUM(total_value) AS total_value
FROM (
        SELECT t1.promotion_id AS promotion_id,
            t1.camp_type AS camp_type,
            t1.total_value AS total_value,
            COALESCE(t2.user_contribution_tag, '05_undefined') AS user_contribution_tag
        FROM (
                SELECT t1.*,
                    COALESCE(t2.total_value, 0) AS total_value
                FROM (
                        SELECT promotion_id,
                            voucher_name,
                            description,
                            promotion_start_date,
                            promotion_end_date,
                            product_code,
                            product_sub_code,
                            prom_type,
                            prom_sub_type,
                            features,
                            coupon_value,
                            min_order_value,
                            promotion_cap_value,
                            '03_vcm_ui_ops' AS promo_mechanics,
                            CAST(
                                REGEXP_EXTRACT(
                                    KEYVALUE(features, ',', ':', '"voucherSubType"'),
                                    '\"([0-9]*)\"'
                                ) AS BIGINT
                            ) AS vouchersubtype,
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
                            END AS voucher_scheme,
                            CASE
                                WHEN voucher_type IN ('percent_amount') THEN CASE
                                    WHEN CAST(
                                        COALESCE(promotion_cap_value, 0) / 1000 AS BIGINT
                                    ) IN (1000) THEN 'Campaign bundle - Ultra high' --<< Campaign bundle section logic
                                    WHEN CAST(
                                        COALESCE(promotion_cap_value, 0) / 1000 AS BIGINT
                                    ) IN (400) THEN 'Campaign bundle - High'
                                    WHEN CAST(
                                        COALESCE(promotion_cap_value, 0) / 1000 AS BIGINT
                                    ) IN (100) THEN 'Campaign bundle - Mid'
                                    WHEN CAST(
                                        COALESCE(promotion_cap_value, 0) / 1000 AS BIGINT
                                    ) IN (30) THEN 'Campaign bundle - Low'
                                    WHEN CAST(
                                        COALESCE(promotion_cap_value, 0) / 1000 AS BIGINT
                                    ) IN (75) THEN 'Daily bundle - High' --<< Daily bundle section logic
                                    WHEN CAST(
                                        COALESCE(promotion_cap_value, 0) / 1000 AS BIGINT
                                    ) IN (15) THEN 'Daily bundle - Mid'
                                    WHEN CAST(
                                        COALESCE(promotion_cap_value, 0) / 1000 AS BIGINT
                                    ) IN (10) THEN 'Daily bundle - Low'
                                    ELSE 'Other'
                                END
                                ELSE 'Other'
                            END AS voucher_tier,
                            CASE
                                WHEN voucher_type IN ('percent_amount') THEN CASE
                                    WHEN CAST(
                                        COALESCE(promotion_cap_value, 0) / 1000 AS BIGINT
                                    ) IN (1000, 400, 100, 30) THEN 'non Bau'
                                    WHEN CAST(
                                        COALESCE(promotion_cap_value, 0) / 1000 AS BIGINT
                                    ) IN (75, 15, 10) THEN 'Bau'
                                    ELSE 'Other'
                                END
                                ELSE 'Other'
                            END AS camp_type
                        FROM lazada_cdm.dim_lzd_pro_collectibles
                        WHERE 1 = 1
                            AND ds = MAX_PT('lazada_cdm.dim_lzd_pro_collectibles')
                            AND TO_CHAR(promotion_start_date, 'yyyymmdd') >= 20241001
                            AND TO_CHAR(promotion_end_date, 'yyyymmdd') <= 20241031
                            AND status = 1
                            AND venture = 'VN'
                            AND REGEXP_INSTR(voucher_name, '^.*(TEST|Test|test)+.*') = 0 --<< Clear data
                            AND sponsor = 'platform'
                            AND TOLOWER(COALESCE(retail_sponsor, 'platform')) IN ('platform')
                            AND (
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
                    ) AS t1
                    LEFT JOIN (
                        SELECT *
                        FROM lazada_ods.s_promotion_budget_vn
                        WHERE 1 = 1
                            AND ds = MAX_PT('lazada_ods.s_promotion_budget_vn')
                            AND budget_type = 1
                    ) AS t2 ON t1.promotion_id = t2.promotion_id
            ) AS t1
            LEFT JOIN (
                SELECT *,
                    CASE
                        WHEN byr_cnt = max_r THEN 1
                        ELSE 0
                    END AS flag
                FROM (
                        SELECT *,
                            MAX(byr_cnt) OVER (PARTITION BY promotion_id) AS max_r
                        FROM (
                                SELECT COALESCE(t2.user_contribution_tag, '05_undefined') AS user_contribution_tag,
                                    t1.promotion_id,
                                    COUNT(DISTINCT t1.buyer_id) AS byr_cnt
                                FROM (
                                        SELECT *
                                        FROM lazada_cdm.dwd_lzd_pro_voucher_collect_di
                                        WHERE 1 = 1
                                            AND TO_CHAR(TO_DATE(ds, 'yyyymmdd'), 'yyyymm') = 202410
                                            AND venture = 'VN'
                                            AND promotion_id IN (
                                                SELECT promotion_id
                                                FROM lazada_cdm.dim_lzd_pro_collectibles
                                                WHERE 1 = 1
                                                    AND ds = MAX_PT('lazada_cdm.dim_lzd_pro_collectibles')
                                                    AND TO_CHAR(promotion_start_date, 'yyyymmdd') >= 20241001
                                                    AND TO_CHAR(promotion_end_date, 'yyyymmdd') <= 20241031
                                                    AND status = 1
                                                    AND venture = 'VN'
                                                    AND REGEXP_INSTR(voucher_name, '^.*(TEST|Test|test)+.*') = 0 --<< Clear data
                                                    AND sponsor = 'platform'
                                                    AND TOLOWER(COALESCE(retail_sponsor, 'platform')) IN ('platform')
                                                    AND (
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
                                    ) AS t1
                                    LEFT JOIN (
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
                                        WHERE 1 = 1
                                            AND ds_month = 202409
                                            AND venture = 'VN'
                                    ) AS t2 ON t1.buyer_id = t2.user_id
                                GROUP BY COALESCE(t2.user_contribution_tag, '05_undefined'),
                                    t1.promotion_id
                                ORDER BY t1.promotion_id ASC
                            )
                    )
                WHERE 1 = 1
                    AND CASE
                        WHEN byr_cnt = max_r THEN 1
                        ELSE 0
                    END = 1
            ) AS t2 ON t1.promotion_id = t2.promotion_id
    )
GROUP BY user_contribution_tag,
    camp_type;
-------------------------------------------------------------------------------------
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
    WHERE 1 = 1
        AND ds_month = 202407
        AND venture = 'VN'
),
t_dim_camp AS (
    SELECT ds,
        CASE
            WHEN camp_type_fixed = 'BAU' THEN '01_bau'
            ELSE '02_non_bau'
        END AS camp_type
    FROM (
            SELECT ds,
                day_type,
                camp_type AS camp_type_original,
                CASE
                    WHEN TOLOWER(camp_type) IN ('mega') THEN 'Mega'
                    WHEN TOLOWER(camp_type) IN ('pd') THEN 'A+'
                    WHEN TOLOWER(camp_type) IN ('holiday') THEN 'BAU'
                    ELSE camp_type
                END AS camp_type_fixed
            FROM lazada_analyst.cp_calendar
            WHERE 1 = 1
                AND TO_CHAR(TO_DATE(ds, 'yyyymmdd'), 'yyyymm') = 202408
        )
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
            ) THEN '03_vcm_ui_ops'
        END AS promo_mechanics,
        CAST(
            REGEXP_EXTRACT(
                KEYVALUE(features, ',', ':', '"voucherSubType"'),
                '\"([0-9]*)\"'
            ) AS BIGINT
        ) AS vouchersutype,
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
        END AS voucher_scheme,
        CASE
            WHEN voucher_type IN ('percent_amount') THEN CASE
                WHEN CAST(
                    COALESCE(promotion_cap_value, 0) / 1000 AS BIGINT
                ) IN (1000) THEN 'Campaign bundle - Ultra high' --<< Campaign bundle section logic
                WHEN CAST(
                    COALESCE(promotion_cap_value, 0) / 1000 AS BIGINT
                ) IN (400) THEN 'Campaign bundle - High'
                WHEN CAST(
                    COALESCE(promotion_cap_value, 0) / 1000 AS BIGINT
                ) IN (100) THEN 'Campaign bundle - Mid'
                WHEN CAST(
                    COALESCE(promotion_cap_value, 0) / 1000 AS BIGINT
                ) IN (30) THEN 'Campaign bundle - Low'
                WHEN CAST(
                    COALESCE(promotion_cap_value, 0) / 1000 AS BIGINT
                ) IN (75) THEN 'Daily bundle - High' --<< Daily bundle section logic
                WHEN CAST(
                    COALESCE(promotion_cap_value, 0) / 1000 AS BIGINT
                ) IN (15) THEN 'Daily bundle - Mid'
                WHEN CAST(
                    COALESCE(promotion_cap_value, 0) / 1000 AS BIGINT
                ) IN (10) THEN 'Daily bundle - Low'
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
        )
),
t_trf AS (
    SELECT COALESCE(t3.camp_type, '00_full_month') AS camp_type,
        COALESCE(
            COALESCE(t2.user_contribution_tag, '05_undefined'),
            '00_all'
        ) AS user_contribution_tag,
        COUNT(DISTINCT t1.utdid) AS mau
    FROM (
            SELECT *
            FROM lazada_cdm.dws_lzd_mkt_app_ad_mp_di
            WHERE 1 = 1
                AND TO_CHAR(TO_DATE(ds, 'yyyymmdd'), 'yyyymm') = 202408
                AND venture = 'VN'
                AND attr_model IN ('ft_1d_np')
                AND utdid IS NOT NULL
                AND is_active_utdid = 1
        ) AS t1
        LEFT JOIN (
            SELECT *
            FROM t_user_segment
        ) AS t2 ON t1.user_id = t2.user_id
        LEFT JOIN (
            SELECT *
            FROM t_dim_camp
        ) AS t3 ON t1.ds = t3.ds
    GROUP BY CUBE(
            t3.camp_type,
            COALESCE(t2.user_contribution_tag, '05_undefined')
        )
),
t_collect AS (
    SELECT COALESCE(t5.camp_type, '00_full_month') AS camp_type,
        COALESCE(
            COALESCE(t4.user_contribution_tag, '05_undefined'),
            '00_all'
        ) AS user_contribution_tag,
        COUNT(DISTINCT t1.buyer_id) AS collect_uv,
        COUNT(*) AS collect_cnt
    FROM (
            SELECT *
            FROM lazada_cdm.dwd_lzd_pro_voucher_collect_di
            WHERE 1 = 1
                AND TO_CHAR(TO_DATE(ds, 'yyyymmdd'), 'yyyymm') = 202408
                AND venture = 'VN'
                AND start_local_date >= 20240720
        ) AS t1
        INNER JOIN (
            SELECT *
            FROM t_dim_promo
            WHERE 1 = 1
                AND promo_mechanics = '03_vcm_ui_ops'
        ) AS t3 ON t1.promotion_id = t3.promotion_id
        LEFT JOIN (
            SELECT *
            FROM t_user_segment
        ) AS t4 ON t1.buyer_id = t4.user_id
        LEFT JOIN (
            SELECT *
            FROM t_dim_camp
        ) AS t5 ON t1.ds = t5.ds
    GROUP BY CUBE(
            t5.camp_type,
            COALESCE(t4.user_contribution_tag, '05_undefined')
        )
),
t_trf_voh AS (
    SELECT COALESCE(t5.camp_type, '00_full_month') AS camp_type,
        COALESCE(
            COALESCE(t4.user_contribution_tag, '05_undefined'),
            '00_all'
        ) AS user_contribution_tag,
        COUNT(DISTINCT t1.utdid) AS mau_vcm_voh
    FROM (
            SELECT *
            FROM lazada_cdm.dws_lzd_mkt_app_ad_mp_di
            WHERE 1 = 1
                AND TO_CHAR(TO_DATE(ds, 'yyyymmdd'), 'yyyymm') = 202408
                AND venture = 'VN'
                AND attr_model IN ('ft_1d_np')
                AND utdid IS NOT NULL
                AND is_active_utdid = 1
        ) AS t1
        INNER JOIN (
            SELECT *
            FROM lazada_cdm.dwd_lzd_pro_voucher_collect_di
            WHERE 1 = 1
                AND ds >= 20240801
                AND venture = 'VN'
                AND start_local_date >= 20240725
        ) AS t2 ON t1.ds >= t2.ds --<< Collect ==>> Has voucher on hand ==>> Open app
        AND t1.user_id = t2.buyer_id
        AND t1.ds BETWEEN t2.start_local_date AND t2.end_local_date
        INNER JOIN (
            SELECT *
            FROM t_dim_promo
            WHERE 1 = 1
                AND promo_mechanics = '03_vcm_ui_ops'
        ) AS t3 ON t2.promotion_id = t3.promotion_id
        LEFT JOIN (
            SELECT *
            FROM t_user_segment
        ) AS t4 ON t1.user_id = t4.user_id
        LEFT JOIN (
            SELECT *
            FROM t_dim_camp
        ) AS t5 ON t1.ds = t5.ds
    GROUP BY CUBE(
            t5.camp_type,
            COALESCE(t4.user_contribution_tag, '05_undefined')
        )
),
t_trn AS (
    SELECT COALESCE(t5.camp_type, '00_full_month') AS camp_type,
        COALESCE(
            COALESCE(t4.user_contribution_tag, '05_undefined'),
            '00_all'
        ) AS user_contribution_tag,
        COUNT(DISTINCT t1.buyer_id) AS mab,
        COUNT(
            DISTINCT CASE
                WHEN min_r > 0 THEN t1.buyer_id
                ELSE NULL
            END
        ) AS mab_exc_ac_ae_full_month,
        COUNT(
            DISTINCT CASE
                WHEN t2.sales_order_item_id IS NOT NULL THEN t1.buyer_id
                ELSE NULL
            END
        ) AS mab_vcm_gui,
        COUNT(
            DISTINCT CASE
                WHEN t2.sales_order_item_id IS NOT NULL THEN t1.check_out_id
                ELSE NULL
            END
        ) AS number_redeem_vcm_gui,
        COUNT(DISTINCT t1.order_id) AS ord,
        COUNT(
            DISTINCT CASE
                WHEN TOLOWER(t1.business_type_level2) NOT LIKE '%choice%'
                AND TOLOWER(t1.business_type_level2) NOT LIKE '%ae%' THEN t1.order_id
                ELSE NULL
            END
        ) AS ord_exc_ac_ae,
        COUNT(
            DISTINCT CASE
                WHEN t6.sku_id IS NOT NULL THEN t1.order_id
                ELSE NULL
            END
        ) AS ord_vcm_eli,
        COUNT(
            DISTINCT CASE
                WHEN t6.sku_id IS NOT NULL
                AND t2.sales_order_item_id IS NOT NULL THEN t1.order_id
                ELSE NULL
            END
        ) AS ord_vcm_eli_gui,
        COUNT(
            DISTINCT CASE
                WHEN t6.sku_id IS NOT NULL
                AND t2.sales_order_item_id IS NULL THEN t1.order_id
                ELSE NULL
            END
        ) AS ord_vcm_eli_un_gui,
        SUM(t1.actual_gmv * t1.exchange_rate) AS gmv,
        SUM(
            CASE
                WHEN TOLOWER(t1.business_type_level2) NOT LIKE '%choice%'
                AND TOLOWER(t1.business_type_level2) NOT LIKE '%ae%' THEN t1.actual_gmv * t1.exchange_rate
                ELSE 0
            END
        ) AS gmv_exc_ac_ae,
        SUM(
            CASE
                WHEN t6.sku_id IS NOT NULL THEN t1.actual_gmv * t1.exchange_rate
                ELSE 0
            END
        ) AS gmv_vcm_eli,
        SUM(
            CASE
                WHEN t6.sku_id IS NOT NULL
                AND t2.sales_order_item_id IS NOT NULL THEN t1.actual_gmv * t1.exchange_rate
                ELSE 0
            END
        ) AS gmv_vcm_eli_gui,
        SUM(
            CASE
                WHEN t6.sku_id IS NOT NULL
                AND t2.sales_order_item_id IS NULL THEN t1.actual_gmv * t1.exchange_rate
                ELSE 0
            END
        ) AS gmv_vcm_eli_un_gui,
        SUM(
            COALESCE(t2.promotion_amount, 0) * t1.exchange_rate
        ) AS total_promo_amt_ff,
        SUM(COALESCE(t7.vcm_slr_amt, 0) * t1.exchange_rate) AS slr_fee_ff,
        SUM(
            COALESCE(t2.promotion_amount, 0) * t1.exchange_rate - COALESCE(t7.vcm_slr_amt, 0) * t1.exchange_rate
        ) AS plt_promo_amt_ff
    FROM (
            SELECT *,
                MIN(r) OVER (PARTITION BY buyer_id) AS min_r
            FROM (
                    SELECT *,
                        CASE
                            WHEN TOLOWER(business_type_level2) NOT LIKE '%choice%'
                            AND TOLOWER(business_type_level2) NOT LIKE '%ae%' THEN ROW_NUMBER() OVER (
                                PARTITION BY TOLOWER(business_type_level2) NOT LIKE '%choice%',
                                TOLOWER(business_type_level2) NOT LIKE '%ae%',
                                buyer_id,
                                TO_CHAR(TO_DATE(ds, 'yyyymmdd'), 'yyyymm')
                            )
                            ELSE 0
                        END AS r
                    FROM lazada_cdm.dwd_lzd_trd_core_fulfill_di
                    WHERE 1 = 1
                        AND TO_CHAR(TO_DATE(ds, 'yyyymmdd'), 'yyyymm') = 202408
                        AND venture = 'VN'
                        AND is_revenue = 1
                        AND COALESCE(business_application, 'LZD') IN ('LZD,ZAL', 'LZD')
                )
        ) AS t1
        LEFT JOIN (
            SELECT sales_order_item_id,
                SUM(promotion_amount) AS promotion_amount
            FROM lazada_cdm.dwd_lzd_pro_promotion_item_di
            WHERE 1 = 1
                AND ds >= 20240725
                AND venture = 'VN'
                AND is_fulfilled = 1
                AND TOLOWER(promotion_role) IN ('platform')
                AND (
                    TOLOWER(retail_sponsor) IN ('platform')
                    OR retail_sponsor IS NULL
                )
                AND promotion_id IN (
                    SELECT promotion_id
                    FROM t_dim_promo
                    WHERE 1 = 1
                        AND promo_mechanics = '03_vcm_ui_ops'
                )
            GROUP BY sales_order_item_id
        ) AS t2 ON t1.sales_order_item_id = t2.sales_order_item_id
        LEFT JOIN (
            SELECT sales_order_item_id,
                SUM(
                    ABS(
                        COALESCE(KEYVALUE(exp_comm_amt_detail, 'LPI'), 0)
                    )
                ) AS vcm_slr_amt
            FROM lazada_ent_cdm.dwd_lzd_fin_trd_commission_di
            WHERE 1 = 1
                AND TO_CHAR(TO_DATE(ds, 'yyyymmdd'), 'yyyymm') = 202408
                AND venture = 'VN'
            GROUP BY sales_order_item_id
        ) AS t7 ON t1.sales_order_item_id = t7.sales_order_item_id
        LEFT JOIN (
            SELECT *
            FROM t_user_segment
        ) AS t4 ON TO_CHAR(TO_DATE(t1.ds, 'yyyymmdd'), 'yyyymm') = TO_CHAR(
            DATEADD(TO_DATE(t4.ds_month, 'yyyymm'), + 1, 'mm'),
            'yyyymm'
        )
        AND t1.buyer_id = t4.user_id
        LEFT JOIN (
            SELECT *
            FROM t_dim_camp
        ) AS t5 ON t1.ds = t5.ds
        LEFT JOIN (
            SELECT DISTINCT '02_non_bau' AS camp_type,
                sku_id
            FROM lazada_cdm.dim_lzd_pro_treasurebowl_sku_active
            WHERE 1 = 1
                AND (
                    ds = 20240806
                    OR ds = 20240814
                    OR ds = 20240824
                )
                AND venture = 'VN'
                AND master_campaign_id IN (432710, 432711, 432712) --<< Master campaign Aug
                AND sub_campaign_type IN ('Main Entrance Campaign', 'Copyover')
                AND campaign_sku_status = 2
        ) AS t6 ON t1.sku_id = t6.sku_id
        AND t5.camp_type = t6.camp_type
    GROUP BY CUBE(
            t5.camp_type,
            COALESCE(t4.user_contribution_tag, '05_undefined')
        )
),
t_trn_byr AS (
    SELECT COALESCE(t5.camp_type, '00_full_month') AS camp_type,
        COALESCE(
            COALESCE(t4.user_contribution_tag, '05_undefined'),
            '00_all'
        ) AS user_contribution_tag,
        COUNT(DISTINCT t1.buyer_id) AS mab_any_promo_exc_fsm_full_month
    FROM (
            SELECT *
            FROM lazada_cdm.dwd_lzd_trd_core_fulfill_di
            WHERE 1 = 1
                AND TO_CHAR(TO_DATE(ds, 'yyyymmdd'), 'yyyymm') = 202408
                AND venture = 'VN'
                AND is_revenue = 1
                AND COALESCE(business_application, 'LZD') IN ('LZD,ZAL', 'LZD')
        ) AS t1 LEFT ANTI
        JOIN (
            SELECT t1.buyer_id AS buyer_id,
                MAX(
                    CASE
                        WHEN t2.sales_order_item_id IS NOT NULL THEN 1
                        ELSE 0
                    END
                ) AS is_fsm
            FROM (
                    SELECT *
                    FROM lazada_cdm.dwd_lzd_trd_core_fulfill_di
                    WHERE 1 = 1
                        AND TO_CHAR(TO_DATE(ds, 'yyyymmdd'), 'yyyymm') = 202408
                        AND venture = 'VN'
                        AND is_revenue = 1
                        AND COALESCE(business_application, 'LZD') IN ('LZD,ZAL', 'LZD')
                ) AS t1
                LEFT JOIN (
                    SELECT DISTINCT sales_order_item_id
                    FROM lazada_cdm.dwd_lzd_pro_promotion_item_di
                    WHERE 1 = 1
                        AND ds >= 20240725
                        AND venture = 'VN'
                        AND is_fulfilled = 1
                        AND TOLOWER(promotion_role) IN ('platform')
                        AND (
                            TOLOWER(retail_sponsor) IN ('platform')
                            OR retail_sponsor IS NULL
                        )
                        AND promotion_id IN (
                            SELECT promotion_id
                            FROM t_dim_promo
                            WHERE 1 = 1
                                AND promo_mechanics = '01_fsm_all_ops'
                        )
                ) AS t2 ON t1.sales_order_item_id = t2.sales_order_item_id
            GROUP BY t1.buyer_id
            HAVING MAX(
                    CASE
                        WHEN t2.sales_order_item_id IS NOT NULL THEN 1
                        ELSE 0
                    END
                ) = 1
        ) AS t2 ON t1.buyer_id = t2.buyer_id
        LEFT JOIN (
            SELECT *
            FROM t_user_segment
        ) AS t4 ON TO_CHAR(TO_DATE(t1.ds, 'yyyymmdd'), 'yyyymm') = TO_CHAR(
            DATEADD(TO_DATE(t4.ds_month, 'yyyymm'), + 1, 'mm'),
            'yyyymm'
        )
        AND t1.buyer_id = t4.user_id
        LEFT JOIN (
            SELECT *
            FROM t_dim_camp
        ) AS t5 ON t1.ds = t5.ds
    GROUP BY CUBE(
            t5.camp_type,
            COALESCE(t4.user_contribution_tag, '05_undefined')
        )
)
SELECT camp_type,
    user_contribution_tag,
    SUM(usr_pool) AS usr_pool,
    SUM(mau) AS mau,
    SUM(mau_vcm_voh) AS mau_vcm_voh --
    -- ,SUM(collect_uv) AS collect_uv
,
    SUM(collect_cnt) AS collect_cnt,
    SUM(mab) AS mab,
    SUM(mab_exc_ac_ae_full_month) AS mab_exc_ac_ae_full_month,
    SUM(mab_any_promo_exc_fsm_full_month) AS mab_any_promo_exc_fsm_full_month,
    SUM(mab_vcm_gui) AS mab_vcm_gui,
    SUM(number_redeem_vcm_gui) AS number_redeem_vcm_gui,
    SUM(ord) AS ord,
    SUM(ord_exc_ac_ae) AS ord_exc_ac_ae,
    SUM(ord_vcm_eli) AS ord_vcm_eli,
    SUM(ord_vcm_eli_gui) AS ord_vcm_eli_gui,
    SUM(ord_vcm_eli_un_gui) AS ord_vcm_eli_un_gui,
    SUM(gmv) AS gmv,
    SUM(gmv_exc_ac_ae) AS gmv_exc_ac_ae,
    SUM(gmv_vcm_eli) AS gmv_vcm_eli,
    SUM(gmv_vcm_eli_gui) AS gmv_vcm_eli_gui,
    SUM(gmv_vcm_eli_un_gui) AS gmv_vcm_eli_un_gui,
    SUM(total_promo_amt_ff) AS total_promo_amt_ff,
    SUM(slr_fee_ff) AS slr_fee_ff,
    SUM(plt_promo_amt_ff) AS plt_promo_amt_ff
FROM (
        SELECT camp_type,
            user_contribution_tag,
            usr_pool,
            0 AS mau,
            0 AS mau_vcm_voh --
            -- ,0 AS collect_uv
,
            0 AS collect_cnt,
            0 AS mab,
            0 AS mab_exc_ac_ae_full_month,
            0 AS mab_any_promo_exc_fsm_full_month,
            0 AS mab_vcm_gui,
            0 AS number_redeem_vcm_gui,
            0 AS ord,
            0 AS ord_exc_ac_ae,
            0 AS ord_vcm_eli,
            0 AS ord_vcm_eli_gui,
            0 AS ord_vcm_eli_un_gui,
            0 AS gmv,
            0 AS gmv_exc_ac_ae,
            0 AS gmv_vcm_eli,
            0 AS gmv_vcm_eli_gui,
            0 AS gmv_vcm_eli_un_gui,
            0 AS total_promo_amt_ff,
            0 AS slr_fee_ff,
            0 AS plt_promo_amt_ff
        FROM (
                SELECT COALESCE(
                        COALESCE(user_contribution_tag, '05_undefined'),
                        '00_all'
                    ) AS user_contribution_tag,
                    COUNT(DISTINCT user_id) AS usr_pool
                FROM t_user_segment
                GROUP BY CUBE(COALESCE(user_contribution_tag, '05_undefined'))
            ) LATERAL VIEW EXPLODE(SPLIT('00_full_month,01_bau,02_non_bau', ',')) camp_type AS camp_type
        UNION ALL
        --<< BREAK POINT
        SELECT camp_type,
            user_contribution_tag,
            0 AS usr_pool,
            mau,
            0 AS mau_vcm_voh --
            -- ,0 AS collect_uv
,
            0 AS collect_cnt,
            0 AS mab,
            0 AS mab_exc_ac_ae_full_month,
            0 AS mab_any_promo_exc_fsm_full_month,
            0 AS mab_vcm_gui,
            0 AS number_redeem_vcm_gui,
            0 AS ord,
            0 AS ord_exc_ac_ae,
            0 AS ord_vcm_eli,
            0 AS ord_vcm_eli_gui,
            0 AS ord_vcm_eli_un_gui,
            0 AS gmv,
            0 AS gmv_exc_ac_ae,
            0 AS gmv_vcm_eli,
            0 AS gmv_vcm_eli_gui,
            0 AS gmv_vcm_eli_un_gui,
            0 AS total_promo_amt_ff,
            0 AS slr_fee_ff,
            0 AS plt_promo_amt_ff
        FROM t_trf --
        UNION ALL
        --<< BREAK POINT
        SELECT camp_type,
            user_contribution_tag,
            0 AS usr_pool,
            0 AS mau,
            mau_vcm_voh --
            -- ,0 AS collect_uv
,
            0 AS collect_cnt,
            0 AS mab,
            0 AS mab_exc_ac_ae_full_month,
            0 AS mab_any_promo_exc_fsm_full_month,
            0 AS mab_vcm_gui,
            0 AS number_redeem_vcm_gui,
            0 AS ord,
            0 AS ord_exc_ac_ae,
            0 AS ord_vcm_eli,
            0 AS ord_vcm_eli_gui,
            0 AS ord_vcm_eli_un_gui,
            0 AS gmv,
            0 AS gmv_exc_ac_ae,
            0 AS gmv_vcm_eli,
            0 AS gmv_vcm_eli_gui,
            0 AS gmv_vcm_eli_un_gui,
            0 AS total_promo_amt_ff,
            0 AS slr_fee_ff,
            0 AS plt_promo_amt_ff
        FROM t_trf_voh
        UNION ALL
        --<< BREAK POINT
        SELECT camp_type,
            user_contribution_tag,
            0 AS usr_pool,
            0 AS mau,
            0 AS mau_vcm_voh --
            -- ,0 AS collect_uv
,
            0 AS collect_cnt,
            mab,
            mab_exc_ac_ae_full_month,
            0 AS mab_any_promo_exc_fsm_full_month,
            mab_vcm_gui,
            number_redeem_vcm_gui,
            ord,
            ord_exc_ac_ae,
            ord_vcm_eli,
            ord_vcm_eli_gui,
            ord_vcm_eli_un_gui,
            gmv,
            gmv_exc_ac_ae,
            gmv_vcm_eli,
            gmv_vcm_eli_gui,
            gmv_vcm_eli_un_gui,
            total_promo_amt_ff,
            slr_fee_ff,
            plt_promo_amt_ff
        FROM t_trn
        UNION ALL
        --<< BREAK POINT
        SELECT camp_type,
            user_contribution_tag,
            0 AS usr_pool,
            0 AS mau,
            0 AS mau_vcm_voh --
            -- ,0 AS collect_uv
,
            0 AS collect_cnt,
            0 AS mab,
            0 AS mab_exc_ac_ae_full_month,
            mab_any_promo_exc_fsm_full_month,
            0 AS mab_vcm_gui,
            0 AS number_redeem_vcm_gui,
            0 AS ord,
            0 AS ord_exc_ac_ae,
            0 AS ord_vcm_eli,
            0 AS ord_vcm_eli_gui,
            0 AS ord_vcm_eli_un_gui,
            0 AS gmv,
            0 AS gmv_exc_ac_ae,
            0 AS gmv_vcm_eli,
            0 AS gmv_vcm_eli_gui,
            0 AS gmv_vcm_eli_un_gui,
            0 AS total_promo_amt_ff,
            0 AS slr_fee_ff,
            0 AS plt_promo_amt_ff
        FROM t_trn_byr
        UNION ALL
        --<< BREAK POINT
        SELECT camp_type,
            user_contribution_tag,
            0 AS usr_pool,
            0 AS mau,
            0 AS mau_vcm_voh --
            -- ,collect_uv
,
            collect_cnt,
            0 AS mab,
            0 AS mab_exc_ac_ae_full_month,
            0 AS mab_any_promo_exc_fsm_full_month,
            0 AS mab_vcm_gui,
            0 AS number_redeem_vcm_gui,
            0 AS ord,
            0 AS ord_exc_ac_ae,
            0 AS ord_vcm_eli,
            0 AS ord_vcm_eli_gui,
            0 AS ord_vcm_eli_un_gui,
            0 AS gmv,
            0 AS gmv_exc_ac_ae,
            0 AS gmv_vcm_eli,
            0 AS gmv_vcm_eli_gui,
            0 AS gmv_vcm_eli_un_gui,
            0 AS total_promo_amt_ff,
            0 AS slr_fee_ff,
            0 AS plt_promo_amt_ff
        FROM t_collect
    )
GROUP BY camp_type,
    user_contribution_tag;
SELECT t1.promotion_id AS promotion_id,
    COALESCE(t2.user_contribution_tag, '05_undefined') AS user_contribution_tag,
    SUM(COALESCE(t1.total_value, 0)) AS total_value
FROM (
        SELECT t1.*,
            COALESCE(t2.total_value, 0) AS total_value
        FROM (
                SELECT promotion_id,
                    voucher_name,
                    description,
                    promotion_start_date,
                    promotion_end_date,
                    product_code,
                    product_sub_code,
                    prom_type,
                    prom_sub_type,
                    features,
                    coupon_value,
                    min_order_value,
                    promotion_cap_value,
                    '03_vcm_ui_ops' AS promo_mechanics,
                    CAST(
                        REGEXP_EXTRACT(
                            KEYVALUE(features, ',', ':', '"voucherSubType"'),
                            '\"([0-9]*)\"'
                        ) AS BIGINT
                    ) AS vouchersubtype,
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
                    END AS voucher_scheme,
                    CASE
                        WHEN voucher_type IN ('percent_amount') THEN CASE
                            WHEN CAST(
                                COALESCE(promotion_cap_value, 0) / 1000 AS BIGINT
                            ) IN (1000) THEN 'Campaign bundle - Ultra high' --<< Campaign bundle section logic
                            WHEN CAST(
                                COALESCE(promotion_cap_value, 0) / 1000 AS BIGINT
                            ) IN (400) THEN 'Campaign bundle - High'
                            WHEN CAST(
                                COALESCE(promotion_cap_value, 0) / 1000 AS BIGINT
                            ) IN (100) THEN 'Campaign bundle - Mid'
                            WHEN CAST(
                                COALESCE(promotion_cap_value, 0) / 1000 AS BIGINT
                            ) IN (30) THEN 'Campaign bundle - Low'
                            WHEN CAST(
                                COALESCE(promotion_cap_value, 0) / 1000 AS BIGINT
                            ) IN (75) THEN 'Daily bundle - High' --<< Daily bundle section logic
                            WHEN CAST(
                                COALESCE(promotion_cap_value, 0) / 1000 AS BIGINT
                            ) IN (15) THEN 'Daily bundle - Mid'
                            WHEN CAST(
                                COALESCE(promotion_cap_value, 0) / 1000 AS BIGINT
                            ) IN (10) THEN 'Daily bundle - Low'
                            ELSE 'Other'
                        END
                        ELSE 'Other'
                    END AS voucher_tier,
                    CASE
                        WHEN voucher_type IN ('percent_amount') THEN CASE
                            WHEN CAST(
                                COALESCE(promotion_cap_value, 0) / 1000 AS BIGINT
                            ) IN (1000, 400, 100, 30) THEN 'non Bau'
                            WHEN CAST(
                                COALESCE(promotion_cap_value, 0) / 1000 AS BIGINT
                            ) IN (75, 15, 10) THEN 'Bau'
                            ELSE 'Other'
                        END
                        ELSE 'Other'
                    END AS camp_type
                FROM lazada_cdm.dim_lzd_pro_collectibles
                WHERE 1 = 1
                    AND ds = MAX_PT('lazada_cdm.dim_lzd_pro_collectibles')
                    AND TO_CHAR(promotion_start_date, 'yyyymmdd') >= 20240801
                    AND TO_CHAR(promotion_end_date, 'yyyymmdd') <= 20240831
                    AND status = 1
                    AND venture = 'VN'
                    AND REGEXP_INSTR(voucher_name, '^.*(TEST|Test|test)+.*') = 0 --<< Clear data
                    AND sponsor = 'platform'
                    AND TOLOWER(COALESCE(retail_sponsor, 'platform')) IN ('platform')
                    AND product_code IN ('categoryCoupon', 'collectibleCoupon')
                    AND (
                        TOLOWER(voucher_name) LIKE '%voucher%bonus%'
                        OR TOLOWER(description) LIKE '%voucher%bonus%'
                    )
            ) AS t1
            LEFT JOIN (
                SELECT *
                FROM lazada_ods.s_promotion_budget_vn
                WHERE 1 = 1
                    AND ds = MAX_PT('lazada_ods.s_promotion_budget_vn')
                    AND budget_type = 1
            ) AS t2 ON t1.promotion_id = t2.promotion_id
    ) AS t1
    LEFT JOIN (
        SELECT *,
            CASE
                WHEN byr_cnt = max_r THEN 1
                ELSE 0
            END AS flag
        FROM (
                SELECT *,
                    MAX(byr_cnt) OVER (PARTITION BY promotion_id) AS max_r
                FROM (
                        SELECT COALESCE(t2.user_contribution_tag, '05_undefined') AS user_contribution_tag,
                            t1.promotion_id,
                            COUNT(DISTINCT t1.buyer_id) AS byr_cnt
                        FROM (
                                SELECT *
                                FROM lazada_cdm.dwd_lzd_pro_voucher_collect_di
                                WHERE 1 = 1
                                    AND TO_CHAR(TO_DATE(ds, 'yyyymmdd'), 'yyyymm') = 202408
                                    AND venture = 'VN'
                                    AND promotion_id IN (
                                        SELECT promotion_id
                                        FROM lazada_cdm.dim_lzd_pro_collectibles
                                        WHERE 1 = 1
                                            AND ds = MAX_PT('lazada_cdm.dim_lzd_pro_collectibles')
                                            AND TO_CHAR(promotion_start_date, 'yyyymmdd') >= 20240801
                                            AND TO_CHAR(promotion_end_date, 'yyyymmdd') <= 20240831
                                            AND status = 1
                                            AND venture = 'VN'
                                            AND REGEXP_INSTR(voucher_name, '^.*(TEST|Test|test)+.*') = 0 --<< Clear data
                                            AND sponsor = 'platform'
                                            AND TOLOWER(COALESCE(retail_sponsor, 'platform')) IN ('platform')
                                            AND product_code IN ('categoryCoupon', 'collectibleCoupon')
                                            AND (
                                                TOLOWER(voucher_name) LIKE '%voucher%bonus%'
                                                OR TOLOWER(description) LIKE '%voucher%bonus%'
                                            )
                                    )
                            ) AS t1
                            LEFT JOIN (
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
                                WHERE 1 = 1
                                    AND ds_month = 202407
                                    AND venture = 'VN'
                            ) AS t2 ON t1.buyer_id = t2.user_id
                        GROUP BY COALESCE(t2.user_contribution_tag, '05_undefined'),
                            t1.promotion_id
                        ORDER BY t1.promotion_id ASC
                    )
            )
        WHERE 1 = 1
            AND CASE
                WHEN byr_cnt = max_r THEN 1
                ELSE 0
            END = 1
    ) AS t2 ON t1.promotion_id = t2.promotion_id
GROUP BY t1.promotion_id,
    COALESCE(t2.user_contribution_tag, '05_undefined');
----- Mov view
WITH t_asov_tag AS (
    SELECT order_number,
        CASE
            WHEN order_number_base_value >= 0
            AND order_number_base_value < 50000 THEN '[0,50)'
            WHEN order_number_base_value >= 50000
            AND order_number_base_value < 100000 THEN '[50,100)'
            WHEN order_number_base_value >= 100000
            AND order_number_base_value < 150000 THEN '[100,150)'
            WHEN order_number_base_value >= 150000
            AND order_number_base_value < 200000 THEN '[150,200)'
            WHEN order_number_base_value >= 200000
            AND order_number_base_value < 250000 THEN '[200,250)'
            WHEN order_number_base_value >= 250000
            AND order_number_base_value < 300000 THEN '[250,300)'
            WHEN order_number_base_value >= 300000
            AND order_number_base_value < 350000 THEN '[300,350)'
            WHEN order_number_base_value >= 350000
            AND order_number_base_value < 400000 THEN '[350,400)'
            WHEN order_number_base_value >= 400000
            AND order_number_base_value < 450000 THEN '[400,450)'
            WHEN order_number_base_value >= 450000
            AND order_number_base_value < 500000 THEN '[450,500)'
            WHEN order_number_base_value >= 500000
            AND order_number_base_value < 1000000 THEN '[500,1000)'
            WHEN order_number_base_value >= 1000000
            AND order_number_base_value < 2000000 THEN '[1000,2000)'
            WHEN order_number_base_value >= 2000000 THEN '[2000,+)'
        END AS asov_range_tag,
        unit_price,
        discount_amount_by_seller,
        flexi_combo_discount_amount,
        order_number_base_value
    FROM (
            SELECT order_number,
                SUM(unit_price) AS unit_price,
                SUM(discount_amount_by_seller) AS discount_amount_by_seller,
                SUM(flexi_combo_discount_amount) AS flexi_combo_discount_amount,
                SUM(
                    unit_price - discount_amount_by_seller - flexi_combo_discount_amount
                ) AS order_number_base_value
            FROM lazada_cdm.dwd_lzd_trd_core_fulfill_di
            WHERE 1 = 1
                AND TO_CHAR(TO_DATE(ds, 'yyyymmdd'), 'yyyymm') = 202410
                AND venture = 'VN'
                AND is_revenue = 1
                AND COALESCE(business_application, 'LZD') IN ('LZD,ZAL', 'LZD')
            GROUP BY order_number
        )
)
SELECT *
FROM t_asov_tag
WHERE 1 = 1
    AND asov_range_tag IS NULL;
-------------------------------------------------------------------------------------
-- DROP TABLE IF EXISTS lazada_analyst_dev.tmp_loutruong_vcm_optimization_cohort
-- ;
-- CREATE TABLE IF NOT EXISTS lazada_analyst_dev.tmp_loutruong_vcm_optimization_cohort
-- LIFECYCLE 30 AS
WITH t_dim_slr_master AS (
    SELECT t1.seller_id,
        t1.seller_short_code,
        t1.seller_name,
        t1.bu_lv1,
        t1.bu_lv2,
        t1.main_industry,
        t1.main_cat_lv1,
        t2.new_seller_segment AS segment
    FROM (
            SELECT seller_id,
                short_code AS seller_short_code,
                seller_name,
                business_type AS bu_lv1,
                business_type_level2 AS bu_lv2,
                industry_name AS main_industry,
                main_category_name AS main_cat_lv1
            FROM lazada_cdm.dim_lzd_slr_seller_vn
            WHERE 1 = 1
                AND ds = 20241117
                AND venture = 'VN'
        ) AS t1
        LEFT JOIN (
            SELECT ext_num_id AS seller_id,
                MAX(new_seller_segment) AS new_seller_segment,
                MAX(pic_lead_name) AS pic_lead_name
            FROM lazada_analyst_dev.vn_map_memid_slrid
            WHERE 1 = 1
                AND date_ = 20241117
            GROUP BY ext_num_id
        ) AS t2 ON t1.seller_id = t2.seller_id
),
t_dim_fsm AS (
    SELECT seller_id,
        MAX(
            CASE
                WHEN mm IN ('202408')
                AND duration IN (30, 31) THEN 1
                ELSE 0
            END
        ) AS before_fsm --<< Filter join duration
,
        MAX(
            CASE
                WHEN mm IN ('202410')
                AND duration IN (30, 31) THEN 1
                ELSE 0
            END
        ) AS after_fsm --<< Filter join duration
    FROM (
            SELECT mm,
                seller_id,
                COUNT(DISTINCT ds) AS duration
            FROM (
                    SELECT TO_CHAR(TO_DATE(t1.ds, 'yyyymmdd'), 'yyyymm') AS mm,
                        t1.ds,
                        t1.seller_id,
                        t1.program_id,
                        t2.program_name,
                        t2.program_description,
                        t2.program_begin_time,
                        t2.program_end_time,
                        t2.program_status,
                        t1.seller_status,
                        CASE
                            WHEN t1.ds BETWEEN t2.program_begin_time AND t2.program_end_time THEN t1.seller_status
                            ELSE '06. Program expired'
                        END AS status
                    FROM (
                            SELECT ds,
                                program_id,
                                seller_id,
                                CASE
                                    WHEN status IN (0) THEN '01. Invite'
                                    WHEN status IN (1) THEN '02. Join'
                                    WHEN status IN (2) THEN '03. Exit'
                                    WHEN status IN (3) THEN '04. Replace join to quit'
                                    WHEN status IN (4) THEN '05. Replace invite to quit'
                                    ELSE '07. Unknown seller status'
                                END AS seller_status
                            FROM lazada_ds.ds_lzd_program_seller
                            WHERE 1 = 1
                                AND TO_CHAR(TO_DATE(ds, 'yyyymmdd'), 'yyyymm') IN (202408, 202410)
                                AND venture = 'VN'
                        ) AS t1
                        INNER JOIN (
                            SELECT ds,
                                id AS program_id,
                                name AS program_name,
                                description AS program_description,
                                status AS program_status,
                                TO_CHAR(
                                    sg_udf :epoch_to_timezone(begin_time, venture),
                                    'yyyymmdd'
                                ) AS program_begin_time,
                                TO_CHAR(
                                    sg_udf :epoch_to_timezone(end_time, venture),
                                    'yyyymmdd'
                                ) AS program_end_time
                            FROM lazada_ds.ds_lzd_program
                            WHERE 1 = 1
                                AND TO_CHAR(TO_DATE(ds, 'yyyymmdd'), 'yyyymm') IN (202408, 202410)
                                AND venture = 'VN'
                                AND program_type = 1
                                AND is_test = 0
                                AND REGEXP_INSTR(name, '^.*(TEST|Test|test)+.*') = 0
                        ) AS t2 ON t1.ds = t2.ds
                        AND t1.program_id = t2.program_id
                )
            WHERE 1 = 1
                AND status IN ('02. Join')
            GROUP BY mm,
                seller_id
        )
    GROUP BY seller_id
),
t_dim_lpi_vcm AS (
    SELECT seller_id,
        SUM(before_lpi) AS before_lpi,
        SUM(after_vcm) AS after_vcm
    FROM (
            SELECT DISTINCT seller_id,
                1 AS before_lpi,
                0 AS after_vcm
            FROM lazada_cdm.dim_lzd_pro_treasurebowl_sku_active
            WHERE 1 = 1
                AND TO_CHAR(TO_DATE(ds, 'yyyymmdd'), 'yyyymm') IN (202408)
                AND venture = 'VN'
                AND master_campaign_id IN (432710, 432711, 432712) --<< Master campaign Aug
                AND sub_campaign_type IN ('Main Entrance Campaign', 'Copyover')
                AND campaign_sku_status = 2
                AND seller_id NOT IN (
                    SELECT seller_id
                    FROM t_dim_slr_master
                    WHERE 1 = 1
                        AND (
                            TOLOWER(bu_lv2) LIKE '%choice%'
                            OR TOLOWER(bu_lv2) LIKE '%ae%'
                        )
                    UNION ALL
                    SELECT CAST(seller_id AS BIGINT) AS seller_id
                    FROM lazada_analyst_dev.loutruong_slr_non_trusted_dbs
                ) --
            UNION ALL
            --<< BREAK POINT
            SELECT DISTINCT seller_id,
                0 AS before_lpi,
                1 AS after_vcm
            FROM lazada_ds.ds_lzd_program_seller
            WHERE 1 = 1
                AND TO_CHAR(TO_DATE(ds, 'yyyymmdd'), 'yyyymm') IN (202410)
                AND venture = 'VN'
                AND program_id IN (1534, 1564) --<< Oct, Nov
                AND status IN (1) --
            UNION ALL
            --<< BREAK POINT
            SELECT DISTINCT seller_id,
                0 AS before_lpi,
                1 AS after_vcm
            FROM lazada_cdm.dim_lzd_slr_seller_vn
            WHERE 1 = 1
                AND TO_CHAR(TO_DATE(ds, 'yyyymmdd'), 'yyyymm') IN (202410)
                AND venture = 'VN'
                AND is_mp3_seller = 1
        )
    GROUP BY seller_id
),
t_dim_slr_grp AS (
    SELECT mm,
        seller_id,
        seller_short_code,
        seller_name,
        bu_lv1,
        bu_lv2,
        main_industry,
        main_cat_lv1,
        segment,
        before_fsm,
        after_fsm,
        before_lpi,
        after_vcm,
        seller_group
    FROM (
            SELECT *
            FROM (
                    SELECT t1.seller_id,
                        t1.seller_short_code,
                        t1.seller_name,
                        t1.bu_lv1,
                        t1.bu_lv2,
                        t1.main_industry,
                        t1.main_cat_lv1,
                        t1.segment,
                        COALESCE(t2.before_fsm, 0) AS before_fsm,
                        COALESCE(t2.after_fsm, 0) AS after_fsm,
                        COALESCE(t3.before_lpi, 0) AS before_lpi,
                        COALESCE(t3.after_vcm, 0) AS after_vcm,
                        CASE
                            WHEN COALESCE(t2.before_fsm, 0) = 1
                            AND COALESCE(t2.after_fsm, 0) = 1
                            AND COALESCE(t3.before_lpi, 0) = 1
                            AND COALESCE(t3.after_vcm, 0) = 1 THEN 'Group 1 - Has LPI/VCM'
                            WHEN COALESCE(t2.before_fsm, 0) = 1
                            AND COALESCE(t2.after_fsm, 0) = 1
                            AND COALESCE(t3.before_lpi, 0) = 1
                            AND COALESCE(t3.after_vcm, 0) = 0 THEN 'Group 2 - Has LPI'
                            WHEN COALESCE(t2.before_fsm, 0) = 1
                            AND COALESCE(t2.after_fsm, 0) = 1
                            AND COALESCE(t3.before_lpi, 0) = 0
                            AND COALESCE(t3.after_vcm, 0) = 1 THEN 'Group 3 - Has VCM'
                            WHEN COALESCE(t2.before_fsm, 0) = 1
                            AND COALESCE(t2.after_fsm, 0) = 1
                            AND COALESCE(t3.before_lpi, 0) = 0
                            AND COALESCE(t3.after_vcm, 0) = 0 THEN 'Group 4 - Non LPI/VCM'
                            ELSE 'Unknown'
                        END AS seller_group
                    FROM (
                            SELECT *
                            FROM t_dim_slr_master
                        ) AS t1
                        LEFT JOIN (
                            SELECT *
                            FROM t_dim_fsm
                        ) AS t2 ON t1.seller_id = t2.seller_id
                        LEFT JOIN (
                            SELECT *
                            FROM t_dim_lpi_vcm
                        ) AS t3 ON t1.seller_id = t3.seller_id
                )
            WHERE 1 = 1
                AND seller_group <> 'Unknown'
        ) LATERAL VIEW EXPLODE(SPLIT('T-1,T0', ',')) mm AS mm
),
t_trf AS (
    SELECT t1.mm,
        t1.seller_id,
        t1.seller_short_code,
        t1.seller_name,
        t1.bu_lv1,
        t1.bu_lv2,
        t1.main_industry,
        t1.main_cat_lv1,
        t1.segment,
        t1.seller_group,
        SUM(
            CASE
                WHEN t2.url_type IN ('ipv') THEN NVL(t2.pv_count, 0)
                ELSE 0
            END
        ) AS app_pdp_pv
    FROM (
            SELECT *
            FROM t_dim_slr_grp
        ) AS t1
        LEFT JOIN (
            SELECT CASE
                    WHEN TO_CHAR(TO_DATE(ds, 'yyyymmdd'), 'yyyymm') IN (202408) THEN 'T-1'
                    WHEN TO_CHAR(TO_DATE(ds, 'yyyymmdd'), 'yyyymm') IN (202410) THEN 'T0'
                END AS mm,
                *
            FROM lazada_cdm.dws_lzd_mkt_app_ad_mp_di
            WHERE 1 = 1
                AND TO_CHAR(TO_DATE(ds, 'yyyymmdd'), 'yyyymm') IN (202408, 202410)
                AND venture = 'VN'
                AND attr_model IN ('ft_1d_np')
                AND utdid IS NOT NULL
        ) AS t2 ON t1.mm = t2.mm
        AND t1.seller_id = t2.seller_id
    GROUP BY t1.mm,
        t1.seller_id,
        t1.seller_short_code,
        t1.seller_name,
        t1.bu_lv1,
        t1.bu_lv2,
        t1.main_industry,
        t1.main_cat_lv1,
        t1.segment,
        t1.seller_group
),
t_trn AS (
    SELECT t1.mm,
        t1.seller_id,
        t1.seller_short_code,
        t1.seller_name,
        t1.bu_lv1,
        t1.bu_lv2,
        t1.main_industry,
        t1.main_cat_lv1,
        t1.segment,
        t1.seller_group,
        COUNT(DISTINCT t2.order_id) AS ord_cnt,
        COUNT(
            DISTINCT CASE
                WHEN COALESCE(t3.is_fsm_promo, 0) = 1 THEN t2.order_id
                ELSE NULL
            END
        ) AS ord_fsm_cnt,
        COUNT(
            DISTINCT CASE
                WHEN COALESCE(t3.is_lpi_vcm_promo, 0) = 1 THEN t2.order_id
                ELSE NULL
            END
        ) AS ord_lpi_vcm_cnt,
        SUM(
            COALESCE(t2.actual_gmv, 0) * COALESCE(t2.exchange_rate, 0)
        ) AS gmv,
        SUM(
            CASE
                WHEN COALESCE(t3.is_fsm_promo, 0) = 1 THEN COALESCE(t2.actual_gmv, 0) * COALESCE(t2.exchange_rate, 0)
                ELSE 0
            END
        ) AS gmv_fsm,
        SUM(
            CASE
                WHEN COALESCE(t3.is_lpi_vcm_promo, 0) = 1 THEN COALESCE(t2.actual_gmv, 0) * COALESCE(t2.exchange_rate, 0)
                ELSE 0
            END
        ) AS gmv_lpi_vcm,
        SUM(
            COALESCE(t3.total_total_promo_amt, 0) * COALESCE(t2.exchange_rate, 0)
        ) AS total_total_promo_amt,
        SUM(
            COALESCE(t3.total_fsm_promo_amt, 0) * COALESCE(t2.exchange_rate, 0)
        ) AS total_fsm_promo_amt,
        SUM(
            COALESCE(t3.total_lpi_vcm_promo_amt, 0) * COALESCE(t2.exchange_rate, 0)
        ) AS total_lpi_vcm_promo_amt,
        SUM(
            COALESCE(t4.fsm_slr_amt, 0) * COALESCE(t2.exchange_rate, 0)
        ) AS fsm_slr_amt,
        SUM(
            COALESCE(t4.lpi_vcm_slr_amt, 0) * COALESCE(t2.exchange_rate, 0)
        ) AS lpi_vcm_slr_amt
    FROM (
            SELECT *
            FROM t_dim_slr_grp
        ) AS t1
        LEFT JOIN (
            SELECT CASE
                    WHEN TO_CHAR(TO_DATE(ds, 'yyyymmdd'), 'yyyymm') IN (202408) THEN 'T-1'
                    WHEN TO_CHAR(TO_DATE(ds, 'yyyymmdd'), 'yyyymm') IN (202410) THEN 'T0'
                END AS mm,
                *
            FROM lazada_cdm.dwd_lzd_trd_core_fulfill_di
            WHERE 1 = 1
                AND TO_CHAR(TO_DATE(ds, 'yyyymmdd'), 'yyyymm') IN (202408, 202410)
                AND venture = 'VN'
                AND is_revenue = 1
                AND COALESCE(business_application, 'LZD') IN ('LZD,ZAL', 'LZD')
        ) AS t2 ON t1.mm = t2.mm
        AND t1.seller_id = t2.seller_id
        LEFT JOIN (
            SELECT t1.sales_order_item_id,
                MAX(
                    CASE
                        WHEN TOLOWER(t1.promotion_role) IN ('platform')
                        AND (
                            TOLOWER(t1.retail_sponsor) IN ('platform')
                            OR t1.retail_sponsor IS NULL
                        )
                        AND t2.prom_sub_type IN ('FS Max') THEN 1
                        ELSE 0
                    END
                ) AS is_fsm_promo,
                MAX(
                    CASE
                        WHEN TOLOWER(t1.promotion_role) IN ('platform')
                        AND (
                            TOLOWER(t1.retail_sponsor) IN ('platform')
                            OR t1.retail_sponsor IS NULL
                        )
                        AND (
                            (
                                t2.product_code IN ('categoryCoupon', 'collectibleCoupon')
                                AND (
                                    TOLOWER(t1.promotion_name) LIKE '%voucher%bonus%'
                                    OR TOLOWER(t2.voucher_name) LIKE '%voucher%bonus%'
                                    OR TOLOWER(t2.description) LIKE '%voucher%bonus%'
                                )
                            )
                            OR (
                                CAST(
                                    REGEXP_EXTRACT(
                                        KEYVALUE(t2.features, ',', ':', '"voucherSubType"'),
                                        '\"([0-9]*)\"'
                                    ) AS BIGINT
                                ) IN (7)
                                OR (
                                    t1.promotion_type IN ('categoryCoupon')
                                    AND (
                                        TOLOWER(t1.promotion_name) LIKE '%platform_voucher max%'
                                        OR TOLOWER(t1.promotion_name) LIKE '%voucher%max%'
                                        OR TOLOWER(t2.voucher_name) LIKE '%platform_voucher max%'
                                        OR TOLOWER(t2.voucher_name) LIKE '%voucher%max%'
                                        OR TOLOWER(t2.description) LIKE '%platform_voucher max%'
                                        OR TOLOWER(t2.description) LIKE '%voucher%max%'
                                    )
                                )
                            )
                        )
                        AND TOLOWER(t2.voucher_name) NOT LIKE '%day%' THEN 1
                        ELSE 0
                    END
                ) AS is_lpi_vcm_promo,
                SUM(t1.promotion_amount) AS total_total_promo_amt,
                SUM(
                    CASE
                        WHEN TOLOWER(t1.promotion_role) IN ('platform')
                        AND (
                            TOLOWER(t1.retail_sponsor) IN ('platform')
                            OR t1.retail_sponsor IS NULL
                        )
                        AND t2.prom_sub_type IN ('FS Max') THEN t1.promotion_amount
                        ELSE 0
                    END
                ) AS total_fsm_promo_amt,
                SUM(
                    CASE
                        WHEN TOLOWER(t1.promotion_role) IN ('platform')
                        AND (
                            TOLOWER(t1.retail_sponsor) IN ('platform')
                            OR t1.retail_sponsor IS NULL
                        )
                        AND (
                            (
                                t2.product_code IN ('categoryCoupon', 'collectibleCoupon')
                                AND (
                                    TOLOWER(t1.promotion_name) LIKE '%voucher%bonus%'
                                    OR TOLOWER(t2.voucher_name) LIKE '%voucher%bonus%'
                                    OR TOLOWER(t2.description) LIKE '%voucher%bonus%'
                                )
                            )
                            OR (
                                CAST(
                                    REGEXP_EXTRACT(
                                        KEYVALUE(t2.features, ',', ':', '"voucherSubType"'),
                                        '\"([0-9]*)\"'
                                    ) AS BIGINT
                                ) IN (7)
                                OR (
                                    t1.promotion_type IN ('categoryCoupon')
                                    AND (
                                        TOLOWER(t1.promotion_name) LIKE '%platform_voucher max%'
                                        OR TOLOWER(t1.promotion_name) LIKE '%voucher%max%'
                                        OR TOLOWER(t2.voucher_name) LIKE '%platform_voucher max%'
                                        OR TOLOWER(t2.voucher_name) LIKE '%voucher%max%'
                                        OR TOLOWER(t2.description) LIKE '%platform_voucher max%'
                                        OR TOLOWER(t2.description) LIKE '%voucher%max%'
                                    )
                                )
                            )
                        )
                        AND TOLOWER(t2.voucher_name) NOT LIKE '%day%' THEN t1.promotion_amount
                        ELSE 0
                    END
                ) AS total_lpi_vcm_promo_amt
            FROM (
                    SELECT *
                    FROM lazada_cdm.dwd_lzd_pro_promotion_item_di
                    WHERE 1 = 1
                        AND ds >= 20240701
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
        ) AS t3 ON t2.sales_order_item_id = t3.sales_order_item_id
        LEFT JOIN (
            SELECT sales_order_item_id,
                MAX(
                    CASE
                        WHEN ABS(
                            COALESCE(KEYVALUE(exp_comm_amt_detail, 'FS_MAX'), 0)
                        ) > 0 THEN 1
                        ELSE 0
                    END
                ) AS is_fsm_slr,
                MAX(
                    CASE
                        WHEN ABS(
                            COALESCE(KEYVALUE(exp_comm_amt_detail, 'LPI'), 0)
                        ) > 0 THEN 1
                        ELSE 0
                    END
                ) AS is_lpi_vcm_slr,
                SUM(
                    ABS(
                        COALESCE(KEYVALUE(exp_comm_amt_detail, 'FS_MAX'), 0)
                    )
                ) AS fsm_slr_amt,
                SUM(
                    ABS(
                        COALESCE(KEYVALUE(exp_comm_amt_detail, 'LPI'), 0)
                    )
                ) AS lpi_vcm_slr_amt
            FROM lazada_ent_cdm.dwd_lzd_fin_trd_commission_di
            WHERE 1 = 1
                AND TO_CHAR(TO_DATE(ds, 'yyyymmdd'), 'yyyymm') IN (202408, 202410)
                AND venture = 'VN'
            GROUP BY sales_order_item_id
        ) AS t4 ON t2.sales_order_item_id = t4.sales_order_item_id
    GROUP BY t1.mm,
        t1.seller_id,
        t1.seller_short_code,
        t1.seller_name,
        t1.bu_lv1,
        t1.bu_lv2,
        t1.main_industry,
        t1.main_cat_lv1,
        t1.segment,
        t1.seller_group
),
t_final AS (
    SELECT mm,
        seller_id,
        seller_short_code,
        seller_name,
        bu_lv1,
        bu_lv2,
        main_industry,
        main_cat_lv1,
        segment,
        seller_group,
        SUM(app_pdp_pv) AS app_pdp_pv,
        SUM(ord_cnt) AS ord_cnt,
        SUM(ord_fsm_cnt) AS ord_fsm_cnt,
        SUM(ord_lpi_vcm_cnt) AS ord_lpi_vcm_cnt,
        SUM(gmv) AS gmv,
        SUM(gmv_fsm) AS gmv_fsm,
        SUM(gmv_lpi_vcm) AS gmv_lpi_vcm,
        SUM(total_total_promo_amt) AS total_total_promo_amt,
        SUM(total_fsm_promo_amt) AS total_fsm_promo_amt,
        SUM(total_lpi_vcm_promo_amt) AS total_lpi_vcm_promo_amt,
        SUM(fsm_slr_amt) AS fsm_slr_amt,
        SUM(lpi_vcm_slr_amt) AS lpi_vcm_slr_amt
    FROM (
            SELECT mm,
                seller_id,
                seller_short_code,
                seller_name,
                bu_lv1,
                bu_lv2,
                main_industry,
                main_cat_lv1,
                segment,
                seller_group,
                app_pdp_pv,
                0 AS ord_cnt,
                0 AS ord_fsm_cnt,
                0 AS ord_lpi_vcm_cnt,
                0 AS gmv,
                0 AS gmv_fsm,
                0 AS gmv_lpi_vcm,
                0 AS total_total_promo_amt,
                0 AS total_fsm_promo_amt,
                0 AS total_lpi_vcm_promo_amt,
                0 AS fsm_slr_amt,
                0 AS lpi_vcm_slr_amt
            FROM t_trf
            UNION ALL
            --<< BREAK POINT
            SELECT mm,
                seller_id,
                seller_short_code,
                seller_name,
                bu_lv1,
                bu_lv2,
                main_industry,
                main_cat_lv1,
                segment,
                seller_group,
                0 AS app_pdp_pv,
                ord_cnt,
                ord_fsm_cnt,
                ord_lpi_vcm_cnt,
                gmv,
                gmv_fsm,
                gmv_lpi_vcm,
                total_total_promo_amt,
                total_fsm_promo_amt,
                total_lpi_vcm_promo_amt,
                fsm_slr_amt,
                lpi_vcm_slr_amt
            FROM t_trn
        )
    GROUP BY mm,
        seller_id,
        seller_short_code,
        seller_name,
        bu_lv1,
        bu_lv2,
        main_industry,
        main_cat_lv1,
        segment,
        seller_group
)
SELECT mm,
    CAST(seller_id AS STRING) AS seller_id,
    CASE
        WHEN ord_cnt > 0 THEN CAST(seller_id AS STRING)
        ELSE ''
    END AS seller_id_selling,
    seller_short_code,
    seller_name,
    bu_lv1,
    bu_lv2,
    main_industry,
    main_cat_lv1,
    segment,
    seller_group,
    app_pdp_pv,
    ord_cnt,
    ord_fsm_cnt,
    ord_lpi_vcm_cnt,
    gmv,
    gmv_fsm,
    gmv_lpi_vcm,
    total_total_promo_amt,
    total_fsm_promo_amt,
    total_lpi_vcm_promo_amt,
    fsm_slr_amt,
    lpi_vcm_slr_amt
FROM t_final;
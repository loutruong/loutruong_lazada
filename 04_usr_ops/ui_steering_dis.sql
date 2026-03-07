-- MaxCompute SQL 
-- ********************************************************************--
-- author:Truong, Van Thanh
-- create time:2024-08-05 13:07:34
-- ********************************************************************--
-- ********************************************************************--
-- UI steering clm 
-- ********************************************************************--
--@@ Input = lazada_cdm.dwd_lzd_trd_core_fulfill_di
--@@ Input = lazada_cdm.dwd_lzd_pro_promotion_item_di
--@@ Input = lazada_cdm.dwd_lzd_trd_fulfill_spending_di
--@@ Input = lazada_analyst.vc_cost_share
--@@ Input = lazada_cdm.dim_lzd_pro_collectibles
--@@ Input = lazada_cdm.dim_lzd_pro_voucher_rule
--@@ Input = lazada_cdm.dim_lzd_usr
--@@ Input = lazada_ads.dws_lzd_cdp_usr_uid_contribute_value_v3_day
--@@ Input = lazada_ads.ads_lzd_cdp_usr_uid_contribute_user_v3_1d
--@@ Input = lazada_ads.dws_lzd_cdp_usr_uid_contribute_value_v3
WITH t_trd_core_master AS (
    SELECT t1.ds AS ds,
        t1.sales_order_item_id AS sales_order_item_id,
        t1.order_id AS order_id,
        t1.order_number AS order_number,
        t1.check_out_id AS check_out_id,
        t1.buyer_id AS buyer_id,
        COALESCE(t4.user_contribution_tag, '05_undefined') AS user_contribution_tag,
        t1.item_status_esm AS item_status_esm,
        t1.business_type AS bu_lv1,
        t1.business_type_level2 AS bu_lv2,
        t1.industry_name AS industry,
        t1.regional_category1_name AS cat_lv1,
        t1.delivery_company AS delivery_company,
        t1.shipping_region1_name AS delivery_location,
        CASE
            WHEN COALESCE(t2.is_fsm_promo, 0) = 1
            OR COALESCE(t2.is_lpi_promo, 0) = 1
            OR COALESCE(t2.is_vcm_promo, 0) = 1 THEN 1
            ELSE 0
        END AS is_any_promo,
        COALESCE(t2.is_fsm_promo, 0) AS is_fsm_promo,
        COALESCE(t2.is_lpi_promo, 0) AS is_lpi_promo,
        COALESCE(t2.is_vcm_promo, 0) AS is_vcm_promo,
        CASE
            WHEN COALESCE(t3.is_fsm_slr, 0) = 1
            OR COALESCE(t3.is_lpi_slr, 0) = 1
            OR COALESCE(t3.is_vcm_slr, 0) = 1 THEN 1
            ELSE 0
        END AS is_any_slr,
        COALESCE(t3.is_fsm_slr, 0) AS is_fsm_slr,
        COALESCE(t3.is_lpi_slr, 0) AS is_lpi_slr,
        COALESCE(t3.is_vcm_slr, 0) AS is_vcm_slr,
        (
            COALESCE(t2.fsm_promo_amt, 0) + COALESCE(t2.lpi_promo_amt, 0) + COALESCE(t2.vcm_promo_amt, 0)
        ) * t1.exchange_rate AS any_promo_amt,
        COALESCE(t2.fsm_promo_amt, 0) * t1.exchange_rate AS fsm_promo_amt,
        COALESCE(t2.lpi_promo_amt, 0) * t1.exchange_rate AS lpi_promo_amt,
        COALESCE(t2.vcm_promo_amt, 0) * t1.exchange_rate AS vcm_promo_amt,
        (
            COALESCE(t3.fsm_slr_amt, 0) + COALESCE(t3.lpi_slr_amt, 0) + COALESCE(t3.vcm_slr_amt, 0)
        ) * t1.exchange_rate AS any_slr_amt,
        COALESCE(t3.fsm_slr_amt, 0) * t1.exchange_rate AS fsm_slr_amt,
        COALESCE(t3.lpi_slr_amt, 0) * t1.exchange_rate AS lpi_slr_amt,
        COALESCE(t3.vcm_slr_amt, 0) * t1.exchange_rate AS vcm_slr_amt,
        t1.actual_gmv * t1.exchange_rate AS gmv
    FROM (
            SELECT *
            FROM lazada_cdm.dwd_lzd_trd_core_fulfill_di
            WHERE 1 = 1 --
                -- AND     ds = '${bizdate}' --<< Test
                AND ds >= 20240401 --<< Production
                -- AND     ds >= 20241001 --<< Adhoc
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
                        WHEN CAST(
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
                        ) THEN 1
                        ELSE 0
                    END
                ) AS is_vcm_promo,
                SUM(
                    CASE
                        WHEN t2.prom_sub_type IN ('FS Max') THEN promotion_amount
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
                        ) THEN promotion_amount
                        ELSE 0
                    END
                ) AS lpi_promo_amt,
                SUM(
                    CASE
                        WHEN CAST(
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
                        ) THEN promotion_amount
                        ELSE 0
                    END
                ) AS vcm_promo_amt
            FROM (
                    SELECT *
                    FROM lazada_cdm.dwd_lzd_pro_promotion_item_di
                    WHERE 1 = 1 --
                        -- AND     ds = '${bizdate}' --<< Test
                        AND ds >= 20240320 --<< Production
                        -- AND     ds >= 20241001 --<< Adhoc
                        AND venture = 'VN'
                        AND is_fulfilled = 1
                        AND TOLOWER(promotion_role) IN ('platform')
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
                        WHEN ds BETWEEN 20240401 AND 20240930
                        AND ABS(
                            COALESCE(KEYVALUE(exp_comm_amt_detail, 'LPI'), 0)
                        ) > 0 THEN 1
                        ELSE 0
                    END
                ) AS is_lpi_slr --<< LPI 3.0 live duration
,
                MAX(
                    CASE
                        WHEN ds >= 20241001
                        AND ABS(
                            COALESCE(KEYVALUE(exp_comm_amt_detail, 'LPI'), 0)
                        ) > 0 THEN 1
                        ELSE 0
                    END
                ) AS is_vcm_slr --<< VCM 2.0 live duration
,
                SUM(
                    ABS(
                        COALESCE(KEYVALUE(exp_comm_amt_detail, 'FS_MAX'), 0)
                    )
                ) AS fsm_slr_amt,
                SUM(
                    CASE
                        WHEN ds BETWEEN 20240401 AND 20240930 THEN ABS(
                            COALESCE(KEYVALUE(exp_comm_amt_detail, 'LPI'), 0)
                        )
                        ELSE 0
                    END
                ) AS lpi_slr_amt --<< LPI 3.0 live duration
,
                SUM(
                    CASE
                        WHEN ds >= 20241001 THEN ABS(
                            COALESCE(KEYVALUE(exp_comm_amt_detail, 'LPI'), 0)
                        )
                        ELSE 0
                    END
                ) AS vcm_slr_amt --<< VCM 2.0 live duration
            FROM lazada_ent_cdm.dwd_lzd_fin_trd_commission_di
            WHERE 1 = 1 --
                -- AND     ds = '${bizdate}' --<< Test
                AND ds >= 20240401 --<< Production
                -- AND     ds >= 20241001 --<< Adhoc
                AND venture = 'VN'
            GROUP BY sales_order_item_id
        ) AS t3 ON t1.sales_order_item_id = t3.sales_order_item_id --
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
                AND venture = 'VN' --
                -- AND     ds_month = MAX_PT("lazada_ads.dws_lzd_cdp_usr_uid_contribute_value_v3") --<< adhoc
        ) AS t4 ON TO_CHAR(TO_DATE(t1.ds, 'yyyymmdd'), 'yyyymm') = TO_CHAR(
            DATEADD(TO_DATE(t4.ds_month, 'yyyymm'), + 1, 'mm'),
            'yyyymm'
        )
        AND t1.buyer_id = t4.user_id
)
SELECT t1.ds AS ds,
    CONCAT(
        'Week:',
        ' ',
        t6.week_start,
        ' ',
        '-->>',
        ' ',
        t6.week_end
    ) AS week,
    t7.camp_type_fixed AS camp_type,
    t1.promo_program AS promo_program,
    t1.user_contribution_tag AS user_contribution_tag,
    COALESCE(t3.gmv_promo_guided, 0) AS gmv_promo_guided,
    COALESCE(t3.order_promo_guided, 0) AS order_promo_guided,
    COALESCE(t3.buyer_promo_guided, 0) AS buyer_promo_guided,
    t1.gmv_plt_exc_ac_ae AS gmv_plt_exc_ac_ae,
    t1.order_plt_exc_ac_ae AS order_plt_exc_ac_ae,
    t1.buyer_plt_exc_ac_ae AS buyer_plt_exc_ac_ae,
    t1.gmv_plt_exc_ac AS gmv_plt_exc_ac,
    t1.order_plt_exc_ac AS order_plt_exc_ac,
    t1.buyer_plt_exc_ac AS buyer_plt_exc_ac,
    t1.gmv_plt AS gmv_plt,
    t1.order_plt AS order_plt,
    t1.buyer_plt AS buyer_plt,
    COALESCE(t2.gmv_seg_exc_ac_ae, 0) AS gmv_seg_exc_ac_ae,
    COALESCE(t2.order_seg_exc_ac_ae, 0) AS order_seg_exc_ac_ae,
    COALESCE(t2.buyer_seg_exc_ac_ae, 0) AS buyer_seg_exc_ac_ae,
    COALESCE(t2.gmv_seg_exc_ac, 0) AS gmv_seg_exc_ac,
    COALESCE(t2.order_seg_exc_ac, 0) AS order_seg_exc_ac,
    COALESCE(t2.buyer_seg_exc_ac, 0) AS buyer_seg_exc_ac,
    COALESCE(t2.gmv_seg, 0) AS gmv_seg,
    COALESCE(t2.order_seg, 0) AS order_seg,
    COALESCE(t2.buyer_seg, 0) AS buyer_seg,
    COALESCE(t3.promo_amt_gross_fulfill, 0) AS promo_amt_gross_fulfill,
    COALESCE(t3.slr_amt_fulfill, 0) AS slr_amt_fulfill,
    COALESCE(t3.promo_amt_net_fulfill, 0) AS promo_amt_net_fulfill,
    COALESCE(t3.promo_amt_gross_deliver, 0) AS promo_amt_gross_deliver,
    COALESCE(t3.slr_amt_deliver, 0) AS slr_amt_deliver,
    COALESCE(t3.promo_amt_net_deliver, 0) AS promo_amt_net_deliver --
    --<< Full performance monthly
,
    COALESCE(t3.gmv_promo_guided_mm, 0) AS gmv_promo_guided_mm,
    COALESCE(t3.order_promo_guided_mm, 0) AS order_promo_guided_mm,
    COALESCE(t3.buyer_promo_guided_mm, 0) AS buyer_promo_guided_mm,
    COALESCE(t3.promo_amt_gross_fulfill_mm, 0) AS promo_amt_gross_fulfill_mm,
    COALESCE(t3.slr_amt_fulfill_mm, 0) AS slr_amt_fulfill_mm,
    COALESCE(t3.promo_amt_net_fulfill_mm, 0) AS promo_amt_net_fulfill_mm,
    COALESCE(t3.promo_amt_gross_deliver_mm, 0) AS promo_amt_gross_deliver_mm,
    COALESCE(t3.slr_amt_deliver_mm, 0) AS slr_amt_deliver_mm,
    COALESCE(t3.promo_amt_net_deliver_mm, 0) AS promo_amt_net_deliver_mm --
    --<< Target
,
    CAST(COALESCE(t4.gmv_target, 0) AS DOUBLE) AS gmv_target,
    CAST(COALESCE(t4.order_target, 0) AS DOUBLE) AS order_target,
    CAST(COALESCE(t4.budget_gross, 0) AS DOUBLE) AS budget_gross,
    CAST(COALESCE(t4.budget_net, 0) AS DOUBLE) AS budget_net,
    COALESCE(t5.gmv_target_mm, 0) AS gmv_target_mm,
    COALESCE(t5.order_target_mm, 0) AS order_target_mm,
    COALESCE(t5.budget_gross_mm, 0) AS budget_gross_mm,
    COALESCE(t5.budget_net_mm, 0) AS budget_net_mm
FROM (
        SELECT ds,
            promo_program,
            user_contribution_tag,
            gmv_plt_exc_ac_ae,
            order_plt_exc_ac_ae,
            buyer_plt_exc_ac_ae,
            gmv_plt_exc_ac,
            order_plt_exc_ac,
            buyer_plt_exc_ac,
            gmv_plt,
            order_plt,
            buyer_plt
        FROM (
                SELECT ds,
                    SUM(
                        CASE
                            WHEN TOLOWER(bu_lv2) NOT LIKE '%choice%'
                            AND TOLOWER(bu_lv2) NOT LIKE '%ae%' THEN gmv
                            ELSE 0
                        END
                    ) AS gmv_plt_exc_ac_ae,
                    COUNT(
                        DISTINCT CASE
                            WHEN TOLOWER(bu_lv2) NOT LIKE '%choice%'
                            AND TOLOWER(bu_lv2) NOT LIKE '%ae%' THEN order_id
                            ELSE NULL
                        END
                    ) AS order_plt_exc_ac_ae,
                    COUNT(
                        DISTINCT CASE
                            WHEN TOLOWER(bu_lv2) NOT LIKE '%choice%'
                            AND TOLOWER(bu_lv2) NOT LIKE '%ae%' THEN buyer_id
                            ELSE NULL
                        END
                    ) AS buyer_plt_exc_ac_ae,
                    SUM(
                        CASE
                            WHEN TOLOWER(bu_lv2) NOT LIKE '%choice%' THEN gmv
                            ELSE 0
                        END
                    ) AS gmv_plt_exc_ac,
                    COUNT(
                        DISTINCT CASE
                            WHEN TOLOWER(bu_lv2) NOT LIKE '%choice%' THEN order_id
                            ELSE NULL
                        END
                    ) AS order_plt_exc_ac,
                    COUNT(
                        DISTINCT CASE
                            WHEN TOLOWER(bu_lv2) NOT LIKE '%choice%' THEN buyer_id
                            ELSE NULL
                        END
                    ) AS buyer_plt_exc_ac,
                    SUM(gmv) AS gmv_plt,
                    COUNT(DISTINCT order_id) AS order_plt,
                    COUNT(DISTINCT buyer_id) AS buyer_plt
                FROM t_trd_core_master
                GROUP BY ds
            ) LATERAL VIEW EXPLODE(
                SPLIT(
                    '00_all,01_fsm_all_ops,02_lpi_all_ops,03_vcm_all_ops',
                    ','
                )
            ) promo_program AS promo_program LATERAL VIEW EXPLODE(
                SPLIT(
                    '00_all,01_high,02_middle,03_low,04_lto 3-,05_undefined',
                    ','
                )
            ) user_contribution_tag AS user_contribution_tag
    ) AS t1
    LEFT JOIN (
        SELECT ds,
            promo_program,
            user_contribution_tag,
            gmv_seg_exc_ac_ae,
            order_seg_exc_ac_ae,
            buyer_seg_exc_ac_ae,
            gmv_seg_exc_ac,
            order_seg_exc_ac,
            buyer_seg_exc_ac,
            gmv_seg,
            order_seg,
            buyer_seg
        FROM (
                SELECT ds,
                    COALESCE(user_contribution_tag, '00_all') AS user_contribution_tag,
                    SUM(
                        CASE
                            WHEN TOLOWER(bu_lv2) NOT LIKE '%choice%'
                            AND TOLOWER(bu_lv2) NOT LIKE '%ae%' THEN gmv
                            ELSE 0
                        END
                    ) AS gmv_seg_exc_ac_ae,
                    COUNT(
                        DISTINCT CASE
                            WHEN TOLOWER(bu_lv2) NOT LIKE '%choice%'
                            AND TOLOWER(bu_lv2) NOT LIKE '%ae%' THEN order_id
                            ELSE NULL
                        END
                    ) AS order_seg_exc_ac_ae,
                    COUNT(
                        DISTINCT CASE
                            WHEN TOLOWER(bu_lv2) NOT LIKE '%choice%'
                            AND TOLOWER(bu_lv2) NOT LIKE '%ae%' THEN buyer_id
                            ELSE NULL
                        END
                    ) AS buyer_seg_exc_ac_ae,
                    SUM(
                        CASE
                            WHEN TOLOWER(bu_lv2) NOT LIKE '%choice%' THEN gmv
                            ELSE 0
                        END
                    ) AS gmv_seg_exc_ac,
                    COUNT(
                        DISTINCT CASE
                            WHEN TOLOWER(bu_lv2) NOT LIKE '%choice%' THEN order_id
                            ELSE NULL
                        END
                    ) AS order_seg_exc_ac,
                    COUNT(
                        DISTINCT CASE
                            WHEN TOLOWER(bu_lv2) NOT LIKE '%choice%' THEN buyer_id
                            ELSE NULL
                        END
                    ) AS buyer_seg_exc_ac,
                    SUM(gmv) AS gmv_seg,
                    COUNT(DISTINCT order_id) AS order_seg,
                    COUNT(DISTINCT buyer_id) AS buyer_seg
                FROM t_trd_core_master
                GROUP BY ds,
                    CUBE(user_contribution_tag)
            ) LATERAL VIEW EXPLODE(
                SPLIT(
                    '00_all,01_fsm_all_ops,02_lpi_all_ops,03_vcm_all_ops',
                    ','
                )
            ) promo_program AS promo_program
    ) AS t2 ON t1.ds = t2.ds
    AND t1.promo_program = t2.promo_program
    AND t1.user_contribution_tag = t2.user_contribution_tag
    LEFT JOIN (
        SELECT ds,
            '00_all' AS promo_program,
            COALESCE(user_contribution_tag, '00_all') AS user_contribution_tag --
            --<< Topline fulfilled daily >>--
,
            SUM(
                CASE
                    WHEN is_any_promo = 1 THEN gmv
                    ELSE 0
                END
            ) AS gmv_promo_guided,
            COUNT(
                DISTINCT CASE
                    WHEN is_any_promo = 1 THEN order_id
                    ELSE NULL
                END
            ) AS order_promo_guided,
            COUNT(
                DISTINCT CASE
                    WHEN is_any_promo = 1 THEN buyer_id
                    ELSE NULL
                END
            ) AS buyer_promo_guided --
            --<< Botline fulfilled daily >>--
,
            SUM(any_promo_amt) AS promo_amt_gross_fulfill,
            SUM(any_slr_amt) AS slr_amt_fulfill,
            SUM(any_promo_amt) - SUM(any_slr_amt) AS promo_amt_net_fulfill --
            --<< Bottom line delivered daily >>--
,
            SUM(
                CASE
                    WHEN TOLOWER(item_status_esm) IN ('shipped', 'exportable', 'delivered') THEN any_promo_amt
                    ELSE 0
                END
            ) AS promo_amt_gross_deliver,
            SUM(
                CASE
                    WHEN TOLOWER(item_status_esm) IN ('shipped', 'exportable', 'delivered') THEN any_slr_amt
                    ELSE 0
                END
            ) AS slr_amt_deliver,
            SUM(
                CASE
                    WHEN TOLOWER(item_status_esm) IN ('shipped', 'exportable', 'delivered') THEN any_promo_amt
                    ELSE 0
                END
            ) - SUM(
                CASE
                    WHEN TOLOWER(item_status_esm) IN ('shipped', 'exportable', 'delivered') THEN any_slr_amt
                    ELSE 0
                END
            ) AS promo_amt_net_deliver --
            --<< Topline monthly >>--
,
            SUM(
                SUM(
                    CASE
                        WHEN is_any_promo = 1 THEN gmv
                        ELSE 0
                    END
                )
            ) OVER (
                PARTITION BY TO_CHAR(TO_DATE(ds, 'yyyymmdd'), 'yyyymm'),
                COALESCE(user_contribution_tag, '00_all')
                ORDER BY ds ASC
            ) AS gmv_promo_guided_mm,
            SUM(
                COUNT(
                    DISTINCT CASE
                        WHEN is_any_promo = 1 THEN order_id
                        ELSE NULL
                    END
                )
            ) OVER (
                PARTITION BY TO_CHAR(TO_DATE(ds, 'yyyymmdd'), 'yyyymm'),
                COALESCE(user_contribution_tag, '00_all')
                ORDER BY ds ASC
            ) AS order_promo_guided_mm,
            SUM(
                COUNT(
                    DISTINCT CASE
                        WHEN is_any_promo = 1 THEN buyer_id
                        ELSE NULL
                    END
                )
            ) OVER (
                PARTITION BY TO_CHAR(TO_DATE(ds, 'yyyymmdd'), 'yyyymm'),
                COALESCE(user_contribution_tag, '00_all')
                ORDER BY ds ASC
            ) AS buyer_promo_guided_mm --
            --<<  Bottom line fulfilled monthly >>--
,
            SUM(SUM(any_promo_amt)) OVER (
                PARTITION BY TO_CHAR(TO_DATE(ds, 'yyyymmdd'), 'yyyymm'),
                COALESCE(user_contribution_tag, '00_all')
                ORDER BY ds ASC
            ) AS promo_amt_gross_fulfill_mm,
            SUM(SUM(any_slr_amt)) OVER (
                PARTITION BY TO_CHAR(TO_DATE(ds, 'yyyymmdd'), 'yyyymm'),
                COALESCE(user_contribution_tag, '00_all')
                ORDER BY ds ASC
            ) AS slr_amt_fulfill_mm,
            SUM(SUM(any_promo_amt) - SUM(any_slr_amt)) OVER (
                PARTITION BY TO_CHAR(TO_DATE(ds, 'yyyymmdd'), 'yyyymm'),
                COALESCE(user_contribution_tag, '00_all')
                ORDER BY ds ASC
            ) AS promo_amt_net_fulfill_mm --
            --<<  Bottom line delivered monthly >>--
,
            SUM(
                SUM(
                    CASE
                        WHEN TOLOWER(item_status_esm) IN ('shipped', 'exportable', 'delivered') THEN any_promo_amt
                        ELSE 0
                    END
                )
            ) OVER (
                PARTITION BY TO_CHAR(TO_DATE(ds, 'yyyymmdd'), 'yyyymm'),
                COALESCE(user_contribution_tag, '00_all')
                ORDER BY ds ASC
            ) AS promo_amt_gross_deliver_mm,
            SUM(
                SUM(
                    CASE
                        WHEN TOLOWER(item_status_esm) IN ('shipped', 'exportable', 'delivered') THEN any_slr_amt
                        ELSE 0
                    END
                )
            ) OVER (
                PARTITION BY TO_CHAR(TO_DATE(ds, 'yyyymmdd'), 'yyyymm'),
                COALESCE(user_contribution_tag, '00_all')
                ORDER BY ds ASC
            ) AS slr_amt_deliver_mm,
            SUM(
                SUM(
                    CASE
                        WHEN TOLOWER(item_status_esm) IN ('shipped', 'exportable', 'delivered') THEN any_promo_amt
                        ELSE 0
                    END
                ) - SUM(
                    CASE
                        WHEN TOLOWER(item_status_esm) IN ('shipped', 'exportable', 'delivered') THEN any_slr_amt
                        ELSE 0
                    END
                )
            ) OVER (
                PARTITION BY TO_CHAR(TO_DATE(ds, 'yyyymmdd'), 'yyyymm'),
                COALESCE(user_contribution_tag, '00_all')
                ORDER BY ds ASC
            ) AS promo_amt_net_deliver_mm
        FROM t_trd_core_master
        WHERE 1 = 1
            AND (
                is_any_promo = 1
                OR is_any_slr = 1
            )
        GROUP BY ds,
            CUBE(user_contribution_tag)
        UNION ALL
        --<< BREAK POINT
        SELECT ds,
            '01_fsm_all_ops' AS promo_program,
            COALESCE(user_contribution_tag, '00_all') AS user_contribution_tag --
            --<< Topline daily >>--
,
            SUM(
                CASE
                    WHEN is_fsm_promo = 1 THEN gmv
                    ELSE 0
                END
            ) AS gmv_promo_guided,
            COUNT(
                DISTINCT CASE
                    WHEN is_fsm_promo = 1 THEN order_id
                    ELSE NULL
                END
            ) AS order_promo_guided,
            COUNT(
                DISTINCT CASE
                    WHEN is_fsm_promo = 1 THEN buyer_id
                    ELSE NULL
                END
            ) AS buyer_promo_guided --
            --<< Bottom line fulfilled daily >>--
,
            SUM(fsm_promo_amt) AS promo_amt_gross_fulfill,
            SUM(fsm_slr_amt) AS slr_amt_fulfill,
            SUM(fsm_promo_amt) - SUM(fsm_slr_amt) AS promo_amt_net_fulfill --
            --<< Bottom line delivered daily >>--
,
            SUM(
                CASE
                    WHEN TOLOWER(item_status_esm) IN ('shipped', 'exportable', 'delivered') THEN fsm_promo_amt
                    ELSE 0
                END
            ) AS promo_amt_gross_deliver,
            SUM(
                CASE
                    WHEN TOLOWER(item_status_esm) IN ('shipped', 'exportable', 'delivered') THEN fsm_slr_amt
                    ELSE 0
                END
            ) AS slr_amt_deliver,
            SUM(
                CASE
                    WHEN TOLOWER(item_status_esm) IN ('shipped', 'exportable', 'delivered') THEN fsm_promo_amt
                    ELSE 0
                END
            ) - SUM(
                CASE
                    WHEN TOLOWER(item_status_esm) IN ('shipped', 'exportable', 'delivered') THEN fsm_slr_amt
                    ELSE 0
                END
            ) AS promo_amt_net_deliver --
            --<< Topline monthly >>--
,
            SUM(
                SUM(
                    CASE
                        WHEN is_fsm_promo = 1 THEN gmv
                        ELSE 0
                    END
                )
            ) OVER (
                PARTITION BY TO_CHAR(TO_DATE(ds, 'yyyymmdd'), 'yyyymm'),
                COALESCE(user_contribution_tag, '00_all')
                ORDER BY ds ASC
            ) AS gmv_promo_guided_mm,
            SUM(
                COUNT(
                    DISTINCT CASE
                        WHEN is_fsm_promo = 1 THEN order_id
                        ELSE NULL
                    END
                )
            ) OVER (
                PARTITION BY TO_CHAR(TO_DATE(ds, 'yyyymmdd'), 'yyyymm'),
                COALESCE(user_contribution_tag, '00_all')
                ORDER BY ds ASC
            ) AS order_promo_guided_mm,
            SUM(
                COUNT(
                    DISTINCT CASE
                        WHEN is_fsm_promo = 1 THEN buyer_id
                        ELSE NULL
                    END
                )
            ) OVER (
                PARTITION BY TO_CHAR(TO_DATE(ds, 'yyyymmdd'), 'yyyymm'),
                COALESCE(user_contribution_tag, '00_all')
                ORDER BY ds ASC
            ) AS buyer_promo_guided_mm --
            --<<  Bottom line fulfilled monthly >>--
,
            SUM(SUM(fsm_promo_amt)) OVER (
                PARTITION BY TO_CHAR(TO_DATE(ds, 'yyyymmdd'), 'yyyymm'),
                COALESCE(user_contribution_tag, '00_all')
                ORDER BY ds ASC
            ) AS promo_amt_gross_fulfill_mm,
            SUM(SUM(fsm_slr_amt)) OVER (
                PARTITION BY TO_CHAR(TO_DATE(ds, 'yyyymmdd'), 'yyyymm'),
                COALESCE(user_contribution_tag, '00_all')
                ORDER BY ds ASC
            ) AS slr_amt_fulfill_mm,
            SUM(SUM(fsm_promo_amt) - SUM(fsm_slr_amt)) OVER (
                PARTITION BY TO_CHAR(TO_DATE(ds, 'yyyymmdd'), 'yyyymm'),
                COALESCE(user_contribution_tag, '00_all')
                ORDER BY ds ASC
            ) AS promo_amt_net_fulfill_mm --
            --<<  Bottom line delivered monthly >>--
,
            SUM(
                SUM(
                    CASE
                        WHEN TOLOWER(item_status_esm) IN ('shipped', 'exportable', 'delivered') THEN fsm_promo_amt
                        ELSE 0
                    END
                )
            ) OVER (
                PARTITION BY TO_CHAR(TO_DATE(ds, 'yyyymmdd'), 'yyyymm'),
                COALESCE(user_contribution_tag, '00_all')
                ORDER BY ds ASC
            ) AS promo_amt_gross_deliver_mm,
            SUM(
                SUM(
                    CASE
                        WHEN TOLOWER(item_status_esm) IN ('shipped', 'exportable', 'delivered') THEN fsm_slr_amt
                        ELSE 0
                    END
                )
            ) OVER (
                PARTITION BY TO_CHAR(TO_DATE(ds, 'yyyymmdd'), 'yyyymm'),
                COALESCE(user_contribution_tag, '00_all')
                ORDER BY ds ASC
            ) AS slr_amt_deliver_mm,
            SUM(
                SUM(
                    CASE
                        WHEN TOLOWER(item_status_esm) IN ('shipped', 'exportable', 'delivered') THEN fsm_promo_amt
                        ELSE 0
                    END
                ) - SUM(
                    CASE
                        WHEN TOLOWER(item_status_esm) IN ('shipped', 'exportable', 'delivered') THEN fsm_slr_amt
                        ELSE 0
                    END
                )
            ) OVER (
                PARTITION BY TO_CHAR(TO_DATE(ds, 'yyyymmdd'), 'yyyymm'),
                COALESCE(user_contribution_tag, '00_all')
                ORDER BY ds ASC
            ) AS promo_amt_net_deliver_mm
        FROM t_trd_core_master
        WHERE 1 = 1
            AND (
                is_fsm_promo = 1
                OR is_fsm_slr = 1
            )
        GROUP BY ds,
            CUBE(user_contribution_tag)
        UNION ALL
        --<< BREAK POINT
        SELECT ds,
            '02_lpi_all_ops' AS promo_program,
            COALESCE(user_contribution_tag, '00_all') AS user_contribution_tag --
            --<< Topline daily >>--
,
            SUM(
                CASE
                    WHEN is_lpi_promo = 1 THEN gmv
                    ELSE 0
                END
            ) AS gmv_promo_guided,
            COUNT(
                DISTINCT CASE
                    WHEN is_lpi_promo = 1 THEN order_id
                    ELSE NULL
                END
            ) AS order_promo_guided,
            COUNT(
                DISTINCT CASE
                    WHEN is_lpi_promo = 1 THEN buyer_id
                    ELSE NULL
                END
            ) AS buyer_promo_guided --
            --<< Bottom line fulfilled daily >>--
,
            SUM(lpi_promo_amt) AS promo_amt_gross_fulfill,
            SUM(lpi_slr_amt) AS slr_amt_fulfill,
            SUM(lpi_promo_amt) - SUM(lpi_slr_amt) AS promo_amt_net_fulfill --
            --<< Bottom line delivered daily >>--
,
            SUM(
                CASE
                    WHEN TOLOWER(item_status_esm) IN ('shipped', 'exportable', 'delivered') THEN lpi_promo_amt
                    ELSE 0
                END
            ) AS promo_amt_gross_deliver,
            SUM(
                CASE
                    WHEN TOLOWER(item_status_esm) IN ('shipped', 'exportable', 'delivered') THEN lpi_slr_amt
                    ELSE 0
                END
            ) AS slr_amt_deliver,
            SUM(
                CASE
                    WHEN TOLOWER(item_status_esm) IN ('shipped', 'exportable', 'delivered') THEN lpi_promo_amt
                    ELSE 0
                END
            ) - SUM(
                CASE
                    WHEN TOLOWER(item_status_esm) IN ('shipped', 'exportable', 'delivered') THEN lpi_slr_amt
                    ELSE 0
                END
            ) AS promo_amt_net_deliver --
            --<< Topline monthly >>--
,
            SUM(
                SUM(
                    CASE
                        WHEN is_lpi_promo = 1 THEN gmv
                        ELSE 0
                    END
                )
            ) OVER (
                PARTITION BY TO_CHAR(TO_DATE(ds, 'yyyymmdd'), 'yyyymm'),
                COALESCE(user_contribution_tag, '00_all')
                ORDER BY ds ASC
            ) AS gmv_promo_guided_mm,
            SUM(
                COUNT(
                    DISTINCT CASE
                        WHEN is_lpi_promo = 1 THEN order_id
                        ELSE NULL
                    END
                )
            ) OVER (
                PARTITION BY TO_CHAR(TO_DATE(ds, 'yyyymmdd'), 'yyyymm'),
                COALESCE(user_contribution_tag, '00_all')
                ORDER BY ds ASC
            ) AS order_promo_guided_mm,
            SUM(
                COUNT(
                    DISTINCT CASE
                        WHEN is_lpi_promo = 1 THEN buyer_id
                        ELSE NULL
                    END
                )
            ) OVER (
                PARTITION BY TO_CHAR(TO_DATE(ds, 'yyyymmdd'), 'yyyymm'),
                COALESCE(user_contribution_tag, '00_all')
                ORDER BY ds ASC
            ) AS buyer_promo_guided_mm --
            --<<  Bottom line fulfilled monthly >>--
,
            SUM(SUM(lpi_promo_amt)) OVER (
                PARTITION BY TO_CHAR(TO_DATE(ds, 'yyyymmdd'), 'yyyymm'),
                COALESCE(user_contribution_tag, '00_all')
                ORDER BY ds ASC
            ) AS promo_amt_gross_fulfill_mm,
            SUM(SUM(lpi_slr_amt)) OVER (
                PARTITION BY TO_CHAR(TO_DATE(ds, 'yyyymmdd'), 'yyyymm'),
                COALESCE(user_contribution_tag, '00_all')
                ORDER BY ds ASC
            ) AS slr_amt_fulfill_mm,
            SUM(SUM(lpi_promo_amt) - SUM(lpi_slr_amt)) OVER (
                PARTITION BY TO_CHAR(TO_DATE(ds, 'yyyymmdd'), 'yyyymm'),
                COALESCE(user_contribution_tag, '00_all')
                ORDER BY ds ASC
            ) AS promo_amt_net_fulfill_mm --
            --<<  Bottom line delivered monthly >>--
,
            SUM(
                SUM(
                    CASE
                        WHEN TOLOWER(item_status_esm) IN ('shipped', 'exportable', 'delivered') THEN lpi_promo_amt
                        ELSE 0
                    END
                )
            ) OVER (
                PARTITION BY TO_CHAR(TO_DATE(ds, 'yyyymmdd'), 'yyyymm'),
                COALESCE(user_contribution_tag, '00_all')
                ORDER BY ds ASC
            ) AS promo_amt_gross_deliver_mm,
            SUM(
                SUM(
                    CASE
                        WHEN TOLOWER(item_status_esm) IN ('shipped', 'exportable', 'delivered') THEN lpi_slr_amt
                        ELSE 0
                    END
                )
            ) OVER (
                PARTITION BY TO_CHAR(TO_DATE(ds, 'yyyymmdd'), 'yyyymm'),
                COALESCE(user_contribution_tag, '00_all')
                ORDER BY ds ASC
            ) AS slr_amt_deliver_mm,
            SUM(
                SUM(
                    CASE
                        WHEN TOLOWER(item_status_esm) IN ('shipped', 'exportable', 'delivered') THEN lpi_promo_amt
                        ELSE 0
                    END
                ) - SUM(
                    CASE
                        WHEN TOLOWER(item_status_esm) IN ('shipped', 'exportable', 'delivered') THEN lpi_slr_amt
                        ELSE 0
                    END
                )
            ) OVER (
                PARTITION BY TO_CHAR(TO_DATE(ds, 'yyyymmdd'), 'yyyymm'),
                COALESCE(user_contribution_tag, '00_all')
                ORDER BY ds ASC
            ) AS promo_amt_net_deliver_mm
        FROM t_trd_core_master
        WHERE 1 = 1
            AND (
                is_lpi_promo = 1
                OR is_lpi_slr = 1
            )
        GROUP BY ds,
            CUBE(user_contribution_tag)
        UNION ALL
        --<< BREAK POINT
        SELECT ds,
            '03_vcm_all_ops' AS promo_program,
            COALESCE(user_contribution_tag, '00_all') AS user_contribution_tag --
            --<< Topline daily >>--
,
            SUM(
                CASE
                    WHEN is_vcm_promo = 1 THEN gmv
                    ELSE 0
                END
            ) AS gmv_promo_guided,
            COUNT(
                DISTINCT CASE
                    WHEN is_vcm_promo = 1 THEN order_id
                    ELSE NULL
                END
            ) AS order_promo_guided,
            COUNT(
                DISTINCT CASE
                    WHEN is_vcm_promo = 1 THEN buyer_id
                    ELSE NULL
                END
            ) AS buyer_promo_guided --
            --<< Bottom line fulfilled daily >>--
,
            SUM(vcm_promo_amt) AS promo_amt_gross_fulfill,
            SUM(vcm_slr_amt) AS slr_amt_fulfill,
            SUM(vcm_promo_amt) - SUM(vcm_slr_amt) AS promo_amt_net_fulfill --
            --<< Bottom line delivered daily >>--
,
            SUM(
                CASE
                    WHEN TOLOWER(item_status_esm) IN ('shipped', 'exportable', 'delivered') THEN vcm_promo_amt
                    ELSE 0
                END
            ) AS promo_amt_gross_deliver,
            SUM(
                CASE
                    WHEN TOLOWER(item_status_esm) IN ('shipped', 'exportable', 'delivered') THEN vcm_slr_amt
                    ELSE 0
                END
            ) AS slr_amt_deliver,
            SUM(
                CASE
                    WHEN TOLOWER(item_status_esm) IN ('shipped', 'exportable', 'delivered') THEN vcm_promo_amt
                    ELSE 0
                END
            ) - SUM(
                CASE
                    WHEN TOLOWER(item_status_esm) IN ('shipped', 'exportable', 'delivered') THEN vcm_slr_amt
                    ELSE 0
                END
            ) AS promo_amt_net_deliver --
            --<< Topline monthly >>--
,
            SUM(
                SUM(
                    CASE
                        WHEN is_vcm_promo = 1 THEN gmv
                        ELSE 0
                    END
                )
            ) OVER (
                PARTITION BY TO_CHAR(TO_DATE(ds, 'yyyymmdd'), 'yyyymm'),
                COALESCE(user_contribution_tag, '00_all')
                ORDER BY ds ASC
            ) AS gmv_promo_guided_mm,
            SUM(
                COUNT(
                    DISTINCT CASE
                        WHEN is_vcm_promo = 1 THEN order_id
                        ELSE NULL
                    END
                )
            ) OVER (
                PARTITION BY TO_CHAR(TO_DATE(ds, 'yyyymmdd'), 'yyyymm'),
                COALESCE(user_contribution_tag, '00_all')
                ORDER BY ds ASC
            ) AS order_promo_guided_mm,
            SUM(
                COUNT(
                    DISTINCT CASE
                        WHEN is_vcm_promo = 1 THEN buyer_id
                        ELSE NULL
                    END
                )
            ) OVER (
                PARTITION BY TO_CHAR(TO_DATE(ds, 'yyyymmdd'), 'yyyymm'),
                COALESCE(user_contribution_tag, '00_all')
                ORDER BY ds ASC
            ) AS buyer_promo_guided_mm --
            --<<  Bottom line fulfilled monthly >>--
,
            SUM(SUM(vcm_promo_amt)) OVER (
                PARTITION BY TO_CHAR(TO_DATE(ds, 'yyyymmdd'), 'yyyymm'),
                COALESCE(user_contribution_tag, '00_all')
                ORDER BY ds ASC
            ) AS promo_amt_gross_fulfill_mm,
            SUM(SUM(vcm_slr_amt)) OVER (
                PARTITION BY TO_CHAR(TO_DATE(ds, 'yyyymmdd'), 'yyyymm'),
                COALESCE(user_contribution_tag, '00_all')
                ORDER BY ds ASC
            ) AS slr_amt_fulfill_mm,
            SUM(SUM(vcm_promo_amt) - SUM(vcm_slr_amt)) OVER (
                PARTITION BY TO_CHAR(TO_DATE(ds, 'yyyymmdd'), 'yyyymm'),
                COALESCE(user_contribution_tag, '00_all')
                ORDER BY ds ASC
            ) AS promo_amt_net_fulfill_mm --
            --<<  Bottom line delivered monthly >>--
,
            SUM(
                SUM(
                    CASE
                        WHEN TOLOWER(item_status_esm) IN ('shipped', 'exportable', 'delivered') THEN vcm_promo_amt
                        ELSE 0
                    END
                )
            ) OVER (
                PARTITION BY TO_CHAR(TO_DATE(ds, 'yyyymmdd'), 'yyyymm'),
                COALESCE(user_contribution_tag, '00_all')
                ORDER BY ds ASC
            ) AS promo_amt_gross_deliver_mm,
            SUM(
                SUM(
                    CASE
                        WHEN TOLOWER(item_status_esm) IN ('shipped', 'exportable', 'delivered') THEN vcm_slr_amt
                        ELSE 0
                    END
                )
            ) OVER (
                PARTITION BY TO_CHAR(TO_DATE(ds, 'yyyymmdd'), 'yyyymm'),
                COALESCE(user_contribution_tag, '00_all')
                ORDER BY ds ASC
            ) AS slr_amt_deliver_mm,
            SUM(
                SUM(
                    CASE
                        WHEN TOLOWER(item_status_esm) IN ('shipped', 'exportable', 'delivered') THEN vcm_promo_amt
                        ELSE 0
                    END
                ) - SUM(
                    CASE
                        WHEN TOLOWER(item_status_esm) IN ('shipped', 'exportable', 'delivered') THEN vcm_slr_amt
                        ELSE 0
                    END
                )
            ) OVER (
                PARTITION BY TO_CHAR(TO_DATE(ds, 'yyyymmdd'), 'yyyymm'),
                COALESCE(user_contribution_tag, '00_all')
                ORDER BY ds ASC
            ) AS promo_amt_net_deliver_mm
        FROM t_trd_core_master
        WHERE 1 = 1
            AND (
                is_vcm_promo = 1
                OR is_vcm_slr = 1
            )
        GROUP BY ds,
            CUBE(user_contribution_tag)
    ) AS t3 ON t1.ds = t3.ds
    AND t1.promo_program = t3.promo_program
    AND t1.user_contribution_tag = t3.user_contribution_tag
    LEFT JOIN (
        SELECT *,
            '00_all' AS user_contribution_tag
        FROM lazada_analyst.loutruong_upo_ui_target
    ) AS t4 ON t1.ds = t4.ds
    AND t1.promo_program = t4.promo_bucket
    AND t1.user_contribution_tag = t4.user_contribution_tag
    LEFT JOIN (
        SELECT TO_CHAR(TO_DATE(ds, 'yyyymmdd'), 'yyyymm') AS mm,
            promo_bucket,
            '00_all' AS user_contribution_tag,
            SUM(gmv_target) AS gmv_target_mm,
            SUM(order_target) AS order_target_mm,
            SUM(budget_gross) AS budget_gross_mm,
            SUM(budget_net) AS budget_net_mm
        FROM lazada_analyst.loutruong_upo_ui_target
        GROUP BY TO_CHAR(TO_DATE(ds, 'yyyymmdd'), 'yyyymm'),
            promo_bucket
    ) AS t5 ON TO_CHAR(TO_DATE(t1.ds, 'yyyymmdd'), 'yyyymm') = t5.mm
    AND t1.promo_program = t5.promo_bucket
    AND t1.user_contribution_tag = t5.user_contribution_tag
    LEFT JOIN (
        SELECT ds_day AS ds,
            MAX(ds_day) OVER (PARTITION BY text_week) AS week_end,
            MIN(ds_day) OVER (PARTITION BY text_week) AS week_start
        FROM lazada_cdm.dim_lzd_date
        WHERE 1 = 1
            AND ds_day BETWEEN 20240401 AND TO_CHAR(GETDATE(), 'yyyymmdd')
        GROUP BY ds_day,
            text_week
    ) AS t6 ON t1.ds = t6.ds
    LEFT JOIN (
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
            AND ds BETWEEN 20240401 AND TO_CHAR(GETDATE(), 'yyyymmdd')
    ) AS t7 ON t1.ds = t7.ds
ORDER BY t1.ds ASC,
    t1.promo_program ASC,
    t1.user_contribution_tag ASC;
--------------------------------------------------
SELECT t1.ds AS ds,
    TO_CHAR(
        TO_DATE(
            t1.fulfillment_create_date,
            'yyyy-mm-dd hh:mi:ss'
        ),
        'hh'
    ) AS hh,
    COUNT(DISTINCT t1.order_id) AS ord_id,
    SUM(t1.actual_gmv * t1.exchange_rate) AS gmv,
    COUNT(
        DISTINCT CASE
            WHEN COALESCE(t2.is_fsm_promo, 0) = 1 THEN t1.order_id
            ELSE NULL
        END
    ) AS ord_id_fsm_gui,
    SUM(
        CASE
            WHEN COALESCE(t2.is_fsm_promo, 0) = 1 THEN t1.actual_gmv * t1.exchange_rate
            ELSE 0
        END
    ) AS gmv_fsm_gui,
    COUNT(
        DISTINCT CASE
            WHEN COALESCE(t2.is_vcm_promo, 0) = 1 THEN t1.order_id
            ELSE NULL
        END
    ) AS ord_id_vcm_gui,
    SUM(
        CASE
            WHEN COALESCE(t2.is_vcm_promo, 0) = 1 THEN t1.actual_gmv * t1.exchange_rate
            ELSE 0
        END
    ) AS gmv_vcm_gui,
    SUM(COALESCE(t2.fsm_promo_amt, 0) * t1.exchange_rate) AS fsm_promo_amt,
    SUM(COALESCE(t2.vcm_promo_amt, 0) * t1.exchange_rate) AS vcm_promo_amt,
    SUM(COALESCE(t3.fsm_slr_amt, 0) * t1.exchange_rate) AS fsm_slr_amt,
    SUM(COALESCE(t3.vcm_slr_amt, 0) * t1.exchange_rate) AS vcm_slr_amt
FROM (
        SELECT *
        FROM lazada_cdm.dwd_lzd_trd_core_fulfill_di
        WHERE 1 = 1
            AND ds = 20241111
            AND venture = 'VN'
            AND is_revenue = 1
            AND COALESCE(business_application, 'LZD') IN ('LZD,ZAL', 'LZD')
    ) AS t1
    LEFT JOIN (
        SELECT t1.sales_order_item_id AS sales_order_item_id,
            MAX(
                CASE
                    WHEN t2.promo_mechaincs IN ('01_fsm_all_ops') THEN 1
                    ELSE 0
                END
            ) AS is_fsm_promo,
            MAX(
                CASE
                    WHEN t2.promo_mechaincs IN ('03_vcm_ui_ops') THEN 1
                    ELSE 0
                END
            ) AS is_vcm_promo,
            SUM(
                CASE
                    WHEN t2.promo_mechaincs IN ('01_fsm_all_ops') THEN promotion_amount
                    ELSE 0
                END
            ) AS fsm_promo_amt,
            SUM(
                CASE
                    WHEN t2.promo_mechaincs IN ('03_vcm_ui_ops') THEN promotion_amount
                    ELSE 0
                END
            ) AS vcm_promo_amt
        FROM (
                SELECT *
                FROM lazada_cdm.dwd_lzd_pro_promotion_item_di
                WHERE 1 = 1
                    AND ds >= 20241101
                    AND venture = 'VN'
                    AND is_fulfilled = 1
            ) AS t1
            INNER JOIN (
                SELECT promotion_id,
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
                    END AS promo_mechaincs,
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
            ) AS t2 ON t1.promotion_id = t2.promotion_id
        GROUP BY t1.sales_order_item_id
    ) AS t2 ON t1.sales_order_item_id = t2.sales_order_item_id
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
                    WHEN ds BETWEEN 20240401 AND 20240930
                    AND ABS(
                        COALESCE(KEYVALUE(exp_comm_amt_detail, 'LPI'), 0)
                    ) > 0 THEN 1
                    ELSE 0
                END
            ) AS is_lpi_slr --<< LPI 3.0 live duration
,
            MAX(
                CASE
                    WHEN ds >= 20241001
                    AND ABS(
                        COALESCE(KEYVALUE(exp_comm_amt_detail, 'LPI'), 0)
                    ) > 0 THEN 1
                    ELSE 0
                END
            ) AS is_vcm_slr --<< VCM 2.0 live duration
,
            SUM(
                ABS(
                    COALESCE(KEYVALUE(exp_comm_amt_detail, 'FS_MAX'), 0)
                )
            ) AS fsm_slr_amt,
            SUM(
                CASE
                    WHEN ds BETWEEN 20240401 AND 20240930 THEN ABS(
                        COALESCE(KEYVALUE(exp_comm_amt_detail, 'LPI'), 0)
                    )
                    ELSE 0
                END
            ) AS lpi_slr_amt --<< LPI 3.0 live duration
,
            SUM(
                CASE
                    WHEN ds >= 20241001 THEN ABS(
                        COALESCE(KEYVALUE(exp_comm_amt_detail, 'LPI'), 0)
                    )
                    ELSE 0
                END
            ) AS vcm_slr_amt --<< VCM 2.0 live duration
        FROM lazada_ent_cdm.dwd_lzd_fin_trd_commission_di
        WHERE 1 = 1
            AND ds = 20241111
            AND venture = 'VN'
        GROUP BY sales_order_item_id
    ) AS t3 ON t1.sales_order_item_id = t3.sales_order_item_id
GROUP BY t1.ds,
    TO_CHAR(
        TO_DATE(
            t1.fulfillment_create_date,
            'yyyy-mm-dd hh:mi:ss'
        ),
        'hh'
    )
ORDER BY t1.ds,
    TO_CHAR(
        TO_DATE(
            t1.fulfillment_create_date,
            'yyyy-mm-dd hh:mi:ss'
        ),
        'hh'
    );
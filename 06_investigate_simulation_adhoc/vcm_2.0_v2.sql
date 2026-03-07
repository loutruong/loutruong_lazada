-- MaxCompute SQL 
-- ********************************************************************--
-- author:Truong, Van Thanh
-- create time:2024-08-11 16:29:48
-- ********************************************************************--
-- ********************************************************************--
-- VCM 2.0: slr view
-- ********************************************************************--
SELECT TO_CHAR(TO_DATE(t1.ds, 'yyyymmdd'), 'yyyymm') AS mm,
    t1.ds AS ds,
    t6.camp_type AS camp_type,
    t5.seller_segment_v3_mapped AS seller_segment,
    t5.business_type AS bu_lv1,
    t5.business_type_level2 AS bu_lv2,
    COUNT(DISTINCT t1.seller_id) AS selling_slr,
    COUNT(DISTINCT t1.order_id) AS order_id_cnt,
    SUM(t1.actual_gmv * t1.exchange_rate) AS gmv,
    COUNT(
        DISTINCT CASE
            WHEN t2.sales_order_item_id IS NOT NULL THEN t1.order_id
            ELSE NULL
        END
    ) AS order_id_cnt_lpi_guided,
    SUM(
        CASE
            WHEN t2.sales_order_item_id IS NOT NULL THEN t1.actual_gmv * t1.exchange_rate
            ELSE 0
        END
    ) AS gmv_lpi_guided,
    COUNT(
        DISTINCT CASE
            WHEN t3.sales_order_item_id IS NOT NULL
            AND (
                COALESCE(t3.clvc_slr_amt, 0) > 0
                OR COALESCE(t3.fs_slr_amt, 0) > 0
                OR COALESCE(t3.coin_slr_amt, 0) > 0
            ) THEN t1.order_id
            ELSE NULL
        END
    ) AS order_id_cnt_slr_promo_guided,
    SUM(
        CASE
            WHEN t3.sales_order_item_id IS NOT NULL
            AND (
                COALESCE(t3.clvc_slr_amt, 0) > 0
                OR COALESCE(t3.fs_slr_amt, 0) > 0
                OR COALESCE(t3.coin_slr_amt, 0) > 0
            ) THEN t1.actual_gmv * t1.exchange_rate
            ELSE 0
        END
    ) AS gmv_slr_promo_guided,
    SUM(COALESCE(t4.fsm_slr_amt, 0) * t1.exchange_rate) + SUM(COALESCE(t4.lpi_slr_amt, 0) * t1.exchange_rate) + SUM(COALESCE(t3.clvc_slr_amt, 0) * t1.exchange_rate) + SUM(COALESCE(t3.fs_slr_amt, 0) * t1.exchange_rate) + SUM(COALESCE(t3.coin_slr_amt, 0) * t1.exchange_rate) AS promo_slr_amt_total,
    SUM(COALESCE(t4.fsm_slr_amt, 0) * t1.exchange_rate) AS fsm_slr_amt,
    SUM(COALESCE(t4.lpi_slr_amt, 0) * t1.exchange_rate) AS lpi_slr_amt,
    SUM(COALESCE(t3.clvc_slr_amt, 0) * t1.exchange_rate) AS clvc_slr_amt,
    SUM(COALESCE(t3.fs_slr_amt, 0) * t1.exchange_rate) AS fs_slr_amt,
    SUM(COALESCE(t3.coin_slr_amt, 0) * t1.exchange_rate) AS coin_slr_amt
FROM (
        SELECT *
        FROM lazada_cdm.dwd_lzd_trd_core_fulfill_di
        WHERE 1 = 1 --
            -- AND     ds = '${bizdate}' --<< Test
            AND ds BETWEEN 20240401 AND 20240731 --<< Production
            AND venture = 'VN'
            AND is_revenue = 1
            AND COALESCE(business_application, 'LZD') IN ('LZD,ZAL', 'LZD')
    ) AS t1
    LEFT JOIN (
        SELECT sales_order_item_id
        FROM (
                SELECT t1.sales_order_item_id AS sales_order_item_id,
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
                    ) AS is_lpi_promo
                FROM (
                        SELECT *
                        FROM lazada_cdm.dwd_lzd_pro_promotion_item_di
                        WHERE 1 = 1 --
                            -- AND     ds = '${bizdate}' --<< Test
                            AND ds >= 20240101 --<< Production
                            AND venture = 'VN'
                            AND is_fulfilled = 1
                            AND promotion_type IN (
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
                        FROM lazada_cdm.dim_lzd_pro_collectibles
                        WHERE 1 = 1
                            AND ds = MAX_PT('lazada_cdm.dim_lzd_pro_collectibles')
                            AND venture = 'VN'
                    ) AS t2 ON t1.promotion_id = t2.promotion_id
                GROUP BY t1.sales_order_item_id
            )
        WHERE 1 = 1
            AND is_lpi_promo = 1
    ) AS t2 ON t1.sales_order_item_id = t2.sales_order_item_id
    LEFT JOIN (
        SELECT sales_order_item_id,
            SUM(
                CASE
                    WHEN promotion_type IN ('collectibleCoupon') THEN promotion_amount
                    ELSE 0
                END
            ) AS clvc_slr_amt,
            SUM(
                CASE
                    WHEN promotion_type IN ('shippingFee') THEN promotion_amount
                    ELSE 0
                END
            ) AS fs_slr_amt,
            SUM(
                CASE
                    WHEN promotion_type IN ('flexiCoin') THEN promotion_amount
                    ELSE 0
                END
            ) AS coin_slr_amt
        FROM lazada_cdm.dwd_lzd_pro_promotion_item_di
        WHERE 1 = 1 --
            -- AND     ds = '${bizdate}' --<< Test
            AND ds >= 20240101 --<< Production
            AND venture = 'VN'
            AND is_fulfilled = 1
            AND TOLOWER(promotion_role) IN ('seller')
        GROUP BY sales_order_item_id
    ) AS t3 ON t1.sales_order_item_id = t3.sales_order_item_id
    LEFT JOIN (
        SELECT sales_order_item_id,
            ABS(
                SUM(
                    COALESCE(KEYVALUE(exp_comm_amt_detail, 'FS_MAX'), 0)
                )
            ) AS fsm_slr_amt,
            ABS(
                SUM(
                    COALESCE(KEYVALUE(exp_comm_amt_detail, 'LPI'), 0)
                )
            ) AS lpi_slr_amt
        FROM lazada_ent_cdm.dwd_lzd_fin_trd_commission_di
        WHERE 1 = 1 --
            -- AND     ds = '${bizdate}' --<< Test
            AND ds BETWEEN 20240401 AND 20240731 --<< Production
            AND venture = 'VN'
        GROUP BY sales_order_item_id
    ) AS t4 ON t1.sales_order_item_id = t4.sales_order_item_id
    LEFT JOIN (
        SELECT seller_id,
            business_type,
            business_type_level2,
            seller_segment_v3_mapped
        FROM lazada_analyst_dev.datx_slr_performance_overview_seller_profile
        WHERE 1 = 1
            AND ds = MAX_PT("lazada_cdm.dwd_lzd_trd_core_fulfill_di")
    ) AS t5 ON t1.seller_id = t5.seller_id
    LEFT JOIN (
        SELECT ds,
            day_type,
            CASE
                WHEN TOLOWER(camp_type) IN ('mega') THEN 'Mega'
                WHEN TOLOWER(camp_type) IN ('pd') THEN 'A+'
                WHEN TOLOWER(camp_type) IN ('holiday') THEN 'BAU'
                ELSE camp_type
            END AS camp_type
        FROM lazada_analyst.cp_calendar
        WHERE 1 = 1
            AND ds >= 20240401
        GROUP BY ds,
            day_type,
            CASE
                WHEN TOLOWER(camp_type) IN ('mega') THEN 'Mega'
                WHEN TOLOWER(camp_type) IN ('pd') THEN 'A+'
                WHEN TOLOWER(camp_type) IN ('holiday') THEN 'BAU'
                ELSE camp_type
            END
    ) AS t6 ON t1.ds = t6.ds
GROUP BY TO_CHAR(TO_DATE(t1.ds, 'yyyymmdd'), 'yyyymm'),
    t1.ds,
    t6.camp_type,
    t5.seller_segment_v3_mapped,
    t5.business_type,
    t5.business_type_level2
ORDER BY t1.ds ASC,
    t5.seller_segment_v3_mapped ASC,
    t5.business_type ASC,
    t5.business_type_level2 ASC;
-- ********************************************************************--
-- VCM 2.0: byr view
-- ********************************************************************--
SELECT TO_CHAR(TO_DATE(t1.ds, 'yyyymmdd'), 'yyyymm') AS mm,
    t1.ds AS ds,
    t6.camp_type AS camp_type,
    t5.seller_segment_v3_mapped AS seller_segment,
    t5.business_type AS bu_lv1,
    t5.business_type_level2 AS bu_lv2,
    COUNT(DISTINCT t1.seller_id) AS selling_seller,
    COUNT(DISTINCT t1.order_id) AS order_id_cnt,
    COUNT(
        DISTINCT CASE
            WHEN (
                t2.sales_order_item_id IS NOT NULL
                OR (
                    t3.sales_order_item_id IS NOT NULL
                    AND (
                        COALESCE(t3.clvc_slr_amt, 0) > 0
                        OR COALESCE(t3.fs_slr_amt, 0) > 0
                        OR COALESCE(t3.coin_slr_amt, 0) > 0
                    )
                )
            ) THEN t1.order_id
            ELSE NULL
        END
    ) AS order_id_cnt_any_promo,
    COUNT(
        DISTINCT CASE
            WHEN t2.sales_order_item_id IS NOT NULL
            AND COALESCE(t2.is_lpi_promo, 0) = 1 THEN t1.order_id
            ELSE NULL
        END
    ) AS order_id_cnt_lpi_guided,
    COUNT(
        DISTINCT CASE
            WHEN t3.sales_order_item_id IS NOT NULL
            AND (
                COALESCE(t3.clvc_slr_amt, 0) > 0
                OR COALESCE(t3.fs_slr_amt, 0) > 0
                OR COALESCE(t3.coin_slr_amt, 0) > 0
            ) THEN t1.order_id
            ELSE NULL
        END
    ) AS order_id_cnt_slr_promo_guided,
    COUNT(
        DISTINCT CASE
            WHEN t2.sales_order_item_id IS NOT NULL
            AND (
                COALESCE(t2.is_flash_promo, 0) = 1
                OR COALESCE(t2.is_bau_promo, 0) = 1
            ) THEN t1.order_id
            ELSE NULL
        END
    ) AS order_id_cnt_bau_fvc_guided,
    COUNT(DISTINCT t1.buyer_id) AS buyer,
    COUNT(
        DISTINCT CASE
            WHEN (
                t2.sales_order_item_id IS NOT NULL
                OR (
                    t3.sales_order_item_id IS NOT NULL
                    AND (
                        COALESCE(t3.clvc_slr_amt, 0) > 0
                        OR COALESCE(t3.fs_slr_amt, 0) > 0
                        OR COALESCE(t3.coin_slr_amt, 0) > 0
                    )
                )
            ) THEN t1.buyer_id
            ELSE NULL
        END
    ) AS buyer_any_promo,
    COUNT(
        DISTINCT CASE
            WHEN t2.sales_order_item_id IS NOT NULL
            AND COALESCE(t2.is_lpi_promo, 0) = 1 THEN t1.buyer_id
            ELSE NULL
        END
    ) AS buyer_lpi_guided,
    COUNT(
        DISTINCT CASE
            WHEN t3.sales_order_item_id IS NOT NULL
            AND (
                COALESCE(t3.clvc_slr_amt, 0) > 0
                OR COALESCE(t3.fs_slr_amt, 0) > 0
                OR COALESCE(t3.coin_slr_amt, 0) > 0
            ) THEN t1.buyer_id
            ELSE NULL
        END
    ) AS buyer_slr_promo_guided,
    COUNT(
        DISTINCT CASE
            WHEN t2.sales_order_item_id IS NOT NULL
            AND (
                COALESCE(t2.is_flash_promo, 0) = 1
                OR COALESCE(t2.is_bau_promo, 0) = 1
            ) THEN t1.buyer_id
            ELSE NULL
        END
    ) AS buyer_bau_fvc_guided
FROM (
        SELECT *
        FROM lazada_cdm.dwd_lzd_trd_core_fulfill_di
        WHERE 1 = 1 --
            -- AND     ds = '${bizdate}' --<< Test
            AND ds BETWEEN 20240401 AND 20240731 --<< Production
            AND venture = 'VN'
            AND is_revenue = 1
            AND COALESCE(business_application, 'LZD') IN ('LZD,ZAL', 'LZD')
    ) AS t1
    LEFT JOIN (
        SELECT sales_order_item_id,
            is_lpi_promo,
            is_flash_promo,
            is_bau_promo
        FROM (
                SELECT t1.sales_order_item_id AS sales_order_item_id,
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
                            WHEN t1.promotion_id IN (
                                900000020540430,
                                900000023170149,
                                900000024370496,
                                900000028394148,
                                900000020102414,
                                900000023429035,
                                900000023435033,
                                900000025660054,
                                900000023157154,
                                900000026684173,
                                900000026946859,
                                900000028102124,
                                900000028108006,
                                900000020107414,
                                900000023841194,
                                900000020981765,
                                900000025011740,
                                900000023833265,
                                900000026202377,
                                900000028082120,
                                900000028406136,
                                900000021562012,
                                900000021758213,
                                900000022325266,
                                900000023837259,
                                900000021859555,
                                900000022821560,
                                900000024750603,
                                900000025115312,
                                900000025481273,
                                900000026668230,
                                900000028080116,
                                900000028380193,
                                900000020325687,
                                900000021865526,
                                900000025011381,
                                900000025110333,
                                900000022993652,
                                900000024045124,
                                900000024367538,
                                900000024758651,
                                900000028418102,
                                900000020726021,
                                900000022569789,
                                900000023161170,
                                900000023472456,
                                900000024745608,
                                900000026164362,
                                900000028122017,
                                900000028412113,
                                900000020924617,
                                900000025090145,
                                900000025117149,
                                900000026162397,
                                900000026194359,
                                900000021558039,
                                900000022567746,
                                900000024375503,
                                900000024992389,
                                900000020097429,
                                900000020716019,
                                900000022317296,
                                900000026656204,
                                900000024040127,
                                900000025646056,
                                900000028128011,
                                900000022071045,
                                900000022987625,
                                900000023678233,
                                900000024016001,
                                900000024041156,
                                900000024382494,
                                900000024753592,
                                900000024759593,
                                900000026988801,
                                900000028416110,
                                900000022911188,
                                900000026946861,
                                900000026978806,
                                900000028398156,
                                900000021350823
                            ) THEN 1
                            ELSE 0
                        END
                    ) AS is_flash_promo,
                    MAX(
                        CASE
                            WHEN t1.promotion_id IN (
                                900000021413226,
                                900000020328760,
                                900000020147309,
                                900000020319762,
                                900000020577220,
                                900000020915348,
                                900000021416204,
                                900000020146326,
                                900000020522520,
                                900000020549497,
                                900000020780255,
                                900000020775258,
                                900000020916378,
                                900000020929351,
                                900000020559280,
                                900000020325785,
                                900000020538469,
                                900000020781273,
                                900000020565268,
                                900000020333757,
                                900000020335773,
                                900000020580217,
                                900000020582217,
                                900000025387129,
                                900000025402575,
                                900000025645265,
                                900000026462071,
                                900000022197177,
                                900000023680881,
                                900000023807366,
                                900000020560274,
                                900000021431158,
                                900000023371426,
                                900000027936451,
                                900000020345726,
                                900000020572237,
                                900000023362432,
                                900000023520154,
                                900000024724025,
                                900000026496067,
                                900000022821166,
                                900000023557022,
                                900000024793392,
                                900000025187119,
                                900000021941542,
                                900000022204194,
                                900000022948562,
                                900000023354436,
                                900000023797390,
                                900000025396098,
                                900000023346425,
                                900000025395592,
                                900000020321765,
                                900000024801313,
                                900000025185123,
                                900000028524223,
                                900000020566281,
                                900000021277827,
                                900000021377028,
                                900000021956414,
                                900000022212168,
                                900000022809161,
                                900000022960559,
                                900000023366434,
                                900000023368423,
                                900000025396218,
                                900000021321874,
                                900000020583224,
                                900000023360393,
                                900000020348746,
                                900000022957570,
                                900000023689874,
                                900000023702822,
                                900000025185148,
                                900000025645266,
                                900000025651221,
                                900000027926517,
                                900000020338764,
                                900000020573259,
                                900000021964396,
                                900000023356439,
                                900000023562002,
                                900000023797389,
                                900000024713075,
                                900000025173227,
                                900000025632256,
                                900000026492061,
                                900000021294520,
                                900000023354417,
                                900000024679991,
                                900000025177145,
                                900000027764126,
                                900000027956445,
                                900000023343429,
                                900000025184138,
                                900000027766119,
                                900000020327769,
                                900000022820163,
                                900000024810302,
                                900000025640269,
                                900000025651222,
                                900000027716125
                            ) THEN 1
                            ELSE 0
                        END
                    ) AS is_bau_promo
                FROM (
                        SELECT *
                        FROM lazada_cdm.dwd_lzd_pro_promotion_item_di
                        WHERE 1 = 1 --
                            -- AND     ds = '${bizdate}' --<< Test
                            AND ds >= 20240101 --<< Production
                            AND venture = 'VN'
                            AND is_fulfilled = 1
                            AND promotion_type IN (
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
                        FROM lazada_cdm.dim_lzd_pro_collectibles
                        WHERE 1 = 1
                            AND ds = MAX_PT('lazada_cdm.dim_lzd_pro_collectibles')
                            AND venture = 'VN'
                    ) AS t2 ON t1.promotion_id = t2.promotion_id
                GROUP BY t1.sales_order_item_id
            )
        WHERE 1 = 1
            AND (
                is_lpi_promo = 1
                OR is_flash_promo = 1
                OR is_bau_promo = 1
            )
    ) AS t2 ON t1.sales_order_item_id = t2.sales_order_item_id
    LEFT JOIN (
        SELECT sales_order_item_id,
            SUM(
                CASE
                    WHEN promotion_type IN ('collectibleCoupon') THEN promotion_amount
                    ELSE 0
                END
            ) AS clvc_slr_amt,
            SUM(
                CASE
                    WHEN promotion_type IN ('shippingFee') THEN promotion_amount
                    ELSE 0
                END
            ) AS fs_slr_amt,
            SUM(
                CASE
                    WHEN promotion_type IN ('flexiCoin') THEN promotion_amount
                    ELSE 0
                END
            ) AS coin_slr_amt
        FROM lazada_cdm.dwd_lzd_pro_promotion_item_di
        WHERE 1 = 1 --
            -- AND     ds = '${bizdate}' --<< Test
            AND ds >= 20240101 --<< Production
            AND venture = 'VN'
            AND is_fulfilled = 1
            AND TOLOWER(promotion_role) IN ('seller')
        GROUP BY sales_order_item_id
    ) AS t3 ON t1.sales_order_item_id = t3.sales_order_item_id
    LEFT JOIN (
        SELECT seller_id,
            business_type,
            business_type_level2,
            seller_segment_v3_mapped
        FROM lazada_analyst_dev.datx_slr_performance_overview_seller_profile
        WHERE 1 = 1
            AND ds = MAX_PT("lazada_cdm.dwd_lzd_trd_core_fulfill_di")
    ) AS t5 ON t1.seller_id = t5.seller_id
    LEFT JOIN (
        SELECT ds,
            day_type,
            CASE
                WHEN TOLOWER(camp_type) IN ('mega') THEN 'Mega'
                WHEN TOLOWER(camp_type) IN ('pd') THEN 'A+'
                WHEN TOLOWER(camp_type) IN ('holiday') THEN 'BAU'
                ELSE camp_type
            END AS camp_type
        FROM lazada_analyst.cp_calendar
        WHERE 1 = 1
            AND ds >= 20240401
        GROUP BY ds,
            day_type,
            CASE
                WHEN TOLOWER(camp_type) IN ('mega') THEN 'Mega'
                WHEN TOLOWER(camp_type) IN ('pd') THEN 'A+'
                WHEN TOLOWER(camp_type) IN ('holiday') THEN 'BAU'
                ELSE camp_type
            END
    ) AS t6 ON t1.ds = t6.ds
GROUP BY TO_CHAR(TO_DATE(t1.ds, 'yyyymmdd'), 'yyyymm'),
    t1.ds,
    t6.camp_type,
    t5.seller_segment_v3_mapped,
    t5.business_type,
    t5.business_type_level2
ORDER BY t1.ds ASC,
    t5.seller_segment_v3_mapped ASC,
    t5.business_type ASC,
    t5.business_type_level2 ASC;
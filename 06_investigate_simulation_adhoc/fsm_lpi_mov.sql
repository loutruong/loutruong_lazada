-- MaxCompute SQL 
-- ********************************************************************--
-- author:Truong, Van Thanh
-- create time:2024-07-18 11:40:20
-- ********************************************************************--
--@@ Input = lazada_cdm.dwd_lzd_pro_fsmax_program_seller_detail_df
--@@ Input = lazada_cdm.dwd_lzd_trd_core_fulfill_di
--@@ Input = lazada_cdm.dwd_lzd_pro_promotion_item_di
--@@ Input = lazada_ads.dws_lzd_cdp_usr_uid_contribute_value_v2
--@@ Input = lazada_cdm.dim_lzd_usr
WITH t_mov_tag AS (
    SELECT check_out_id,
        unit_price,
        seller_cost_layer,
        lpi_mov_applied,
CASE
            WHEN ROUND(lpi_mov_applied, 0) BETWEEN 0 AND 0 THEN 'R01'
            WHEN ROUND(lpi_mov_applied) BETWEEN 1 AND 50000 THEN 'R02'
            WHEN ROUND(lpi_mov_applied) BETWEEN 50001 AND 100000 THEN 'R03'
            WHEN ROUND(lpi_mov_applied) BETWEEN 100001 AND 150000 THEN 'R04'
            WHEN ROUND(lpi_mov_applied) BETWEEN 150001 AND 200000 THEN 'R05'
            WHEN ROUND(lpi_mov_applied) BETWEEN 200001 AND 250000 THEN 'R06'
            WHEN ROUND(lpi_mov_applied) BETWEEN 250001 AND 300000 THEN 'R07'
            WHEN ROUND(lpi_mov_applied) BETWEEN 300001 AND 400000 THEN 'R08'
            WHEN ROUND(lpi_mov_applied) BETWEEN 400001 AND 500000 THEN 'R09'
            WHEN ROUND(lpi_mov_applied) BETWEEN 500001 AND 700000 THEN 'R10'
            WHEN ROUND(lpi_mov_applied) BETWEEN 700001 AND 1000000 THEN 'R11'
            WHEN ROUND(lpi_mov_applied) BETWEEN 1000001 AND 2000000 THEN 'R12'
            WHEN ROUND(lpi_mov_applied) BETWEEN 2000001 AND 3000000 THEN 'R13'
            WHEN ROUND(lpi_mov_applied) >= 3000001 THEN 'R14'
            ELSE 'Undified'
        END AS lpi_mov_applied_range,
CASE
            WHEN ROUND(lpi_mov_applied, 0) BETWEEN 0 AND 0 THEN '0'
            WHEN ROUND(lpi_mov_applied) BETWEEN 1 AND 50000 THEN '1'
            WHEN ROUND(lpi_mov_applied) BETWEEN 50001 AND 100000 THEN '50001'
            WHEN ROUND(lpi_mov_applied) BETWEEN 100001 AND 150000 THEN '100001'
            WHEN ROUND(lpi_mov_applied) BETWEEN 150001 AND 200000 THEN '150001'
            WHEN ROUND(lpi_mov_applied) BETWEEN 200001 AND 250000 THEN '200001'
            WHEN ROUND(lpi_mov_applied) BETWEEN 250001 AND 300000 THEN '250001'
            WHEN ROUND(lpi_mov_applied) BETWEEN 300001 AND 400000 THEN '300001'
            WHEN ROUND(lpi_mov_applied) BETWEEN 400001 AND 500000 THEN '400001'
            WHEN ROUND(lpi_mov_applied) BETWEEN 500001 AND 700000 THEN '500001'
            WHEN ROUND(lpi_mov_applied) BETWEEN 700001 AND 1000000 THEN '700001'
            WHEN ROUND(lpi_mov_applied) BETWEEN 1000001 AND 2000000 THEN '1000001'
            WHEN ROUND(lpi_mov_applied) BETWEEN 2000001 AND 3000000 THEN '2000001'
            WHEN ROUND(lpi_mov_applied) >= 3000001 THEN '3000001'
            ELSE 'Undified'
        END AS lpi_mov_applied_from,
CASE
            WHEN ROUND(lpi_mov_applied, 0) BETWEEN 0 AND 0 THEN '0'
            WHEN ROUND(lpi_mov_applied) BETWEEN 1 AND 50000 THEN '50000'
            WHEN ROUND(lpi_mov_applied) BETWEEN 50001 AND 100000 THEN '100000'
            WHEN ROUND(lpi_mov_applied) BETWEEN 100001 AND 150000 THEN '150000'
            WHEN ROUND(lpi_mov_applied) BETWEEN 150001 AND 200000 THEN '200000'
            WHEN ROUND(lpi_mov_applied) BETWEEN 200001 AND 250000 THEN '250000'
            WHEN ROUND(lpi_mov_applied) BETWEEN 250001 AND 300000 THEN '300000'
            WHEN ROUND(lpi_mov_applied) BETWEEN 300001 AND 400000 THEN '400000'
            WHEN ROUND(lpi_mov_applied) BETWEEN 400001 AND 500000 THEN '500000'
            WHEN ROUND(lpi_mov_applied) BETWEEN 500001 AND 700000 THEN '700000'
            WHEN ROUND(lpi_mov_applied) BETWEEN 700001 AND 1000000 THEN '1000000'
            WHEN ROUND(lpi_mov_applied) BETWEEN 1000001 AND 2000000 THEN '2000000'
            WHEN ROUND(lpi_mov_applied) BETWEEN 2000001 AND 3000000 THEN '3000000'
            WHEN ROUND(lpi_mov_applied) >= 3000001 THEN '>= 3000001'
            ELSE 'Undified'
        END AS lpi_mov_applied_to
    FROM (
            SELECT t1.check_out_id AS check_out_id,
                SUM(t1.unit_price) AS unit_price,
                SUM(COALESCE(t2.promo_amt_vnd, 0)) AS seller_cost_layer,
                SUM(t1.unit_price) - SUM(COALESCE(t2.promo_amt_vnd, 0)) AS lpi_mov_applied
            FROM (
                    SELECT check_out_id,
                        sales_order_item_id,
                        unit_price
                    FROM lazada_cdm.dwd_lzd_trd_core_fulfill_di
                    WHERE 1 = 1
                        AND ds IN (
                            20240704,
                            20240705,
                            20240706,
                            20240710,
                            20240711,
                            20240712,
                            20240713,
                            20240714,
                            20240718,
                            20240719,
                            20240720,
                            20240721,
                            20240722,
                            20240723,
                            20240724,
                            20240730,
                            20240731
                        )
                        AND venture = 'VN'
                        AND is_revenue = 1
                        AND COALESCE(business_application, 'LZD') IN ('LZD,ZAL', 'LZD')
                ) AS t1
                LEFT JOIN (
                    SELECT sales_order_item_id,
                        promotion_amount AS promo_amt_vnd
                    FROM lazada_cdm.dwd_lzd_pro_promotion_item_di
                    WHERE 1 = 1
                        AND ds >= 20240701 --<< CHANGE
                        AND venture = 'VN'
                        AND is_fulfilled = 1
                        AND TOLOWER(promotion_role) IN ('seller')
                        AND promotion_type NOT IN ('shippingFee', 'collectibleCoupon')
                ) AS t2 ON t1.sales_order_item_id = t2.sales_order_item_id
            GROUP BY t1.check_out_id
        )
)
SELECT t1.ds AS ds,
    t2.lpi_mov_applied_range AS lpi_mov_applied_range,
    t2.lpi_mov_applied_from AS lpi_mov_applied_from,
    t2.lpi_mov_applied_to AS lpi_mov_applied_to,
    COUNT(DISTINCT t1.order_id) AS plt_order_id_cnt,
    SUM(actual_gmv * exchange_rate) AS plt_gmv_usd,
    COUNT(
        DISTINCT CASE
            WHEN t3.sku_id IS NOT NULL THEN t1.order_id
            ELSE NULL
        END
    ) AS lpi_eligible_order_id_cnt,
    SUM(
        CASE
            WHEN t3.sku_id IS NOT NULL THEN actual_gmv * exchange_rate
            ELSE 0
        END
    ) AS lpi_eligible_gmv_usd,
    COUNT(
        DISTINCT CASE
            WHEN t4.sales_order_item_id IS NOT NULL THEN t1.order_id
            ELSE NULL
        END
    ) AS lpi_guided_order_id_cnt,
    SUM(
        CASE
            WHEN t4.sales_order_item_id IS NOT NULL THEN actual_gmv * exchange_rate
            ELSE 0
        END
    ) AS lpi_guided__gmv_usd
FROM (
        SELECT *
        FROM lazada_cdm.dwd_lzd_trd_core_fulfill_di
        WHERE 1 = 1
            AND ds = 20240707
            AND venture = 'VN'
            AND is_revenue = 1
            AND COALESCE(business_application, 'LZD') IN ('LZD,ZAL', 'LZD')
    ) AS t1
    LEFT JOIN (
        SELECT check_out_id,
            unit_price,
            seller_cost_layer,
            lpi_mov_applied,
            lpi_mov_applied_range,
            lpi_mov_applied_from,
            lpi_mov_applied_to
        FROM t_mov_tag
    ) AS t2 ON t1.check_out_id = t2.check_out_id
    LEFT JOIN (
        SELECT sku_id
        FROM lazada_cdm.dim_lzd_pro_lpi_sku
        WHERE 1 = 1
            AND ds = MAX_PT('lazada_cdm.dim_lzd_pro_lpi_sku')
            AND is_visible = 1
            AND master_campaign_id IN (431600)
            AND venture = 'VN'
    ) AS t3 ON t1.sku_id = t3.sku_id
    LEFT JOIN (
        SELECT DISTINCT sales_order_item_id
        FROM lazada_cdm.dwd_lzd_pro_promotion_item_di
        WHERE 1 = 1
            AND ds >= 20240707
            AND venture = 'VN'
            AND is_fulfilled = 1
            AND promotion_id IN (
                SELECT promotion_id
                FROM lazada_cdm.dim_lzd_pro_collectibles
                WHERE 1 = 1
                    AND ds = 20240709
                    AND venture = 'VN'
                    AND product_code IN ('categoryCoupon', 'collectibleCoupon')
                    AND (
                        TOLOWER(voucher_name) LIKE '%voucher%bonus%'
                        OR TOLOWER(description) LIKE '%voucher%bonus%'
                    )
                    AND TO_CHAR(promotion_start_date, 'yyyymmdd') >= 20240701
            )
    ) AS t4 ON t1.sales_order_item_id = t4.sales_order_item_id
GROUP BY t1.ds,
    t2.lpi_mov_applied_range,
    t2.lpi_mov_applied_from,
    t2.lpi_mov_applied_to
ORDER BY t2.lpi_mov_applied_range;
WITH t_fsm_mov_eligible_tag AS (
    SELECT ds AS ds,
        check_out_id AS check_out_id,
        mov_fsm_eligible AS mov_fsm_eligible,
CASE
            WHEN mov_fsm_eligible = 0 THEN '00_0k'
            WHEN mov_fsm_eligible > 0
            AND mov_fsm_eligible <= 20000 THEN '01_20k'
            WHEN mov_fsm_eligible > 20000
            AND mov_fsm_eligible <= 50000 THEN '02_50k'
            WHEN mov_fsm_eligible > 50000
            AND mov_fsm_eligible <= 100000 THEN '03_100k'
            WHEN mov_fsm_eligible > 100000
            AND mov_fsm_eligible <= 200000 THEN '04_200k'
            WHEN mov_fsm_eligible > 200000
            AND mov_fsm_eligible <= 300000 THEN '05_300k'
            WHEN mov_fsm_eligible > 300000
            AND mov_fsm_eligible <= 500000 THEN '06_500k'
            WHEN mov_fsm_eligible > 500000
            AND mov_fsm_eligible <= 800000 THEN '07_800k'
            WHEN mov_fsm_eligible > 800000
            AND mov_fsm_eligible <= 1000000 THEN '08_1000k'
            WHEN mov_fsm_eligible > 1000000 THEN '10_1000k++'
        END AS mov_fsm_eligible_range,
        sf AS sf,
CASE
            WHEN sf_fsm_eligible = 0 THEN '00_0k'
            WHEN sf_fsm_eligible > 0
            AND sf_fsm_eligible <= 5000 THEN '01_5k'
            WHEN sf_fsm_eligible > 5000
            AND sf_fsm_eligible <= 10000 THEN '02_10k'
            WHEN sf_fsm_eligible > 10000
            AND sf_fsm_eligible <= 15000 THEN '03_15k'
            WHEN sf_fsm_eligible > 15000
            AND sf_fsm_eligible <= 20000 THEN '04_20k'
            WHEN sf_fsm_eligible > 20000
            AND sf_fsm_eligible <= 30000 THEN '05_30k'
            WHEN sf_fsm_eligible > 30000
            AND sf_fsm_eligible <= 40000 THEN '06_40k'
            WHEN sf_fsm_eligible > 40000
            AND sf_fsm_eligible <= 50000 THEN '07_50k'
            WHEN sf_fsm_eligible > 50000
            AND sf_fsm_eligible <= 60000 THEN '08_60k'
            WHEN sf_fsm_eligible > 60000
            AND sf_fsm_eligible <= 80000 THEN '09_80k'
            WHEN sf_fsm_eligible > 80000
            AND sf_fsm_eligible <= 100000 THEN '10_100k'
            WHEN sf_fsm_eligible > 100000
            AND sf_fsm_eligible <= 150000 THEN '11_150k'
            WHEN sf_fsm_eligible > 150000
            AND sf_fsm_eligible <= 200000 THEN '12_200k'
            WHEN sf_fsm_eligible > 200000
            AND sf_fsm_eligible <= 250000 THEN '13_250k'
            WHEN sf_fsm_eligible > 250000
            AND sf_fsm_eligible <= 300000 THEN '14_300k'
            WHEN sf_fsm_eligible > 300000 THEN '15_300k++'
        END sf_fsm_eligible_range
    FROM (
            SELECT t1.ds AS ds,
                t1.check_out_id AS check_out_id,
                SUM(t1.unit_price) AS mov,
                SUM(
                    CASE
                        WHEN t2.seller_id IS NOT NULL THEN t1.unit_price
                        ELSE 0
                    END
                ) AS mov_fsm_eligible,
                SUM(t1.shipping_amount) AS sf,
                SUM(
                    CASE
                        WHEN t2.seller_id IS NOT NULL THEN t1.shipping_amount
                        ELSE 0
                    END
                ) AS sf_fsm_eligible
            FROM (
                    SELECT ds,
                        sales_order_item_id,
                        check_out_id,
                        unit_price,
                        shipping_amount --<< Shipping total cost
,
                        shipping_discount_amount --<< Shipping total cost discounted
,
                        seller_id
                    FROM lazada_cdm.dwd_lzd_trd_core_fulfill_di
                    WHERE 1 = 1
                        AND ds BETWEEN 20240701 AND 20240707 --<< Fixed data time range
                        AND venture = 'VN'
                        AND is_revenue = 1
                        AND COALESCE(business_application, 'LZD') IN ('LZD,ZAL', 'LZD')
                ) AS t1
                LEFT JOIN (
                    SELECT ds,
                        seller_id
                    FROM lazada_cdm.dwd_lzd_pro_fsmax_program_seller_detail_df
                    WHERE 1 = 1
                        AND ds BETWEEN 20240701 AND 20240707 --<< Fixed data time range
                        AND joined = 1
                        AND venture = 'VN'
                        AND ds BETWEEN TO_CHAR(
                            TO_DATE(start_time, 'yyyy-mm-dd hh:mi:ss'),
                            'yyyymmdd'
                        ) AND TO_CHAR(
                            TO_DATE(end_time, 'yyyy-mm-dd hh:mi:ss'),
                            'yyyymmdd'
                        ) --<< Running
                        AND suspend_time IS NULL --<< Not shut down
                        AND program_type = 1 --<< Exclude seller account test
                        AND seller_id NOT IN (202204217328, 200180502014) --<< Exclude seller account test, seller account bug
                ) AS t2 ON t1.ds = t2.ds
                AND t1.seller_id = t2.seller_id
            GROUP BY t1.ds,
                t1.check_out_id
        )
)
SELECT t1.ds AS ds,
    t3.mov_fsm_eligible_range AS mov_fsm_eligible_range,
    t3.sf_fsm_eligible_range AS sf_fsm_eligible_range,
    COALESCE(
        COALESCE(t7.user_contribution_tag, '05_non_user'),
        '00_all_segment'
    ) AS user_contribution_tag,
    COALESCE(
        COALESCE(t6.region, '03_non_metro'),
        '00_all_region'
    ) AS region,
    COUNT(
        DISTINCT CASE
            WHEN t2.seller_id IS NOT NULL THEN t1.check_out_id
            ELSE NULL
        END
    ) AS 00_check_out_id_fsm_eligible,
    COUNT(
        DISTINCT CASE
            WHEN t2.seller_id IS NOT NULL THEN t1.buyer_id
            ELSE NULL
        END
    ) AS 01_buyer_fsm_eligible,
    SUM(
        CASE
            WHEN t2.seller_id IS NOT NULL THEN COALESCE(t4.fsm_slr_exp_charge_vnd, 0) * COALESCE(t1.exchange_rate, 0)
            ELSE 0
        END
    ) AS 02_slr_fee_fsm_eligible,
    SUM(
        CASE
            WHEN t2.seller_id IS NOT NULL THEN COALESCE(t5.fsm_promo_amt_vnd, 0) * COALESCE(t1.exchange_rate, 0)
            ELSE 0
        END
    ) AS 03_promo_amt_fsm_eligible,
    SUM(
        CASE
            WHEN t2.seller_id IS NOT NULL THEN COALESCE(t1.shipping_amount, 0) * COALESCE(t1.exchange_rate, 0)
            ELSE 0
        END
    ) AS 04_shipping_fee_fsm_eligible,
    COUNT(
        DISTINCT CASE
            WHEN t2.seller_id IS NOT NULL
            AND t5.sales_order_item_id IS NOT NULL THEN t1.check_out_id
            ELSE NULL
        END
    ) AS 05_check_out_id_fsm_eligible_guided,
    SUM(
        CASE
            WHEN t2.seller_id IS NOT NULL
            AND t5.sales_order_item_id IS NOT NULL THEN COALESCE(t1.shipping_amount, 0) * COALESCE(t1.exchange_rate, 0)
            ELSE 0
        END
    ) AS 06_shipping_fee_fsm_eligible_guided
FROM (
        SELECT *
        FROM lazada_cdm.dwd_lzd_trd_core_fulfill_di
        WHERE 1 = 1
            AND ds BETWEEN 20240701 AND 20240707 --<< Fixed data time range
            AND venture = 'VN'
            AND is_revenue = 1
            AND COALESCE(business_application, 'LZD') IN ('LZD,ZAL', 'LZD')
    ) AS t1
    LEFT JOIN (
        SELECT ds,
            seller_id
        FROM lazada_cdm.dwd_lzd_pro_fsmax_program_seller_detail_df
        WHERE 1 = 1
            AND ds BETWEEN 20240701 AND 20240707 --<< Fixed data time range
            AND joined = 1
            AND venture = 'VN'
            AND ds BETWEEN TO_CHAR(
                TO_DATE(start_time, 'yyyy-mm-dd hh:mi:ss'),
                'yyyymmdd'
            ) AND TO_CHAR(
                TO_DATE(end_time, 'yyyy-mm-dd hh:mi:ss'),
                'yyyymmdd'
            ) --<< Running
            AND suspend_time IS NULL --<< Not shut down
            AND program_type = 1 --<< Exclude seller account test
            AND seller_id NOT IN (202204217328, 200180502014) --<< Exclude seller account test, seller account bug
    ) AS t2 ON t1.ds = t2.ds
    AND t1.seller_id = t2.seller_id
    LEFT JOIN (
        SELECT *
        FROM t_fsm_mov_eligible_tag
    ) AS t3 ON t1.check_out_id = t3.check_out_id
    LEFT JOIN (
        SELECT sales_order_item_id,
            SUM(
                COALESCE(KEYVALUE(exp_comm_amt_detail, 'FS_MAX'), 0)
            ) AS fsm_slr_exp_charge_vnd
        FROM lazada_cdm.dwd_lzd_trd_fulfill_spending_di
        WHERE 1 = 1
            AND venture = 'VN'
            AND ds BETWEEN 20240701 AND 20240707 --<< Fixed data time range
        GROUP BY sales_order_item_id
    ) AS t4 ON t1.sales_order_item_id = t4.sales_order_item_id
    LEFT JOIN (
        SELECT t1.sales_order_item_id AS sales_order_item_id,
            SUM(t1.promotion_amount) AS fsm_promo_amt_vnd
        FROM (
                SELECT *
                FROM lazada_cdm.dwd_lzd_pro_promotion_item_di
                WHERE 1 = 1
                    AND ds >= 20240601
                    AND venture = 'VN'
            ) AS t1
            INNER JOIN (
                SELECT DISTINCT promotion_id,
                    description
                FROM lazada_cdm.dim_lzd_pro_collectibles
                WHERE 1 = 1
                    AND ds = MAX_PT('lazada_cdm.dim_lzd_pro_collectibles')
                    AND venture = 'VN'
            ) AS t2 ON t1.promotion_id = t2.promotion_id
        WHERE 1 = 1
            AND t1.is_fulfilled = 1
            AND TOLOWER(t1.promotion_role) IN ('platform')
            AND (
                TOLOWER(t1.retail_sponsor) IN ('platform')
                OR t1.retail_sponsor IS NULL
            )
            AND t1.promotion_type IN ('shippingFeeCoupon')
            AND (
                TOLOWER(t1.promotion_name) LIKE '%platform_clm_seller boost program%'
                OR TOLOWER(t2.description) LIKE '%platform_clm_seller boost program%'
            )
        GROUP BY t1.sales_order_item_id
    ) AS t5 ON t1.sales_order_item_id = t5.sales_order_item_id
    LEFT JOIN (
        SELECT user_id,
CASE
                WHEN region2 LIKE '%Hồ Chí Minh%' THEN '01_metro'
                WHEN region2 LIKE '%Hà Nội%' THEN '01_metro'
                WHEN region2 LIKE '%Đà Nẵng%' THEN '01_metro'
                WHEN region2 LIKE '%Cần Thơ%' THEN '02_metro_plus'
                WHEN region2 LIKE '%Hải Phòng%' THEN '02_metro_plus'
                WHEN region2 LIKE '%Bắc Ninh%' THEN '02_metro_plus'
                WHEN region2 LIKE '%Bình Dương%' THEN '02_metro_plus'
                WHEN region2 LIKE '%Đồng Nai%' THEN '02_metro_plus'
                WHEN region2 LIKE '%Long An%' THEN '02_metro_plus'
                WHEN region2 LIKE '%Khánh Hòa%' THEN '02_metro_plus'
            END AS region
        FROM lazada_cdm.dim_lzd_usr
        WHERE 1 = 1
            AND ds = MAX_PT("lazada_cdm.dim_lzd_usr")
            AND venture = 'VN'
    ) AS t6 ON t1.buyer_id = t6.user_id
    LEFT JOIN (
        SELECT user_id,
CASE
                WHEN user_contribution_tag IN ('high') THEN '01_high'
                WHEN user_contribution_tag IN ('middle') THEN '02_middle'
                WHEN user_contribution_tag IN ('low') THEN '03_low' --
                WHEN user_contribution_tag IN ('lto 3-') THEN '04_lto3-'
            END AS user_contribution_tag
        FROM lazada_ads.dws_lzd_cdp_usr_uid_contribute_value_v2
        WHERE 1 = 1
            AND ds_month = MAX_PT(
                "lazada_ads.dws_lzd_cdp_usr_uid_contribute_value_v2"
            )
            AND venture = 'VN'
    ) AS t7 ON t1.buyer_id = t7.user_id
GROUP BY t1.ds,
    t3.mov_fsm_eligible_range,
    t3.sf_fsm_eligible_range,
    CUBE(
        COALESCE(t7.user_contribution_tag, '05_non_user'),
        COALESCE(t6.region, '03_non_metro')
    );
WITH t_fsm_mov_eligible_tag AS (
    SELECT ds AS ds,
        check_out_id AS check_out_id,
        mov_fsm_eligible AS mov_fsm_eligible,
CASE
            WHEN mov_fsm_eligible = 0 THEN '00_0k'
            WHEN mov_fsm_eligible > 0
            AND mov_fsm_eligible <= 20000 THEN '01_20k'
            WHEN mov_fsm_eligible > 20000
            AND mov_fsm_eligible <= 50000 THEN '02_50k'
            WHEN mov_fsm_eligible > 50000
            AND mov_fsm_eligible <= 100000 THEN '03_100k'
            WHEN mov_fsm_eligible > 100000
            AND mov_fsm_eligible <= 200000 THEN '04_200k'
            WHEN mov_fsm_eligible > 200000
            AND mov_fsm_eligible <= 300000 THEN '05_300k'
            WHEN mov_fsm_eligible > 300000
            AND mov_fsm_eligible <= 500000 THEN '06_500k'
            WHEN mov_fsm_eligible > 500000
            AND mov_fsm_eligible <= 800000 THEN '07_800k'
            WHEN mov_fsm_eligible > 800000
            AND mov_fsm_eligible <= 1000000 THEN '08_1000k'
            WHEN mov_fsm_eligible > 1000000 THEN '10_1000k++'
        END AS mov_fsm_eligible_range,
        sf AS sf,
CASE
            WHEN sf_fsm_eligible = 0 THEN '00_0k'
            WHEN sf_fsm_eligible > 0
            AND sf_fsm_eligible <= 5000 THEN '01_5k'
            WHEN sf_fsm_eligible > 5000
            AND sf_fsm_eligible <= 10000 THEN '02_10k'
            WHEN sf_fsm_eligible > 10000
            AND sf_fsm_eligible <= 15000 THEN '03_15k'
            WHEN sf_fsm_eligible > 15000
            AND sf_fsm_eligible <= 20000 THEN '04_20k'
            WHEN sf_fsm_eligible > 20000
            AND sf_fsm_eligible <= 30000 THEN '05_30k'
            WHEN sf_fsm_eligible > 30000
            AND sf_fsm_eligible <= 40000 THEN '06_40k'
            WHEN sf_fsm_eligible > 40000
            AND sf_fsm_eligible <= 50000 THEN '07_50k'
            WHEN sf_fsm_eligible > 50000
            AND sf_fsm_eligible <= 60000 THEN '08_60k'
            WHEN sf_fsm_eligible > 60000
            AND sf_fsm_eligible <= 80000 THEN '09_80k'
            WHEN sf_fsm_eligible > 80000
            AND sf_fsm_eligible <= 100000 THEN '10_100k'
            WHEN sf_fsm_eligible > 100000
            AND sf_fsm_eligible <= 150000 THEN '11_150k'
            WHEN sf_fsm_eligible > 150000
            AND sf_fsm_eligible <= 200000 THEN '12_200k'
            WHEN sf_fsm_eligible > 200000
            AND sf_fsm_eligible <= 250000 THEN '13_250k'
            WHEN sf_fsm_eligible > 250000
            AND sf_fsm_eligible <= 300000 THEN '14_300k'
            WHEN sf_fsm_eligible > 300000 THEN '15_300k++'
        END sf_fsm_eligible_range
    FROM (
            SELECT t1.ds AS ds,
                t1.check_out_id AS check_out_id,
                SUM(t1.unit_price) AS mov,
                SUM(
                    CASE
                        WHEN t2.seller_id IS NOT NULL THEN t1.unit_price
                        ELSE 0
                    END
                ) AS mov_fsm_eligible,
                SUM(t1.shipping_amount) AS sf,
                SUM(
                    CASE
                        WHEN t2.seller_id IS NOT NULL THEN t1.shipping_amount
                        ELSE 0
                    END
                ) AS sf_fsm_eligible
            FROM (
                    SELECT ds,
                        sales_order_item_id,
                        check_out_id,
                        unit_price,
                        shipping_amount --<< Shipping total cost
,
                        shipping_discount_amount --<< Shipping total cost discounted
,
                        seller_id
                    FROM lazada_cdm.dwd_lzd_trd_core_fulfill_di
                    WHERE 1 = 1
                        AND ds BETWEEN 20240701 AND 20240707 --<< Fixed data time range
                        AND venture = 'VN'
                        AND is_revenue = 1
                        AND COALESCE(business_application, 'LZD') IN ('LZD,ZAL', 'LZD')
                ) AS t1
                LEFT JOIN (
                    SELECT ds,
                        seller_id
                    FROM lazada_cdm.dwd_lzd_pro_fsmax_program_seller_detail_df
                    WHERE 1 = 1
                        AND ds BETWEEN 20240701 AND 20240707 --<< Fixed data time range
                        AND joined = 1
                        AND venture = 'VN'
                        AND ds BETWEEN TO_CHAR(
                            TO_DATE(start_time, 'yyyy-mm-dd hh:mi:ss'),
                            'yyyymmdd'
                        ) AND TO_CHAR(
                            TO_DATE(end_time, 'yyyy-mm-dd hh:mi:ss'),
                            'yyyymmdd'
                        ) --<< Running
                        AND suspend_time IS NULL --<< Not shut down
                        AND program_type = 1 --<< Exclude seller account test
                        AND seller_id NOT IN (202204217328, 200180502014) --<< Exclude seller account test, seller account bug
                ) AS t2 ON t1.ds = t2.ds
                AND t1.seller_id = t2.seller_id
            GROUP BY t1.ds,
                t1.check_out_id
        )
)
SELECT t3.mov_fsm_eligible_range AS mov_fsm_eligible_range,
    t3.sf_fsm_eligible_range AS sf_fsm_eligible_range,
    t1.*,
    COALESCE(t5.fsm_promo_amt_vnd, 0) AS fsm_promo_amt_vnd
FROM (
        SELECT *
        FROM lazada_cdm.dwd_lzd_trd_core_fulfill_di
        WHERE 1 = 1
            AND ds BETWEEN 20240701 AND 20240707 --<< Fixed data time range
            AND venture = 'VN'
            AND is_revenue = 1
            AND COALESCE(business_application, 'LZD') IN ('LZD,ZAL', 'LZD')
    ) AS t1
    LEFT JOIN (
        SELECT ds,
            seller_id
        FROM lazada_cdm.dwd_lzd_pro_fsmax_program_seller_detail_df
        WHERE 1 = 1
            AND ds BETWEEN 20240701 AND 20240707 --<< Fixed data time range
            AND joined = 1
            AND venture = 'VN'
            AND ds BETWEEN TO_CHAR(
                TO_DATE(start_time, 'yyyy-mm-dd hh:mi:ss'),
                'yyyymmdd'
            ) AND TO_CHAR(
                TO_DATE(end_time, 'yyyy-mm-dd hh:mi:ss'),
                'yyyymmdd'
            ) --<< Running
            AND suspend_time IS NULL --<< Not shut down
            AND program_type = 1 --<< Exclude seller account test
            AND seller_id NOT IN (202204217328, 200180502014) --<< Exclude seller account test, seller account bug
    ) AS t2 ON t1.ds = t2.ds
    AND t1.seller_id = t2.seller_id
    LEFT JOIN (
        SELECT *
        FROM t_fsm_mov_eligible_tag
    ) AS t3 ON t1.check_out_id = t3.check_out_id
    LEFT JOIN (
        SELECT sales_order_item_id,
            SUM(
                COALESCE(KEYVALUE(exp_comm_amt_detail, 'FS_MAX'), 0)
            ) AS fsm_slr_exp_charge_vnd
        FROM lazada_cdm.dwd_lzd_trd_fulfill_spending_di
        WHERE 1 = 1
            AND venture = 'VN'
            AND ds BETWEEN 20240701 AND 20240707 --<< Fixed data time range
        GROUP BY sales_order_item_id
    ) AS t4 ON t1.sales_order_item_id = t4.sales_order_item_id
    LEFT JOIN (
        SELECT t1.sales_order_item_id AS sales_order_item_id,
            SUM(t1.promotion_amount) AS fsm_promo_amt_vnd
        FROM (
                SELECT *
                FROM lazada_cdm.dwd_lzd_pro_promotion_item_di
                WHERE 1 = 1
                    AND ds >= 20240601
                    AND venture = 'VN'
            ) AS t1
            INNER JOIN (
                SELECT DISTINCT promotion_id,
                    description
                FROM lazada_cdm.dim_lzd_pro_collectibles
                WHERE 1 = 1
                    AND ds = MAX_PT('lazada_cdm.dim_lzd_pro_collectibles')
                    AND venture = 'VN'
            ) AS t2 ON t1.promotion_id = t2.promotion_id
        WHERE 1 = 1
            AND t1.is_fulfilled = 1
            AND TOLOWER(t1.promotion_role) IN ('platform')
            AND (
                TOLOWER(t1.retail_sponsor) IN ('platform')
                OR t1.retail_sponsor IS NULL
            )
            AND t1.promotion_type IN ('shippingFeeCoupon')
            AND (
                TOLOWER(t1.promotion_name) LIKE '%platform_clm_seller boost program%'
                OR TOLOWER(t2.description) LIKE '%platform_clm_seller boost program%'
            )
        GROUP BY t1.sales_order_item_id
    ) AS t5 ON t1.sales_order_item_id = t5.sales_order_item_id
    LEFT JOIN (
        SELECT user_id,
CASE
                WHEN region2 LIKE '%Hồ Chí Minh%' THEN '01_metro'
                WHEN region2 LIKE '%Hà Nội%' THEN '01_metro'
                WHEN region2 LIKE '%Đà Nẵng%' THEN '01_metro'
                WHEN region2 LIKE '%Cần Thơ%' THEN '02_metro_plus'
                WHEN region2 LIKE '%Hải Phòng%' THEN '02_metro_plus'
                WHEN region2 LIKE '%Bắc Ninh%' THEN '02_metro_plus'
                WHEN region2 LIKE '%Bình Dương%' THEN '02_metro_plus'
                WHEN region2 LIKE '%Đồng Nai%' THEN '02_metro_plus'
                WHEN region2 LIKE '%Long An%' THEN '02_metro_plus'
                WHEN region2 LIKE '%Khánh Hòa%' THEN '02_metro_plus'
            END AS region
        FROM lazada_cdm.dim_lzd_usr
        WHERE 1 = 1
            AND ds = MAX_PT("lazada_cdm.dim_lzd_usr")
            AND venture = 'VN'
    ) AS t6 ON t1.buyer_id = t6.user_id
    LEFT JOIN (
        SELECT user_id,
CASE
                WHEN user_contribution_tag IN ('high') THEN '01_high'
                WHEN user_contribution_tag IN ('middle') THEN '02_middle'
                WHEN user_contribution_tag IN ('low') THEN '03_low' --
                WHEN user_contribution_tag IN ('lto 3-') THEN '04_lto3-'
            END AS user_contribution_tag
        FROM lazada_ads.dws_lzd_cdp_usr_uid_contribute_value_v2
        WHERE 1 = 1
            AND ds_month = MAX_PT(
                "lazada_ads.dws_lzd_cdp_usr_uid_contribute_value_v2"
            )
            AND venture = 'VN'
    ) AS t7 ON t1.buyer_id = t7.user_id
WHERE 1 = 1
    AND t2.seller_id IS NOT NULL
    AND t5.sales_order_item_id IS NOT NULL
    AND COALESCE(t5.fsm_promo_amt_vnd, 0) = 0
ORDER BY t1.check_out_id ASC;
WITH t_fsm_mov_eligible_tag AS (
    SELECT ds AS ds,
        check_out_id AS check_out_id,
        mov_fsm_eligible AS mov_fsm_eligible,
CASE
            WHEN mov_fsm_eligible = 0 THEN '00_0k'
            WHEN mov_fsm_eligible > 0
            AND mov_fsm_eligible <= 20000 THEN '01_20k'
            WHEN mov_fsm_eligible > 20000
            AND mov_fsm_eligible <= 50000 THEN '02_50k'
            WHEN mov_fsm_eligible > 50000
            AND mov_fsm_eligible <= 100000 THEN '03_100k'
            WHEN mov_fsm_eligible > 100000
            AND mov_fsm_eligible <= 200000 THEN '04_200k'
            WHEN mov_fsm_eligible > 200000
            AND mov_fsm_eligible <= 300000 THEN '05_300k'
            WHEN mov_fsm_eligible > 300000
            AND mov_fsm_eligible <= 500000 THEN '06_500k'
            WHEN mov_fsm_eligible > 500000
            AND mov_fsm_eligible <= 800000 THEN '07_800k'
            WHEN mov_fsm_eligible > 800000
            AND mov_fsm_eligible <= 1000000 THEN '08_1000k'
            WHEN mov_fsm_eligible > 1000000 THEN '10_1000k++'
        END AS mov_fsm_eligible_range,
        sf AS sf,
CASE
            WHEN sf_fsm_eligible = 0 THEN '00_0k'
            WHEN sf_fsm_eligible > 0
            AND sf_fsm_eligible <= 5000 THEN '01_5k'
            WHEN sf_fsm_eligible > 5000
            AND sf_fsm_eligible <= 10000 THEN '02_10k'
            WHEN sf_fsm_eligible > 10000
            AND sf_fsm_eligible <= 15000 THEN '03_15k'
            WHEN sf_fsm_eligible > 15000
            AND sf_fsm_eligible <= 20000 THEN '04_20k'
            WHEN sf_fsm_eligible > 20000
            AND sf_fsm_eligible <= 30000 THEN '05_30k'
            WHEN sf_fsm_eligible > 30000
            AND sf_fsm_eligible <= 40000 THEN '06_40k'
            WHEN sf_fsm_eligible > 40000
            AND sf_fsm_eligible <= 50000 THEN '07_50k'
            WHEN sf_fsm_eligible > 50000
            AND sf_fsm_eligible <= 60000 THEN '08_60k'
            WHEN sf_fsm_eligible > 60000
            AND sf_fsm_eligible <= 80000 THEN '09_80k'
            WHEN sf_fsm_eligible > 80000
            AND sf_fsm_eligible <= 100000 THEN '10_100k'
            WHEN sf_fsm_eligible > 100000
            AND sf_fsm_eligible <= 150000 THEN '11_150k'
            WHEN sf_fsm_eligible > 150000
            AND sf_fsm_eligible <= 200000 THEN '12_200k'
            WHEN sf_fsm_eligible > 200000
            AND sf_fsm_eligible <= 250000 THEN '13_250k'
            WHEN sf_fsm_eligible > 250000
            AND sf_fsm_eligible <= 300000 THEN '14_300k'
            WHEN sf_fsm_eligible > 300000 THEN '15_300k++'
        END sf_fsm_eligible_range
    FROM (
            SELECT t1.ds AS ds,
                t1.check_out_id AS check_out_id,
                SUM(t1.unit_price) AS mov,
                SUM(
                    CASE
                        WHEN t2.seller_id IS NOT NULL THEN t1.unit_price
                        ELSE 0
                    END
                ) AS mov_fsm_eligible,
                SUM(t1.shipping_amount) AS sf,
                SUM(
                    CASE
                        WHEN t2.seller_id IS NOT NULL THEN t1.shipping_amount
                        ELSE 0
                    END
                ) AS sf_fsm_eligible
            FROM (
                    SELECT ds,
                        sales_order_item_id,
                        check_out_id,
                        unit_price,
                        shipping_amount --<< Shipping total cost
,
                        shipping_discount_amount --<< Shipping total cost discounted
,
                        seller_id
                    FROM lazada_cdm.dwd_lzd_trd_core_fulfill_di
                    WHERE 1 = 1
                        AND ds BETWEEN 20240701 AND 20240707 --<< Fixed data time range
                        AND venture = 'VN'
                        AND is_revenue = 1
                        AND COALESCE(business_application, 'LZD') IN ('LZD,ZAL', 'LZD')
                ) AS t1
                LEFT JOIN (
                    SELECT ds,
                        seller_id
                    FROM lazada_cdm.dwd_lzd_pro_fsmax_program_seller_detail_df
                    WHERE 1 = 1
                        AND ds BETWEEN 20240701 AND 20240707 --<< Fixed data time range
                        AND joined = 1
                        AND venture = 'VN'
                        AND ds BETWEEN TO_CHAR(
                            TO_DATE(start_time, 'yyyy-mm-dd hh:mi:ss'),
                            'yyyymmdd'
                        ) AND TO_CHAR(
                            TO_DATE(end_time, 'yyyy-mm-dd hh:mi:ss'),
                            'yyyymmdd'
                        ) --<< Running
                        AND suspend_time IS NULL --<< Not shut down
                        AND program_type = 1 --<< Exclude seller account test
                        AND seller_id NOT IN (202204217328, 200180502014) --<< Exclude seller account test, seller account bug
                ) AS t2 ON t1.ds = t2.ds
                AND t1.seller_id = t2.seller_id
            GROUP BY t1.ds,
                t1.check_out_id
        )
)
SELECT t1.ds AS ds,
    t3.mov_fsm_eligible_range AS mov_fsm_eligible_range,
    t3.sf_fsm_eligible_range AS sf_fsm_eligible_range,
    COALESCE(
        COALESCE(t7.user_contribution_tag, '05_non_user'),
        '00_all_segment'
    ) AS user_contribution_tag,
    t6.region2 AS region,
    COUNT(
        DISTINCT CASE
            WHEN t2.seller_id IS NOT NULL THEN t1.check_out_id
            ELSE NULL
        END
    ) AS 00_check_out_id_fsm_eligible,
    COUNT(
        DISTINCT CASE
            WHEN t2.seller_id IS NOT NULL THEN t1.buyer_id
            ELSE NULL
        END
    ) AS 01_buyer_fsm_eligible,
    SUM(
        CASE
            WHEN t2.seller_id IS NOT NULL THEN COALESCE(t4.fsm_slr_exp_charge_vnd, 0) * COALESCE(t1.exchange_rate, 0)
            ELSE 0
        END
    ) AS 02_slr_fee_fsm_eligible,
    SUM(
        CASE
            WHEN t2.seller_id IS NOT NULL THEN COALESCE(t5.fsm_promo_amt_vnd, 0) * COALESCE(t1.exchange_rate, 0)
            ELSE 0
        END
    ) AS 03_promo_amt_fsm_eligible,
    SUM(
        CASE
            WHEN t2.seller_id IS NOT NULL THEN COALESCE(t1.shipping_amount, 0) * COALESCE(t1.exchange_rate, 0)
            ELSE 0
        END
    ) AS 04_shipping_fee_fsm_eligible,
    COUNT(
        DISTINCT CASE
            WHEN t2.seller_id IS NOT NULL
            AND t5.sales_order_item_id IS NOT NULL THEN t1.check_out_id
            ELSE NULL
        END
    ) AS 05_check_out_id_fsm_eligible_guided,
    SUM(
        CASE
            WHEN t2.seller_id IS NOT NULL
            AND t5.sales_order_item_id IS NOT NULL THEN COALESCE(t1.shipping_amount, 0) * COALESCE(t1.exchange_rate, 0)
            ELSE 0
        END
    ) AS 06_shipping_fee_fsm_eligible_guided
FROM (
        SELECT *
        FROM lazada_cdm.dwd_lzd_trd_core_fulfill_di
        WHERE 1 = 1
            AND ds BETWEEN 20240701 AND 20240707 --<< Fixed data time range
            AND venture = 'VN'
            AND is_revenue = 1
            AND COALESCE(business_application, 'LZD') IN ('LZD,ZAL', 'LZD')
    ) AS t1
    LEFT JOIN (
        SELECT ds,
            seller_id
        FROM lazada_cdm.dwd_lzd_pro_fsmax_program_seller_detail_df
        WHERE 1 = 1
            AND ds BETWEEN 20240701 AND 20240707 --<< Fixed data time range
            AND joined = 1
            AND venture = 'VN'
            AND ds BETWEEN TO_CHAR(
                TO_DATE(start_time, 'yyyy-mm-dd hh:mi:ss'),
                'yyyymmdd'
            ) AND TO_CHAR(
                TO_DATE(end_time, 'yyyy-mm-dd hh:mi:ss'),
                'yyyymmdd'
            ) --<< Running
            AND suspend_time IS NULL --<< Not shut down
            AND program_type = 1 --<< Exclude seller account test
            AND seller_id NOT IN (202204217328, 200180502014) --<< Exclude seller account test, seller account bug
    ) AS t2 ON t1.ds = t2.ds
    AND t1.seller_id = t2.seller_id
    LEFT JOIN (
        SELECT *
        FROM t_fsm_mov_eligible_tag
    ) AS t3 ON t1.check_out_id = t3.check_out_id
    LEFT JOIN (
        SELECT sales_order_item_id,
            SUM(
                COALESCE(KEYVALUE(exp_comm_amt_detail, 'FS_MAX'), 0)
            ) AS fsm_slr_exp_charge_vnd
        FROM lazada_cdm.dwd_lzd_trd_fulfill_spending_di
        WHERE 1 = 1
            AND venture = 'VN'
            AND ds BETWEEN 20240701 AND 20240707 --<< Fixed data time range
        GROUP BY sales_order_item_id
    ) AS t4 ON t1.sales_order_item_id = t4.sales_order_item_id
    LEFT JOIN (
        SELECT t1.sales_order_item_id AS sales_order_item_id,
            SUM(t1.promotion_amount) AS fsm_promo_amt_vnd
        FROM (
                SELECT *
                FROM lazada_cdm.dwd_lzd_pro_promotion_item_di
                WHERE 1 = 1
                    AND ds >= 20240601
                    AND venture = 'VN'
            ) AS t1
            INNER JOIN (
                SELECT DISTINCT promotion_id,
                    description
                FROM lazada_cdm.dim_lzd_pro_collectibles
                WHERE 1 = 1
                    AND ds = MAX_PT('lazada_cdm.dim_lzd_pro_collectibles')
                    AND venture = 'VN'
            ) AS t2 ON t1.promotion_id = t2.promotion_id
        WHERE 1 = 1
            AND t1.is_fulfilled = 1
            AND TOLOWER(t1.promotion_role) IN ('platform')
            AND (
                TOLOWER(t1.retail_sponsor) IN ('platform')
                OR t1.retail_sponsor IS NULL
            )
            AND t1.promotion_type IN ('shippingFeeCoupon')
            AND (
                TOLOWER(t1.promotion_name) LIKE '%platform_clm_seller boost program%'
                OR TOLOWER(t2.description) LIKE '%platform_clm_seller boost program%'
            )
        GROUP BY t1.sales_order_item_id
    ) AS t5 ON t1.sales_order_item_id = t5.sales_order_item_id
    LEFT JOIN (
        SELECT user_id,
            region2,
CASE
                WHEN region2 LIKE '%Hồ Chí Minh%' THEN '01_metro'
                WHEN region2 LIKE '%Hà Nội%' THEN '01_metro'
                WHEN region2 LIKE '%Đà Nẵng%' THEN '01_metro'
                WHEN region2 LIKE '%Cần Thơ%' THEN '02_metro_plus'
                WHEN region2 LIKE '%Hải Phòng%' THEN '02_metro_plus'
                WHEN region2 LIKE '%Bắc Ninh%' THEN '02_metro_plus'
                WHEN region2 LIKE '%Bình Dương%' THEN '02_metro_plus'
                WHEN region2 LIKE '%Đồng Nai%' THEN '02_metro_plus'
                WHEN region2 LIKE '%Long An%' THEN '02_metro_plus'
                WHEN region2 LIKE '%Khánh Hòa%' THEN '02_metro_plus'
            END AS region
        FROM lazada_cdm.dim_lzd_usr
        WHERE 1 = 1
            AND ds = MAX_PT("lazada_cdm.dim_lzd_usr")
            AND venture = 'VN'
    ) AS t6 ON t1.buyer_id = t6.user_id
    LEFT JOIN (
        SELECT user_id,
CASE
                WHEN user_contribution_tag IN ('high') THEN '01_high'
                WHEN user_contribution_tag IN ('middle') THEN '02_middle'
                WHEN user_contribution_tag IN ('low') THEN '03_low' --
                WHEN user_contribution_tag IN ('lto 3-') THEN '04_lto3-'
            END AS user_contribution_tag
        FROM lazada_ads.dws_lzd_cdp_usr_uid_contribute_value_v2
        WHERE 1 = 1
            AND ds_month = MAX_PT(
                "lazada_ads.dws_lzd_cdp_usr_uid_contribute_value_v2"
            )
            AND venture = 'VN'
    ) AS t7 ON t1.buyer_id = t7.user_id
WHERE 1 = 1
    AND t6.region2 LIKE '%Hà Nội%'
GROUP BY t1.ds,
    t3.mov_fsm_eligible_range,
    t3.sf_fsm_eligible_range,
    t6.region2,
    CUBE(COALESCE(t7.user_contribution_tag, '05_non_user'));
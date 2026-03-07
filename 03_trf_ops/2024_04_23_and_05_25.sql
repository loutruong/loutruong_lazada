-- MaxCompute SQL 
-- ********************************************************************--
-- author:Truong, Van Thanh
-- create time:2024-04-24 18:33:04
-- ********************************************************************--
--@@ Input = lazada_analyst_dev.loutruong_aff_console_di
--@@ Input = lazada_analyst_dev.loutruong_aff_console_df
SELECT 'Data range: 20240401 -->> 20240422' AS data_range,
    '22' AS data_duration,
    member_id,
    member_name,
    SUM(brand_payout_vnd) AS brand_payout_vnd,
    SUM(brand_payout_vnd) * 0.25 AS bonus
FROM lazada_analyst_dev.loutruong_aff_console_df
WHERE 1 = 1
    AND ds = '20240422'
    AND order_create_date BETWEEN '20240401' AND '20240422'
    AND TOLOWER(status) IN ('delivered')
    AND TOLOWER(adjust_type) NOT IN ('stop_first_order')
    AND is_fraud = 0
    AND member_id IN (242831252, 150131327, 150471256)
GROUP BY member_id,
    member_name;
SELECT 'Data range: 20240423 -->> 20240430' AS data_range,
    '08' AS data_duration,
    member_id,
    member_name,
    SUM(brand_payout_vnd) AS brand_payout_vnd,
    SUM(brand_payout_vnd) * 0.25 AS bonus
FROM lazada_analyst_dev.loutruong_aff_console_di
WHERE 1 = 1
    AND ds BETWEEN '20240401' AND '20240430'
    AND TOLOWER(status) IN ('delivered')
    AND TOLOWER(adjust_type) NOT IN ('stop_first_order')
    AND is_fraud = 0
    AND member_id IN (242831252, 150131327, 150471256)
GROUP BY member_id,
    member_name;
SELECT 'Data range: 20240501 -->> 20240525' AS data_range,
    '25' AS data_duration,
    member_id,
    member_name,
    SUM(COALESCE(brand_commission_fee, 0)) AS brand_payout_vnd,
    SUM(COALESCE(brand_commission_fee, 0)) * 0.25 AS bonus
FROM lazada_ads.ads_lzd_marketing_cps_conversion_report_mi
WHERE 1 = 1
    AND mm >= 202404
    AND venture = 'VN'
    AND TO_CHAR(
        TO_DATE(conversion_time, 'yyyy-mm-ddTHH:mi'),
        'yyyymmdd'
    ) BETWEEN '20240501' AND '20240525'
    AND TOLOWER(status) IN ('delivered')
    AND TOLOWER(adjust_type) NOT IN ('stop_first_order')
    AND COALESCE(is_fraud, 0) = 0
    AND source IS NOT NULL
    AND member_id IN (242831252, 150131327, 150471256)
GROUP BY member_id,
    member_name;
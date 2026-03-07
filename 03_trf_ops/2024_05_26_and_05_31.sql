-- MaxCompute SQL 
-- ********************************************************************--
-- author:Truong, Van Thanh
-- create time:2024-06-27 10:52:03
-- ********************************************************************--
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
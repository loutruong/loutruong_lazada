-- MaxCompute SQL 
-- ********************************************************************--
-- author:Truong, Van Thanh
-- create time:2024-07-08 12:22:56
-- ********************************************************************--
--@@ Input = lazada_ads.dws_lzd_cdp_usr_uid_contribute_value_v2
SELECT *
FROM lazada_ads.dws_lzd_cdp_usr_uid_contribute_value_v2
WHERE 1 = 1
    AND ds_month = MAX_PT(
        "lazada_ads.dws_lzd_cdp_usr_uid_contribute_value_v2"
    )
    AND venture = 'VN'
    AND user_id IN (1494200);
SELECT *
FROM lazada_cdm.dws_lzd_trd_byr_ord_ext_nd
LIMIT 10;
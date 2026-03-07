-- MaxCompute SQL 
-- ********************************************************************--
-- author:vanthanh.truong
-- create time:2023-12-02 22:26:46
-- ********************************************************************--
--@@ Input = lazada_analyst_dev.loutruong_dim_hh
--@@ Input = lazada_cdm.dwd_lzd_mkt_app_ft_1d_np_hi
--@@ Input = lazada_cdm.dwd_lzd_mkt_app_pv_mp_hi
--@@ Input = alilog.dws_lzd_log_ut_active_utdid_1d
--@@ Input = lazada_cdm.dwd_lzd_mkt_a2c_uam_hi
--@@ Input = lazada_cdm.dwd_lzd_mkt_trn_uam_hi
SELECT COUNT(DISTINCT utdid) AS app_dau,
    COUNT(DISTINCT session_id) AS app_visit,
    COUNT(DISTINCT CONCAT(utdid, asc_item_id))
FROM lazada_cdm.dwd_lzd_mkt_app_ft_1d_np_hi
WHERE 1 = 1
    AND ds = '${bizdate}'
    AND venture = 'VN';
SELECT ds,
    hh,
    COUNT(DISTINCT utdid)
FROM lazada_cdm.dwd_lzd_mkt_app_ft_1d_np_hi
WHERE 1 = 1
    AND ds = TO_CHAR(GETDATE(), 'yyyymmdd')
    AND venture = 'VN'
    AND url_type IN ('ipv')
GROUP BY ds,
    hh
UNION ALL
SELECT ds,
    hh,
    app_pdp_uv_cnt_ft_1d_np
FROM lazada_analyst_dev.tmp_loutruong_plt_perf_hh_1;
SELECT 'lt_1d' AS lable,
    FORMAT_NUMBER(COUNT(DISTINCT utdid), 0) AS app_dau
FROM lazada_cdm.dws_lzd_mkt_app_ad_mp_di
WHERE 1 = 1
    AND ds = '${bizdate}'
    AND venture = 'VN'
    AND attr_model IN ('lt_1d')
    AND utdid IS NOT NULL
    AND is_active_utdid = 1;
SELECT FORMAT_NUMBER(COUNT(DISTINCT utdid), 0) AS app_dau
FROM lazada_cdm.dwd_lzd_mkt_app_pv_mp_hi
WHERE 1 = 1
    AND ds = '${bizdate}'
    AND venture = 'VN'
    AND is_direct_1d <> 'NULL'
    AND utdid IS NOT NULL
    AND utdid IN (
        SELECT DISTINCT active_utdid
        FROM alilog.dws_lzd_log_ut_active_utdid_1d
        WHERE 1 = 1
            AND ds = '${bizdate}'
            AND venture = 'VN'
    )
    AND url_type IN ('ipv');
SELECT COUNT(DISTINCT t1.utdid) AS app_dau
FROM (
        SELECT *
        FROM lazada_cdm.dwd_lzd_mkt_app_pv_mp_hi
        WHERE 1 = 1
            AND ds = '${bizdate}'
            AND venture = 'VN'
            AND is_direct_1d <> 'NULL'
    ) AS t1
    INNER JOIN (
        SELECT utdid
        FROM lazada_cdm.dws_lzd_mkt_app_ad_mp_di
        WHERE 1 = 1
            AND ds = '${bizdate}'
            AND venture = 'VN'
            AND attr_model IN ('lt_1d')
            AND utdid IS NOT NULL
            AND is_active_utdid = 1
        GROUP BY utdid
    ) AS t2 ON t1.utdid = t2.utdid;
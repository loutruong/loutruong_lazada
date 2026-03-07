-- MaxCompute SQL 
-- ********************************************************************--
-- author:Truong, Van Thanh
-- create time:2024-01-08 12:55:35
-- ********************************************************************--
-- ********************************************************************--
-- CREATE TEMPORARY PHONE RECEIVED ZNS MESSAGE
-- ********************************************************************--
-- DROP TABLE IF EXISTS lazada_analyst_dev.tmp_loutruong_zns_receive_df
-- ;
-- CREATE TABLE IF NOT EXISTS lazada_analyst_dev.tmp_loutruong_zns_receive_df
-- (
--     phone   STRING
--     ,ds_zns STRING COMMENT 'date push/flash/shot zalo'
-- )
-- PARTITIONED BY 
-- (
--     ds      STRING COMMENT 'execute date data snapshot'
-- )
-- LIFECYCLE 3600
-- ;
-- ********************************************************************-- manually_upload_date = 20231222 -- ********************************************************************--
-- INSERT OVERWRITE TABLE lazada_analyst_dev.tmp_loutruong_zns_receive_df PARTITION (ds = '${bizdate}')
-- SELECT  phone
--         ,ds_zns
-- FROM    (
--             SELECT  COALESCE(SPLIT(col_1,';')[3],'') AS phone
--                     ,TO_CHAR(TO_DATE(COALESCE(SPLIT(col_1,';')[18],''),'yyyy-mm-dd hh:mi:ss.ff3'),'yyyymmdd') AS ds_zns
--             FROM    lazada_analyst_dev.tmp_loutruong_zns_upload_1 --<< TABLE 1 >>--
--             WHERE   1 = 1
--             AND     TOLOWER(COALESCE(SPLIT(col_1,';')[12],'')) IN ('delivered')
--             GROUP BY COALESCE(SPLIT(col_1,';')[3],'')
--                      ,TO_CHAR(TO_DATE(COALESCE(SPLIT(col_1,';')[18],''),'yyyy-mm-dd hh:mi:ss.ff3'),'yyyymmdd')
--             UNION ALL --<< Break point
--             SELECT  COALESCE(SPLIT(col_1,';')[3],'') AS phone
--                     ,TO_CHAR(TO_DATE(COALESCE(SPLIT(col_1,';')[18],''),'yyyy-mm-dd hh:mi:ss.ff3'),'yyyymmdd') AS ds_zns
--             FROM    lazada_analyst_dev.tmp_loutruong_zns_upload_2 --<< TABLE 2 >>--
--             WHERE   1 = 1
--             AND     TOLOWER(COALESCE(SPLIT(col_1,';')[12],'')) IN ('delivered')
--             GROUP BY COALESCE(SPLIT(col_1,';')[3],'')
--                      ,TO_CHAR(TO_DATE(COALESCE(SPLIT(col_1,';')[18],''),'yyyy-mm-dd hh:mi:ss.ff3'),'yyyymmdd')
--             UNION ALL --<< Break point
--             SELECT  COALESCE(SPLIT(col_1,';')[3],'') AS phone
--                     ,TO_CHAR(TO_DATE(COALESCE(SPLIT(col_1,';')[18],''),'yyyy-mm-dd hh:mi:ss.ff3'),'yyyymmdd') AS ds_zns
--             FROM    lazada_analyst_dev.tmp_loutruong_zns_upload_3 --<< TABLE 3 >>--
--             WHERE   1 = 1
--             AND     TOLOWER(COALESCE(SPLIT(col_1,';')[12],'')) IN ('delivered')
--             GROUP BY COALESCE(SPLIT(col_1,';')[3],'')
--                      ,TO_CHAR(TO_DATE(COALESCE(SPLIT(col_1,';')[18],''),'yyyy-mm-dd hh:mi:ss.ff3'),'yyyymmdd')
--             UNION ALL --<< Break point
--             SELECT  to AS phone
--                     ,TO_CHAR(TO_DATE(col_19,'yyyy-mm-dd hh:mi:ss.ff3'),'yyyymmdd') AS ds_zns
--             FROM    lazada_analyst_dev.tmp_loutruong_zns_upload_4 --<< TABLE 4 >>--
--             WHERE   1 = 1
--             AND     TOLOWER(status) IN ('delivered')
--             GROUP BY to
--                      ,TO_CHAR(TO_DATE(col_19,'yyyy-mm-dd hh:mi:ss.ff3'),'yyyymmdd')
--             UNION ALL --<< Break point
--             SELECT  to AS phone
--                     ,TO_CHAR(TO_DATE(col_19,'yyyy-mm-dd hh:mi:ss.ff3'),'yyyymmdd') AS ds_zns
--             FROM    lazada_analyst_dev.tmp_loutruong_zns_upload_5 --<< TABLE 5 >>--
--             WHERE   1 = 1
--             AND     TOLOWER(status) IN ('delivered')
--             GROUP BY to
--                      ,TO_CHAR(TO_DATE(col_19,'yyyy-mm-dd hh:mi:ss.ff3'),'yyyymmdd')
--             UNION ALL --<< Break point
--             SELECT  phone
--                     ,CAST(date AS STRING) AS ds_zns
--             FROM    lazada_analyst_dev.tmp_loutruong_zns_upload_6 --<< TABLE 6 >>--
--             GROUP BY phone
--                      ,CAST(date AS STRING)
--             UNION ALL --<< Break point
--             SELECT  phone
--                     ,CAST(date AS STRING) AS ds_zns
--             FROM    lazada_analyst_dev.tmp_loutruong_zns_upload_7 --<< TABLE 7 >>--
--             GROUP BY phone
--                      ,CAST(date AS STRING)
--             UNION ALL --<< Break point
--             SELECT  phone
--                     ,CAST(date AS STRING) AS ds_zns
--             FROM    lazada_analyst_dev.tmp_loutruong_zns_upload_8 --<< TABLE 8 >>--
--             GROUP BY phone
--                      ,CAST(date AS STRING)
--             UNION ALL --<< Break point
--             SELECT  phone
--                     ,CAST(date AS STRING) AS ds_zns
--             FROM    lazada_analyst_dev.tmp_loutruong_zns_upload_9 --<< TABLE 9 >>--
--             GROUP BY phone
--                      ,CAST(date AS STRING)
--             UNION ALL --<< Break point
--             SELECT  phone
--                     ,CAST(date AS STRING) AS ds_zns
--             FROM    lazada_analyst_dev.tmp_loutruong_zns_upload_10 --<< TABLE 10 >>--
--             GROUP BY phone
--                      ,CAST(date AS STRING)
--             UNION ALL --<< Break point
--             SELECT  phone
--                     ,CAST(date AS STRING) AS ds_zns
--             FROM    lazada_analyst_dev.tmp_loutruong_zns_upload_11 --<< TABLE 11 >>--
--             GROUP BY phone
--                      ,CAST(date AS STRING)
--             UNION ALL --<< Break point
--             SELECT  phone
--                     ,CAST(date AS STRING) AS ds_zns
--             FROM    lazada_analyst_dev.tmp_loutruong_zns_upload_12 --<< TABLE 12 >>--
--             GROUP BY phone
--                      ,CAST(date AS STRING)
--             UNION ALL --<< Break point
--             SELECT  phone
--                     ,CAST(date AS STRING) AS ds_zns
--             FROM    lazada_analyst_dev.tmp_loutruong_zns_upload_13 --<< TABLE 13 >>--
--             GROUP BY phone
--                      ,CAST(date AS STRING)
--             UNION ALL --<< Break point
--             SELECT  phone
--                     ,CAST(date AS STRING) AS ds_zns
--             FROM    lazada_analyst_dev.tmp_loutruong_zns_upload_14 --<< TABLE 14 >>--
--             GROUP BY phone
--                      ,CAST(date AS STRING)
--             UNION ALL --<< Break point
--             SELECT  phone
--                     ,CAST(date AS STRING) AS ds_zns
--             FROM    lazada_analyst_dev.tmp_loutruong_zns_upload_15 --<< TABLE 15 >>--
--             GROUP BY phone
--                      ,CAST(date AS STRING)
--             UNION ALL --<< Break point
--             SELECT  phone
--                     ,CAST(date AS STRING) AS ds_zns
--             FROM    lazada_analyst_dev.tmp_loutruong_zns_upload_16 --<< TABLE 16 >>--
--             GROUP BY phone
--                      ,CAST(date AS STRING)
--             UNION ALL --<< Break point
--             SELECT  phone
--                     ,CAST(date AS STRING) AS ds_zns
--             FROM    lazada_analyst_dev.tmp_loutruong_zns_upload_17 --<< TABLE 17 >>--
--             GROUP BY phone
--                      ,CAST(date AS STRING)
--             UNION ALL --<< Break point
--             SELECT  phone
--                     ,CAST(date AS STRING) AS ds_zns
--             FROM    lazada_analyst_dev.tmp_loutruong_zns_upload_18 --<< TABLE 18 >>--
--             GROUP BY phone
--                      ,CAST(date AS STRING)
--             UNION ALL --<< Break point
--             SELECT  phone
--                     ,CAST(date AS STRING) AS ds_zns
--             FROM    lazada_analyst_dev.tmp_loutruong_zns_upload_19 --<< TABLE 19 >>--
--             GROUP BY phone
--                      ,CAST(date AS STRING)
--             UNION ALL --<< Break point
--             SELECT  phone
--                     ,CAST(date AS STRING) AS ds_zns
--             FROM    lazada_analyst_dev.tmp_loutruong_zns_upload_20 --<< TABLE 20 >>--
--             GROUP BY phone
--                      ,CAST(date AS STRING)
--             UNION ALL --<< Break point
--             SELECT  phone
--                     ,CAST(date AS STRING) AS ds_zns
--             FROM    lazada_analyst_dev.tmp_loutruong_zns_upload_21 --<< TABLE 21 >>--
--             GROUP BY phone
--                      ,CAST(date AS STRING)
--             UNION ALL --<< Break point
--             SELECT  phone
--                     ,CAST(date AS STRING) AS ds_zns
--             FROM    lazada_analyst_dev.tmp_loutruong_zns_upload_22 --<< TABLE 22 >>--
--             GROUP BY phone
--                      ,CAST(date AS STRING)
--             UNION ALL --<< Break point
--             SELECT  phone
--                     ,CAST(date AS STRING) AS ds_zns
--             FROM    lazada_analyst_dev.tmp_loutruong_zns_upload_23 --<< TABLE 23 >>--
--             GROUP BY phone
--                      ,CAST(date AS STRING)
--             UNION ALL --<< Break point
--             SELECT  phone
--                     ,CAST(date AS STRING) AS ds_zns
--             FROM    lazada_analyst_dev.tmp_loutruong_zns_upload_24 --<< TABLE 24 >>--
--             GROUP BY phone
--                      ,CAST(date AS STRING)
--             UNION ALL --<< Break point
--             SELECT  phone
--                     ,CAST(date AS STRING) AS ds_zns
--             FROM    lazada_analyst_dev.tmp_loutruong_zns_upload_25 --<< TABLE 25 >>--
--             GROUP BY phone
--                      ,CAST(date AS STRING)
--             UNION ALL --<< Break point
--             SELECT  phone
--                     ,CAST(date AS STRING) AS ds_zns
--             FROM    lazada_analyst_dev.tmp_loutruong_zns_upload_26 --<< TABLE 26 >>--
--             GROUP BY phone
--                      ,CAST(date AS STRING)
--             UNION ALL --<< Break point
--             SELECT  phone
--                     ,CAST(date AS STRING) AS ds_zns
--             FROM    lazada_analyst_dev.tmp_loutruong_zns_upload_27 --<< TABLE 27 >>--
--             GROUP BY phone
--                      ,CAST(date AS STRING)
--             UNION ALL --<< Break point
--             SELECT  phone
--                     ,CAST(date AS STRING) AS ds_zns
--             FROM    lazada_analyst_dev.tmp_loutruong_zns_upload_28 --<< TABLE 28 >>--
--             GROUP BY phone
--                      ,CAST(date AS STRING)
--             UNION ALL --<< Break point
--             SELECT  phone
--                     ,CAST(date AS STRING) AS ds_zns
--             FROM    lazada_analyst_dev.tmp_loutruong_zns_upload_29 --<< TABLE 29 >>--
--             GROUP BY phone
--                      ,CAST(date AS STRING)
--             UNION ALL --<< Break point
--             SELECT  phone
--                     ,CAST(date AS STRING) AS ds_zns
--             FROM    lazada_analyst_dev.tmp_loutruong_zns_upload_30 --<< TABLE 30 >>--
--             GROUP BY phone
--                      ,CAST(date AS STRING)
--             UNION ALL --<< Break point
--             SELECT  phone
--                     ,CAST(date AS STRING) AS ds_zns
--             FROM    lazada_analyst_dev.tmp_loutruong_zns_upload_31 --<< TABLE 31 >>--
--             GROUP BY phone
--                      ,CAST(date AS STRING)
--             UNION ALL --<< Break point
--             SELECT  phone
--                     ,CAST(date AS STRING) AS ds_zns
--             FROM    lazada_analyst_dev.tmp_loutruong_zns_upload_32 --<< TABLE 32 >>--
--             GROUP BY phone
--                      ,CAST(date AS STRING)
--             UNION ALL --<< Break point
--             SELECT  phone
--                     ,CAST(date AS STRING) AS ds_zns
--             FROM    lazada_analyst_dev.tmp_loutruong_zns_upload_33 --<< TABLE 33 >>--
--             GROUP BY phone
--                      ,CAST(date AS STRING)
--             UNION ALL --<< Break point
--             SELECT  phone
--                     ,CAST(date AS STRING) AS ds_zns
--             FROM    lazada_analyst_dev.tmp_loutruong_zns_upload_34 --<< TABLE 34 >>--
--             GROUP BY phone
--                      ,CAST(date AS STRING)
--             UNION ALL --<< Break point
--             SELECT  phone
--                     ,CAST(date AS STRING) AS ds_zns
--             FROM    lazada_analyst_dev.tmp_loutruong_zns_upload_35 --<< TABLE 35 >>--
--             GROUP BY phone
--                      ,CAST(date AS STRING)
--             UNION ALL --<< Break point
--             SELECT  phone
--                     ,CAST(date AS STRING) AS ds_zns
--             FROM    lazada_analyst_dev.tmp_loutruong_zns_upload_36 --<< TABLE 36 >>--
--             GROUP BY phone
--                      ,CAST(date AS STRING)
--             UNION ALL --<< Break point
--             SELECT  phone
--                     ,CAST(date AS STRING) AS ds_zns
--             FROM    lazada_analyst_dev.tmp_loutruong_zns_upload_37 --<< TABLE 37 >>--
--             GROUP BY phone
--                      ,CAST(date AS STRING)
--             UNION ALL --<< Break point
--             SELECT  phone
--                     ,CAST(date AS STRING) AS ds_zns
--             FROM    lazada_analyst_dev.tmp_loutruong_zns_upload_38 --<< TABLE 38 >>--
--             GROUP BY phone
--                      ,CAST(date AS STRING)
--             UNION ALL --<< Break point
--             SELECT  phone
--                     ,CAST(date AS STRING) AS ds_zns
--             FROM    lazada_analyst_dev.tmp_loutruong_zns_upload_39 --<< TABLE 39 >>--
--             GROUP BY phone
--                      ,CAST(date AS STRING)
--             UNION ALL --<< Break point
--             SELECT  phone
--                     ,CAST(date AS STRING) AS ds_zns
--             FROM    lazada_analyst_dev.tmp_loutruong_zns_upload_40 --<< TABLE 40 >>--
--             GROUP BY phone
--                      ,CAST(date AS STRING)
--             UNION ALL --<< Break point
--             SELECT  phone
--                     ,CAST(date AS STRING) AS ds_zns
--             FROM    lazada_analyst_dev.tmp_loutruong_zns_upload_41 --<< TABLE 41 >>--
--             GROUP BY phone
--                      ,CAST(date AS STRING)
--             UNION ALL --<< Break point
--             SELECT  phone
--                     ,CAST(date AS STRING) AS ds_zns
--             FROM    lazada_analyst_dev.tmp_loutruong_zns_upload_42 --<< TABLE 42 >>--
--             GROUP BY phone
--                      ,CAST(date AS STRING)
--             UNION ALL --<< Break point
--             SELECT  phone
--                     ,CAST(date AS STRING) AS ds_zns
--             FROM    lazada_analyst_dev.tmp_loutruong_zns_upload_43 --<< TABLE 43 >>--
--             GROUP BY phone
--                      ,CAST(date AS STRING)
--             UNION ALL --<< Break point
--             SELECT  phone
--                     ,CAST(date AS STRING) AS ds_zns
--             FROM    lazada_analyst_dev.tmp_loutruong_zns_upload_44 --<< TABLE 44 >>--
--             GROUP BY phone
--                      ,CAST(date AS STRING)
--             UNION ALL --<< Break point
--             SELECT  phone
--                     ,CAST(date AS STRING) AS ds_zns
--             FROM    lazada_analyst_dev.tmp_meimei_zns_upload_1 --<< TABLE 45 >>--
--             GROUP BY phone
--                      ,CAST(date AS STRING)
--             UNION ALL --<< Break point
--             SELECT  phone
--                     ,CAST(date AS STRING) AS ds_zns
--             FROM    lazada_analyst_dev.tmp_meimei_zns_upload_2 --<< TABLE 46 >>--
--             GROUP BY phone
--                      ,CAST(date AS STRING)
--             UNION ALL --<< Break point
--             SELECT  phone
--                     ,CAST(date AS STRING) AS ds_zns
--             FROM    lazada_analyst_dev.tmp_meimei_zns_upload_3 --<< TABLE 47 >>--
--             GROUP BY phone
--                      ,CAST(date AS STRING)
--             UNION ALL --<< Break point
--             SELECT  phone
--                     ,CAST(date AS STRING) AS ds_zns
--             FROM    lazada_analyst_dev.tmp_meimei_zns_upload_4 --<< TABLE 48 >>--
--             GROUP BY phone
--                      ,CAST(date AS STRING)
--             UNION ALL --<< Break point
--             SELECT  phone
--                     ,CAST(date AS STRING) AS ds_zns
--             FROM    lazada_analyst_dev.tmp_meimei_zns_upload_5 --<< TABLE 49 >>--
--             GROUP BY phone
--                      ,CAST(date AS STRING)
--             UNION ALL --<< Break point
--             SELECT  phone
--                     ,CAST(date AS STRING) AS ds_zns
--             FROM    lazada_analyst_dev.tmp_meimei_zns_upload_6 --<< TABLE 50 >>--
--             GROUP BY phone
--                      ,CAST(date AS STRING)
--             UNION ALL --<< Break point
--             SELECT  phone
--                     ,CAST(date AS STRING) AS ds_zns
--             FROM    lazada_analyst.tmp_meimei_zns_upload_7 --<< TABLE 51 >>--
--             GROUP BY phone
--                      ,CAST(date AS STRING)
--             UNION ALL --<< Break point
--             SELECT  phone
--                     ,CAST(date AS STRING) AS ds_zns
--             FROM    lazada_analyst_dev.tmp_meimei_zns_upload_8 --<< TABLE 52 >>--
--             GROUP BY phone
--                      ,CAST(date AS STRING)
--             UNION ALL --<< Break point
--             SELECT  phone
--                     ,CAST(date AS STRING) AS ds_zns
--             FROM    lazada_analyst_dev.tmp_meimei_zns_upload_9 --<< TABLE 53 >>--
--             GROUP BY phone
--                      ,CAST(date AS STRING)
--             UNION ALL --<< Break point
--             SELECT  phone
--                     ,CAST(date AS STRING) AS ds_zns
--             FROM    lazada_analyst_dev.tmp_meimei_zns_upload_10 --<< TABLE 54 >>--
--             GROUP BY phone
--                      ,CAST(date AS STRING)
--             UNION ALL --<< Break point
--             SELECT  phone
--                     ,CAST(date AS STRING) AS ds_zns
--             FROM    lazada_analyst_dev.tmp_meimei_zns_upload_11 --<< TABLE 55 >>--
--             GROUP BY phone
--                      ,CAST(date AS STRING)
--             UNION ALL --<< Break point
--             SELECT  phone
--                     ,CAST(date AS STRING) AS ds_zns
--             FROM    lazada_analyst_dev.tmp_meimei_zns_upload_12 --<< TABLE 56 >>--
--             GROUP BY phone
--                      ,CAST(date AS STRING)
--             UNION ALL --<< Break point
--             SELECT  phone
--                     ,CAST(date AS STRING) AS ds_zns
--             FROM    lazada_analyst_dev.tmp_meimei_zns_upload_13 --<< TABLE 57 >>--
--             GROUP BY phone
--                      ,CAST(date AS STRING)
--             UNION ALL --<< Break point
--             SELECT  phone
--                     ,CAST(date AS STRING) AS ds_zns
--             FROM    lazada_analyst_dev.tmp_meimei_zns_upload_14 --<< TABLE 58 >>--
--             GROUP BY phone
--                      ,CAST(date AS STRING)
--             UNION ALL --<< Break point
--             SELECT  phone
--                     ,CAST(date AS STRING) AS ds_zns
--             FROM    lazada_analyst_dev.tmp_meimei_zns_upload_15 --<< TABLE 59 >>--
--             GROUP BY phone
--                      ,CAST(date AS STRING)
--             UNION ALL --<< Break point
--             SELECT  phone
--                     ,CAST(date AS STRING) AS ds_zns
--             FROM    lazada_analyst_dev.tmp_meimei_zns_upload_16 --<< TABLE 60 >>--
--             GROUP BY phone
--                      ,CAST(date AS STRING)
--             UNION ALL --<< Break point
--             SELECT  phone
--                     ,CAST(date AS STRING) AS ds_zns
--             FROM    lazada_analyst_dev.tmp_meimei_zns_upload_17 --<< TABLE 61 >>--
--             GROUP BY phone
--                      ,CAST(date AS STRING)
--             UNION ALL --<< Break point
--             SELECT  phone
--                     ,CAST(date AS STRING) AS ds_zns
--             FROM    lazada_analyst_dev.tmp_meimei_zns_upload_18 --<< TABLE 62 >>--
--             GROUP BY phone
--                      ,CAST(date AS STRING)
--             UNION ALL --<< Break point
--             SELECT  phone
--                     ,CAST(date AS STRING) AS ds_zns
--             FROM    lazada_analyst_dev.tmp_meimei_zns_upload_19 --<< TABLE 63 >>--
--             GROUP BY phone
--                      ,CAST(date AS STRING)
--             UNION ALL --<< Break point
--             SELECT  phone
--                     ,CAST(date AS STRING) AS ds_zns
--             FROM    lazada_analyst_dev.tmp_meimei_zns_upload_20 --<< TABLE 64 >>--
--             GROUP BY phone
--                      ,CAST(date AS STRING)
--             UNION ALL --<< Break point
--             SELECT  phone
--                     ,CAST(date AS STRING) AS ds_zns
--             FROM    lazada_analyst_dev.tmp_meimei_zns_upload_21 --<< TABLE 65 >>--
--             GROUP BY phone
--                      ,CAST(date AS STRING)
--             UNION ALL --<< Break point
--             SELECT  phone
--                     ,CAST(date AS STRING) AS ds_zns
--             FROM    lazada_analyst_dev.tmp_meimei_zns_upload_22 --<< TABLE 66 >>--
--             GROUP BY phone
--                      ,CAST(date AS STRING)
--             UNION ALL --<< Break point
--             SELECT  phone
--                     ,CAST(date AS STRING) AS ds_zns
--             FROM    lazada_analyst_dev.tmp_meimei_zns_upload_23 --<< TABLE 67 >>--
--             GROUP BY phone
--                      ,CAST(date AS STRING)
--             UNION ALL --<< Break point
--             SELECT  phone
--                     ,CAST(date AS STRING) AS ds_zns
--             FROM    lazada_analyst_dev.tmp_meimei_zns_upload_24 --<< TABLE 68 >>--
--             GROUP BY phone
--                      ,CAST(date AS STRING)
--             UNION ALL --<< Break point
--             SELECT  phone
--                     ,CAST(date AS STRING) AS ds_zns
--             FROM    lazada_analyst_dev.tmp_meimei_zns_upload_25 --<< TABLE 69 >>--
--             GROUP BY phone
--                      ,CAST(date AS STRING)
--             UNION ALL --<< Break point
--             SELECT  phone
--                     ,CAST(date AS STRING) AS ds_zns
--             FROM    lazada_analyst_dev.tmp_meimei_zns_upload_26 --<< TABLE 70 >>--
--             GROUP BY phone
--                      ,CAST(date AS STRING)
--             UNION ALL --<< Break point
--             SELECT  phone
--                     ,CAST(date AS STRING) AS ds_zns
--             FROM    lazada_analyst_dev.tmp_meimei_zns_upload_27 --<< TABLE 71 >>--
--             GROUP BY phone
--                      ,CAST(date AS STRING)
--             UNION ALL --<< Break point
--             SELECT  phone
--                     ,CAST(date AS STRING) AS ds_zns
--             FROM    lazada_analyst_dev.tmp_meimei_zns_upload_28 --<< TABLE 72 >>--
--             GROUP BY phone
--                      ,CAST(date AS STRING)
--             UNION ALL --<< Break point
--             SELECT  phone
--                     ,CAST(date AS STRING) AS ds_zns
--             FROM    lazada_analyst_dev.tmp_meimei_zns_upload_29 --<< TABLE 73 >>--
--             GROUP BY phone
--                      ,CAST(date AS STRING)
--             UNION ALL --<< Break point
--             SELECT  phone
--                     ,CAST(date AS STRING) AS ds_zns
--             FROM    lazada_analyst_dev.tmp_meimei_zns_upload_30 --<< TABLE 74 >>--
--             GROUP BY phone
--                      ,CAST(date AS STRING)
--             UNION ALL --<< Break point
--             SELECT  phone
--                     ,CAST(date AS STRING) AS ds_zns
--             FROM    lazada_analyst_dev.tmp_meimei_zns_upload_31 --<< TABLE 75 >>--
--             GROUP BY phone
--                      ,CAST(date AS STRING)
--             UNION ALL --<< Break point
--             SELECT  phone
--                     ,CAST(date AS STRING) AS ds_zns
--             FROM    lazada_analyst_dev.tmp_meimei_zns_upload_32 --<< TABLE 76 >>--
--             GROUP BY phone
--                      ,CAST(date AS STRING)
--             UNION ALL --<< Break point
--             SELECT  phone
--                     ,CAST(date AS STRING) AS ds_zns
--             FROM    lazada_analyst_dev.tmp_meimei_zns_upload_33 --<< TABLE 77 >>--
--             GROUP BY phone
--                      ,CAST(date AS STRING)
--             UNION ALL --<< Break point
--             SELECT  phone
--                     ,CAST(date AS STRING) AS ds_zns
--             FROM    lazada_analyst_dev.tmp_meimei_zns_upload_34 --<< TABLE 78 >>--
--             GROUP BY phone
--                      ,CAST(date AS STRING)
--             UNION ALL --<< Break point
--             SELECT  phone
--                     ,CAST(date AS STRING) AS ds_zns
--             FROM    lazada_analyst_dev.tmp_meimei_zns_upload_35 --<< TABLE 79 >>--
--             GROUP BY phone
--                      ,CAST(date AS STRING)
--             UNION ALL
--             SELECT  phone
--                     ,ds_zns
--             FROM    lazada_analyst_dev.tmp_loutruong_zns_receive_df --<< TABLE TMP >>--
--             GROUP BY phone
--                      ,ds_zns
--         ) 
-- GROUP BY phone
--          ,ds_zns
-- ;
-- ********************************************************************-- manually_upload_date = ... -- ********************************************************************--
INSERT OVERWRITE TABLE lazada_analyst_dev.tmp_loutruong_zns_receive_df PARTITION (ds = '${bizdate}')
SELECT phone,
    ds_zns
FROM (
        SELECT phone,
            ds_zns
        FROM lazada_analyst_dev.tmp_loutruong_zns_receive_df --<< TABLE TMP >>--
        GROUP BY phone,
            ds_zns
    )
GROUP BY phone,
    ds_zns;
-- ********************************************************************--
-- CREATE MEMBER ID POOLS
-- ********************************************************************--
--@@ Input = lazada_ads_data.ads_lzd_cps_partner_info_df
--@@ Input = lazada_ads_data.dws_lzd_cps_app_log_mem_1d
--@@ Input = lazada_cdm.dim_lzd_usr
--@@ Input = lazada_cdm.dwd_lzd_trd_core_df
--@@ Output = lazada_analyst_dev.tmp_loutruong_aff_prt_utdid_uid_df
--@@ Output = lazada_analyst_dev.loutruong_aff_prt_utdid_uid_df
-- DROP TABLE IF EXISTS lazada_analyst_dev.tmp_loutruong_aff_prt_utdid_uid_df
-- ;
-- CREATE TABLE IF NOT EXISTS lazada_analyst_dev.tmp_loutruong_aff_prt_utdid_uid_df
-- LIFECYCLE 5 AS
SELECT DISTINCT t1.member_id AS member_id,
    COALESCE(t1.phone_number, t3.phone_number, t4.phone_number) AS member_phone_number,
    COALESCE(t2.utdid, t3.utdid, t4.utdid) AS member_utdid,
    COALESCE(t2.buyer_id, t3.buyer_id, t4.buyer_id) AS member_uid
FROM (
        SELECT DISTINCT REPLACE(
                REPLACE(REPLACE(phone_number, ' ', ''), '+', ''),
                '-',
                ''
            ) AS phone_number,
            member_id
        FROM lazada_ads_data.ads_lzd_cps_partner_info_df
        WHERE 1 = 1
            AND venture = 'VN'
            AND phone_number IS NOT NULL
    ) AS t1
    LEFT JOIN (
        SELECT member_id,
            utdid,
            user_id AS buyer_id
        FROM lazada_ads_data.dws_lzd_cps_app_log_mem_1d
        WHERE 1 = 1
            AND venture = 'VN'
            AND member_id IS NOT NULL
        GROUP BY member_id,
            utdid,
            user_id
    ) AS t2 ON t1.member_id = t2.member_id
    LEFT JOIN (
        SELECT utdid,
            buyer_id,
            phone_number
        FROM (
                SELECT usertrack_id AS utdid,
                    buyer_id,
                    CONCAT('84', phone_number) AS phone_number
                FROM lazada_cdm.dwd_lzd_trd_core_df
                WHERE 1 = 1
                    AND ds = MAX_PT('lazada_cdm.dwd_lzd_trd_core_df')
                    AND venture = 'VN'
                UNION ALL
                SELECT utdid,
                    CAST(user_id AS BIGINT) AS buyer_id,
                    CONCAT('84', phone) AS phone_number
                FROM lazada_cdm.dim_lzd_usr
                WHERE 1 = 1
                    AND ds = MAX_PT('lazada_cdm.dim_lzd_usr')
                    AND venture = 'VN'
            )
        GROUP BY utdid,
            buyer_id,
            phone_number
    ) AS t3 ON t1.phone_number = t3.phone_number
    LEFT JOIN (
        SELECT utdid,
            buyer_id,
            phone_number
        FROM (
                SELECT usertrack_id AS utdid,
                    buyer_id,
                    phone_number
                FROM lazada_cdm.dwd_lzd_trd_core_df
                WHERE 1 = 1
                    AND ds = MAX_PT('lazada_cdm.dwd_lzd_trd_core_df')
                    AND venture = 'VN'
                UNION ALL
                SELECT utdid,
                    CAST(user_id AS BIGINT) AS buyer_id,
                    phone AS phone_number
                FROM lazada_cdm.dim_lzd_usr
                WHERE 1 = 1
                    AND ds = MAX_PT('lazada_cdm.dim_lzd_usr')
                    AND venture = 'VN'
            )
        GROUP BY utdid,
            buyer_id,
            phone_number
    ) AS t4 ON t1.phone_number = t4.phone_number;
-- DROP TABLE IF EXISTS lazada_analyst_dev.loutruong_aff_prt_utdid_uid_df
-- ;
-- CREATE TABLE IF NOT EXISTS lazada_analyst_dev.loutruong_aff_prt_utdid_uid_df
-- (
--     member_id            STRING
--     ,member_phone_number STRING
--     ,member_utdid        STRING
--     ,member_uid          STRING
-- )
-- PARTITIONED BY 
-- (
--     ds                   STRING COMMENT 'execute date data snapshot'
-- )
-- LIFECYCLE 3600
-- ;
INSERT OVERWRITE TABLE lazada_analyst_dev.loutruong_aff_prt_utdid_uid_df PARTITION (ds = '${bizdate}')
SELECT member_id,
    member_phone_number,
    member_utdid,
    member_uid
FROM (
        SELECT member_id,
            member_phone_number,
            member_utdid,
            member_uid
        FROM lazada_analyst_dev.tmp_loutruong_aff_prt_utdid_uid_df
        UNION ALL
        SELECT CAST(member_id AS BIGINT) AS member_id,
            member_phone_number,
            member_utdid,
            member_uid
        FROM lazada_analyst_dev.loutruong_aff_prt_utdid_uid_df
    )
GROUP BY member_id,
    member_phone_number,
    member_utdid,
    member_uid;
-- ********************************************************************--
-- CREATE UID & UTDID RECEIVED ZNS MESSAGE
-- ********************************************************************--
--@@ Input = lazada_analyst_dev.tmp_loutruong_zns_receive_df
--@@ Input = lazada_cdm.dim_lzd_usr
--@@ Input = lazada_cdm.dwd_lzd_trd_core_df
--@@ Input = lazada_analyst_dev.loutruong_aff_prt_utdid_uid_df
--@@ Ouput = lazada_analyst_dev.loutruong_zns_receive_df
-- DROP TABLE IF EXISTS lazada_analyst_dev.loutruong_zns_receive_df
-- ;
-- CREATE TABLE IF NOT EXISTS lazada_analyst_dev.loutruong_zns_receive_df
-- (
--     utdid        STRING COMMENT 'device_id'
--     ,buyer_id    STRING COMMENT 'internal_system_lazada_id'
--     ,buyer_phone STRING COMMENT 'phone number push on zns system'
--     ,ds_zns      STRING COMMENT 'date push/flash/shot zalo'
-- )
-- PARTITIONED BY 
-- (
--     ds           STRING COMMENT 'execute date data snapshot'
-- )
-- LIFECYCLE 3600
-- ;
INSERT OVERWRITE TABLE lazada_analyst_dev.loutruong_zns_receive_df PARTITION (ds = '${bizdate}')
SELECT COALESCE(t2.utdid, t3.utdid) AS utdid,
    COALESCE(t2.buyer_id, t3.buyer_id) AS buyer_id,
    t1.phone AS buyer_phone,
    t1.ds_zns AS ds_zns
FROM (
        SELECT phone,
            ds_zns
        FROM lazada_analyst_dev.tmp_loutruong_zns_receive_df
        WHERE 1 = 1
            AND ds = MAX_PT(
                'lazada_analyst_dev.tmp_loutruong_zns_receive_df'
            )
        GROUP BY phone,
            ds_zns
    ) AS t1
    LEFT JOIN (
        SELECT utdid,
            buyer_id,
            phone_number
        FROM (
                SELECT usertrack_id AS utdid,
                    buyer_id,
                    CONCAT('84', phone_number) AS phone_number
                FROM lazada_cdm.dwd_lzd_trd_core_df
                WHERE 1 = 1
                    AND ds = MAX_PT('lazada_cdm.dwd_lzd_trd_core_df')
                    AND venture = 'VN'
                UNION ALL
                SELECT utdid,
                    CAST(user_id AS BIGINT) AS buyer_id,
                    CONCAT('84', phone) AS phone_number
                FROM lazada_cdm.dim_lzd_usr
                WHERE 1 = 1
                    AND ds = MAX_PT('lazada_cdm.dim_lzd_usr')
                    AND venture = 'VN'
                UNION ALL
                SELECT member_utdid AS utdid,
                    CAST(member_uid AS BIGINT) AS buyer_id,
                    CONCAT('84', member_phone_number) AS phone_number
                FROM lazada_analyst_dev.loutruong_aff_prt_utdid_uid_df
                WHERE 1 = 1
                    AND ds = MAX_PT(
                        'lazada_analyst_dev.loutruong_aff_prt_utdid_uid_df'
                    )
            )
        GROUP BY utdid,
            buyer_id,
            phone_number
    ) AS t2 ON t1.phone = t2.phone_number
    LEFT JOIN (
        SELECT utdid,
            buyer_id,
            phone_number
        FROM (
                SELECT usertrack_id AS utdid,
                    buyer_id,
                    phone_number
                FROM lazada_cdm.dwd_lzd_trd_core_df
                WHERE 1 = 1
                    AND ds = MAX_PT('lazada_cdm.dwd_lzd_trd_core_df')
                    AND venture = 'VN'
                UNION ALL
                SELECT utdid,
                    CAST(user_id AS BIGINT) AS buyer_id,
                    phone AS phone_number
                FROM lazada_cdm.dim_lzd_usr
                WHERE 1 = 1
                    AND ds = MAX_PT('lazada_cdm.dim_lzd_usr')
                    AND venture = 'VN'
                UNION ALL
                SELECT member_utdid AS utdid,
                    CAST(member_uid AS BIGINT) AS buyer_id,
                    member_phone_number AS phone_number
                FROM lazada_analyst_dev.loutruong_aff_prt_utdid_uid_df
                WHERE 1 = 1
                    AND ds = MAX_PT(
                        'lazada_analyst_dev.loutruong_aff_prt_utdid_uid_df'
                    )
            )
        GROUP BY utdid,
            buyer_id,
            phone_number
    ) AS t3 ON t1.phone = t3.phone_number
GROUP BY COALESCE(t2.utdid, t3.utdid),
    COALESCE(t2.buyer_id, t3.buyer_id),
    t1.phone,
    t1.ds_zns;
-- ********************************************************************--
-- CREATE UTDID CLICK TO ZNS MESSAGE
-- ********************************************************************--
--@@ Input = lazada_cdm.dwd_lzd_clickserver_log_di
--@@ Input = lazada_ads_data.ads_lzd_cps_partner_info_df
-- DROP TABLE IF EXISTS lazada_analyst_dev.tmp_loutruong_click
-- ;
-- CREATE TABLE IF NOT EXISTS lazada_analyst_dev.tmp_loutruong_click
-- (
--     utdid     STRING COMMENT 'device_id'
--     ,buyer_id STRING COMMENT 'internal_system_lazada_id'
-- )
-- PARTITIONED BY 
-- (
--     ds        STRING
-- )
-- LIFECYCLE 3600
-- ;
INSERT OVERWRITE TABLE lazada_analyst_dev.tmp_loutruong_click PARTITION (ds)
SELECT utdid,
    buyer_id,
    ds
FROM (
        SELECT ds,
            member_id,
            utdid,
            buyer_id,
            MAX(is_app) AS is_app
        FROM (
                SELECT ds,
                    COALESCE(SPLIT(pid, '_') [1], '') AS member_id,
                    utdid,
                    buyer_id,
                    clickid,
                    IF(LENGTH(utdid) > 0, 1, 0) AS is_app
                FROM lazada_cdm.dwd_lzd_clickserver_log_di
                WHERE 1 = 1 --
                    -- AND     ds BETWEEN '${start_date}' AND '${end_date}' --<< Start
                    AND ds >= TO_CHAR(
                        DATEADD(TO_DATE('${bizdate}', 'yyyymmdd'), -3, 'dd'),
                        'yyyymmdd'
                    ) --<< Daily
                    AND venture = 'VN'
                    AND bm = 2
                    AND old_clickid IS NULL
                UNION ALL
                SELECT ds,
                    COALESCE(SPLIT(pid, '_') [1], '') AS member_id,
                    utdid,
                    buyer_id,
                    old_clickid AS clickid,
                    1 AS is_app
                FROM lazada_cdm.dwd_lzd_clickserver_log_di
                WHERE 1 = 1 --
                    -- AND     ds BETWEEN '${start_date}' AND '${end_date}' --<< Start
                    AND ds >= TO_CHAR(
                        DATEADD(TO_DATE('${bizdate}', 'yyyymmdd'), -3, 'dd'),
                        'yyyymmdd'
                    ) --<< Daily
                    AND venture = 'VN'
                    AND bm = 2
                    AND old_clickid IS NOT NULL
            )
        GROUP BY ds,
            member_id,
            utdid,
            buyer_id
    )
WHERE 1 = 1
    AND member_id IN (
        SELECT member_id
        FROM lazada_ads_data.ads_lzd_cps_partner_info_df
        WHERE 1 = 1
            AND ds = MAX_PT('lazada_ads_data.ads_lzd_cps_partner_info_df')
            AND venture = 'VN'
            AND TOLOWER(COALESCE(cluster, 'b2b')) LIKE '%b2b%'
            AND TOLOWER(segment) IN ('default')
            AND TOLOWER(member_name) LIKE 'zns%'
        GROUP BY member_id
    )
GROUP BY utdid,
    buyer_id,
    ds;
-- ********************************************************************--
-- CREATE ZNS MASTER POOLS
-- ********************************************************************--
--@@ Input = lazada_cdm.dwd_lzd_trd_core_df
--@@ Input = lazada_mobile.s_ripple_session_all
--@@ Input = lazada_analyst_dev.loutruong_zns_receive_df
--@@ Output = lazada_analyst_dev.tmp_loutruong_click
--@@ Output = lazada_analyst_dev.loutruong_zns_master_pool
-- DROP TABLE IF EXISTS lazada_analyst_dev.loutruong_zns_master_pool
-- ;
-- CREATE TABLE IF NOT EXISTS lazada_analyst_dev.loutruong_zns_master_pool
-- (
--     is_buyer_zns   BIGINT
--     ,buyer_id      STRING
--     ,customer_name STRING
--     ,mobile        STRING
-- )
-- PARTITIONED BY 
-- (
--     ds             STRING
-- )
-- LIFECYCLE 3600
-- ;
-- INSERT OVERWRITE TABLE lazada_analyst_dev.loutruong_zns_master_pool PARTITION (ds = '${bizdate}')
SELECT t1.is_buyer_zns AS is_buyer_zns,
    t1.buyer_id AS buyer_id,
CASE
        WHEN t1.billing_customer_name IS NULL THEN 'Bạn'
        WHEN t1.billing_customer_name IN ('') THEN 'Bạn'
        WHEN LENGTH(t1.billing_customer_name) > 30 THEN 'Bạn'
        ELSE t1.billing_customer_name
    END AS customer_name,
    t1.phone_number AS mobile
FROM (
        SELECT t1.buyer_id AS buyer_id,
            t1.phone_number AS phone_number,
            INITCAP(TOLOWER(t1.billing_customer_name)) AS billing_customer_name,
CASE
                WHEN t3.buyer_id IS NOT NULL THEN 1
                ELSE 0
            END AS is_buyer_zns
        FROM (
                SELECT buyer_id,
                    billing_customer_name,
                    phone_number
                FROM lazada_cdm.dwd_lzd_trd_core_df
                WHERE 1 = 1
                    AND ds = MAX_PT('lazada_cdm.dwd_lzd_trd_core_df')
                    AND venture = 'VN'
                    AND is_revenue = 1
                    AND is_fulfilled = 1
                GROUP BY buyer_id,
                    billing_customer_name,
                    phone_number
            ) AS t1 LEFT ANTI
            JOIN (
                SELECT t1.buyer_id AS buyer_id,
                    t1.utdid AS utdid
                FROM (
                        SELECT utdid,
                            buyer_id
                        FROM lazada_analyst_dev.loutruong_zns_receive_df
                        WHERE 1 = 1
                            AND ds = MAX_PT('lazada_analyst_dev.loutruong_zns_receive_df')
                        GROUP BY utdid,
                            buyer_id
                    ) AS t1 LEFT ANTI
                    JOIN (
                        SELECT utdid
                        FROM lazada_analyst_dev.tmp_loutruong_click
                        WHERE 1 = 1
                            AND ds >= TO_CHAR(
                                DATEADD(TO_DATE(ds, 'yyyymmdd'), -365, 'dd'),
                                'yyyymmdd'
                            )
                        GROUP BY utdid
                    ) AS t2 ON t1.utdid = t2.utdid LEFT ANTI
                    JOIN (
                        SELECT buyer_id
                        FROM lazada_analyst_dev.tmp_loutruong_click
                        WHERE 1 = 1
                            AND ds >= TO_CHAR(
                                DATEADD(TO_DATE(ds, 'yyyymmdd'), -365, 'dd'),
                                'yyyymmdd'
                            )
                        GROUP BY buyer_id
                    ) AS t3 ON t1.buyer_id = t3.buyer_id
                GROUP BY t1.buyer_id,
                    t1.utdid
            ) AS t2 ON t1.buyer_id = t2.buyer_id --<< Remove user received ZNS no click l365d
            LEFT JOIN (
                SELECT buyer_id
                FROM lazada_cdm.dwd_lzd_trd_core_df
                WHERE 1 = 1
                    AND ds = MAX_PT('lazada_cdm.dwd_lzd_trd_core_df')
                    AND venture = 'VN'
                    AND is_revenue = 1
                    AND is_fulfilled = 1
                    AND usertrack_id IN (
                        SELECT utdid
                        FROM lazada_analyst_dev.tmp_loutruong_click
                        WHERE 1 = 1
                            AND ds >= TO_CHAR(
                                DATEADD(TO_DATE('${bizdate}', 'yyyymmdd'), -365, 'dd'),
                                'yyyymmdd'
                            )
                        GROUP BY utdid
                    )
                GROUP BY buyer_id
            ) AS t3 ON t1.buyer_id = t3.buyer_id --<< Buyer has click from ZNS l365d
        GROUP BY t1.buyer_id,
            t1.phone_number,
            INITCAP(TOLOWER(t1.billing_customer_name)),
CASE
                WHEN t3.buyer_id IS NOT NULL THEN 1
                ELSE 0
            END
    ) AS t1 LEFT ANTI
    JOIN (
        SELECT user_id AS buyer_id
        FROM lazada_mobile.s_ripple_session_all
        WHERE 1 = 1
            AND ds = MAX_PT('lazada_mobile.s_ripple_session_all')
            AND venture = 'VN'
            AND setting_type = 1008
            AND setting_value = 0
        GROUP BY user_id
    ) AS t3 ON t1.buyer_id = t3.buyer_id
WHERE 1 = 1
    AND LENGTH(t1.phone_number) BETWEEN 9 AND 12
GROUP BY t1.is_buyer_zns,
    t1.buyer_id,
CASE
        WHEN t1.billing_customer_name IS NULL THEN 'Bạn'
        WHEN t1.billing_customer_name IN ('') THEN 'Bạn'
        WHEN LENGTH(t1.billing_customer_name) > 30 THEN 'Bạn'
        ELSE t1.billing_customer_name
    END,
    t1.phone_number;
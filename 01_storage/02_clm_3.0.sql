-- MaxCompute SQL 
-- ********************************************************************--
-- author:vanthanh.truong
-- create time:2023-11-06 22:48:45
-- ********************************************************************--
--@@ Input = lazada_cdm.dws_lzd_usr_utdid_clm_glevel_1d
--@@ Input = lazada_cdm.dws_lzd_usr_uid_clm_glevel_1d
--@@ Input = lazada_datamining.ads_lzd_asset_usr_uid_monetary_d
--@@ Output = lazada_analyst_dev.loutruong_g_seg_utdid_1d
--@@ Output = lazada_analyst_dev.loutruong_g_seg_uid_1d
--@@ Output = lazada_analyst_dev.loutruong_m_tag_uid_1d
----------------------------------------------------------------------------------------------------------------------------
-- lazada_analyst_dev.loutruong_g_seg_utdid_1d
----------------------------------------------------------------------------------------------------------------------------
-- DROP TABLE IF EXISTS lazada_analyst_dev.loutruong_g_seg_utdid_1d
-- ;
-- CREATE TABLE IF NOT EXISTS lazada_analyst_dev.loutruong_g_seg_utdid_1d
-- (
--     utdid                         STRING COMMENT 'user_id / buyer_id'
--     ,last_used_user_id            STRING
--     ,lifetime_check_out_order_cnt BIGINT COMMENT 'LTO'
--     ,today_check_out_order        BIGINT COMMENT 'Checkout Order count in bizdate'
--     ,g_level                      STRING COMMENT 'G Level in CLM3.0'
-- )
-- COMMENT 'VN only lazada_cdm.dws_lzd_usr_utdid_clm_glevel_1d'
-- PARTITIONED BY 
-- (
--     ds                            STRING
-- )
-- LIFECYCLE 3600
-- ;
-- INSERT OVERWRITE TABLE lazada_analyst_dev.loutruong_g_seg_utdid_1d PARTITION (ds)
-- SELECT  utdid
--         ,last_used_user_id
--         ,lifetime_check_out_order_cnt
--         ,today_check_out_order
--         ,g_level
--         ,ds
-- FROM    lazada_cdm.dws_lzd_usr_utdid_clm_glevel_1d
-- WHERE   1 = 1 --
-- -- AND     ds >= '20220701' --<< Start
-- AND     ds >= TO_CHAR(DATEADD(TO_DATE('${bizdate}','yyyymmdd'),-2,'dd'),'yyyymmdd') --<< Daily
-- AND     venture = 'VN'
-- ;
----------------------------------------------------------------------------------------------------------------------------
-- lazada_analyst_dev.loutruong_g_seg_uid_1d
----------------------------------------------------------------------------------------------------------------------------
-- DROP TABLE IF EXISTS lazada_analyst_dev.loutruong_g_seg_uid_1d
-- ;
-- CREATE TABLE IF NOT EXISTS lazada_analyst_dev.loutruong_g_seg_uid_1d
-- (
--     user_id                        STRING COMMENT 'user_id / buyer_id'
--     ,lifetime_check_out_order_cnt  BIGINT COMMENT 'LTO'
--     ,today_check_out_order         BIGINT COMMENT 'Checkout Order count in bizdate'
--     ,check_out_order_l30d          BIGINT COMMENT 'Recent checkout Orders in the past 30 days'
--     ,check_out_order_l90d          BIGINT COMMENT 'Recent checkout Orders in the past 90 days'
--     ,check_out_order_l180d         BIGINT COMMENT 'Recent checkout Orders in the past 180 days'
--     ,first_fulfillment_create_date STRING COMMENT 'Frist order date (excluding bizdate)'
--     ,first_install_date            STRING COMMENT 'Frist install date'
--     ,g_level                       STRING COMMENT 'G Level in CLM3.0'
-- )
-- COMMENT 'VN only lazada_cdm.dws_lzd_usr_uid_clm_glevel_1d'
-- PARTITIONED BY 
-- (
--     ds                             STRING
-- )
-- LIFECYCLE 3600
-- ;
INSERT OVERWRITE TABLE lazada_analyst_dev.loutruong_g_seg_uid_1d PARTITION (ds)
SELECT user_id,
    lifetime_check_out_order_cnt,
    today_check_out_order,
    check_out_order_l30d,
    check_out_order_l90d,
    check_out_order_l180d,
    first_fulfillment_create_date,
    first_install_date,
    g_level,
    ds
FROM lazada_cdm.dws_lzd_usr_uid_clm_glevel_1d
WHERE 1 = 1 --
    -- AND     ds >= '20220701' --<< Start
    AND ds >= TO_CHAR(
        DATEADD(TO_DATE('${bizdate}', 'yyyymmdd'), -2, 'dd'),
        'yyyymmdd'
    ) --<< Daily
    AND venture = 'VN';
----------------------------------------------------------------------------------------------------------------------------
-- lazada_analyst_dev.loutruong_m_tag_uid_1d
----------------------------------------------------------------------------------------------------------------------------
-- DROP TABLE IF EXISTS lazada_analyst_dev.loutruong_m_tag_uid_1d
-- ;
-- CREATE TABLE IF NOT EXISTS lazada_analyst_dev.loutruong_m_tag_uid_1d
-- (
--     user_id       STRING COMMENT 'user_id / buyer_id'
--     ,g_level      STRING COMMENT 'G Level in CLM3.0'
--     ,aov_tier     STRING COMMENT 'AOV Tier'
--     ,pnl_tier     STRING COMMENT 'P&L Tier'
--     ,monetary_tag STRING COMMENT 'M Tag'
--     ,pc275        DOUBLE COMMENT 'User profitable tag'
-- )
-- COMMENT 'VN only lazada_datamining.ads_lzd_asset_usr_uid_monetary_d'
-- PARTITIONED BY 
-- (
--     ds            STRING
-- )
-- LIFECYCLE 3600
-- ;
INSERT OVERWRITE TABLE lazada_analyst_dev.loutruong_m_tag_uid_1d PARTITION (ds)
SELECT user_id,
    g_level,
    aov_tier,
    pnl_tier,
    monetary_tag,
    pc275,
    ds
FROM lazada_datamining.ads_lzd_asset_usr_uid_monetary_d
WHERE 1 = 1 --
    -- AND     ds >= '20220701' --<< Start
    AND ds >= TO_CHAR(
        DATEADD(TO_DATE('${bizdate}', 'yyyymmdd'), -2, 'dd'),
        'yyyymmdd'
    ) --<< Daily
    AND venture = 'VN';
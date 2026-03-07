-- MaxCompute SQL 
-- ********************************************************************--
-- author:Truong, Van Thanh
-- create time:2024-05-10 11:57:35
-- ********************************************************************--
--@@ Input = lazada_cdm.dws_lzd_usr_segment_usr_utdid_1d
--@@ Input = lazada_ads.ads_lzd_usr_uid_clm_d
--@@ Output = lazada_analyst_dev.loutruong_d_seg_utdid_1d
--@@ Output = lazada_analyst_dev.loutruong_d_seg_uid_1d
----------------------------------------------------------------------------------------------------------------------------
-- lazada_analyst_dev.loutruong_d_seg_utdid_1d
----------------------------------------------------------------------------------------------------------------------------
-- DROP TABLE IF EXISTS lazada_analyst_dev.loutruong_d_seg_utdid_1d
-- ;
-- CREATE TABLE IF NOT EXISTS lazada_analyst_dev.loutruong_d_seg_utdid_1d
-- (
--     utdid                           STRING
--     ,visit_frequency_level          STRING
--     ,purchase_frequency_level       STRING
--     ,purchase_order_frequency_level STRING
--     ,clm_level                      STRING COMMENT 'D Level in CLM2.0'
-- )
-- COMMENT 'VN only lazada_ads.ads_lzd_usr_uid_clm_d'
-- PARTITIONED BY 
-- (
--     ds                              STRING
-- )
-- LIFECYCLE 3600
-- ;
INSERT OVERWRITE TABLE lazada_analyst_dev.loutruong_d_seg_utdid_1d PARTITION (ds)
SELECT t1.utdid AS utdid,
CASE
        WHEN t2.visit_frequency_level IS NOT NULL THEN t2.visit_frequency_level
        ELSE 'Guest'
    END AS visit_frequency_level,
CASE
        WHEN t2.purchase_frequency_level IS NOT NULL THEN t2.purchase_frequency_level
        ELSE 'Guest'
    END AS purchase_frequency_level,
CASE
        WHEN t2.purchase_order_frequency_level IS NOT NULL THEN t2.purchase_order_frequency_level
        ELSE 'Guest'
    END AS purchase_order_frequency_level,
CASE
        WHEN t2.clm_level IS NOT NULL THEN t2.clm_level
        ELSE 'Guest'
    END AS d_level,
    t1.ds AS ds
FROM (
        SELECT ds,
            visitor_id,
            utdid
        FROM lazada_cdm.dws_lzd_usr_segment_usr_utdid_1d
        WHERE 1 = 1 --
            -- AND     ds >= 20220101 --< Start
            AND ds >= TO_CHAR(
                DATEADD(TO_DATE('${bizdate}', 'yyyymmdd'), -14, 'dd'),
                'yyyymmdd'
            ) --<< Daily
            AND venture = 'VN'
        GROUP BY ds,
            visitor_id,
            utdid
    ) AS t1
    LEFT JOIN (
        SELECT ds,
            user_id,
            MAX(visit_frequency_level) AS visit_frequency_level,
            MAX(purchase_frequency_level) AS purchase_frequency_level,
            MAX(purchase_order_frequency_level) AS purchase_order_frequency_level,
            MAX(clm_level) AS clm_level
        FROM lazada_ads.ads_lzd_usr_uid_clm_d
        WHERE 1 = 1 --
            -- AND     ds >= 20211231 --< Start
            AND ds >= TO_CHAR(
                DATEADD(TO_DATE('${bizdate}', 'yyyymmdd'), -15, 'dd'),
                'yyyymmdd'
            ) --<< Daily
            AND venture = 'VN'
            AND clm_level IS NOT NULL
            AND clm_level NOT IN ('default')
        GROUP BY ds,
            user_id
    ) AS t2 ON t1.visitor_id = t2.user_id
    AND t1.ds = TO_CHAR(
        DATEADD(TO_DATE(t2.ds, 'yyyymmdd'), + 1, 'dd'),
        'yyyymmdd'
    );
----------------------------------------------------------------------------------------------------------------------------
-- lazada_analyst_dev.loutruong_d_seg_uid_1d
----------------------------------------------------------------------------------------------------------------------------
-- DROP TABLE IF EXISTS lazada_analyst_dev.loutruong_d_seg_uid_1d
-- ;
-- CREATE TABLE IF NOT EXISTS lazada_analyst_dev.loutruong_d_seg_uid_1d
-- (
--     user_id                         STRING
--     ,visit_frequency_level          STRING
--     ,purchase_frequency_level       STRING
--     ,purchase_order_frequency_level STRING
--     ,clm_level                      STRING COMMENT 'D Level in CLM2.0'
-- )
-- COMMENT 'VN only lazada_ads.ads_lzd_usr_uid_clm_d'
-- PARTITIONED BY 
-- (
--     ds                              STRING
-- )
-- LIFECYCLE 3600
-- ;
INSERT OVERWRITE TABLE lazada_analyst_dev.loutruong_d_seg_uid_1d PARTITION (ds)
SELECT user_id,
    visit_frequency_level,
    purchase_frequency_level,
    purchase_order_frequency_level,
    clm_level AS d_level,
    ds
FROM (
        SELECT TO_CHAR(
                DATEADD(TO_DATE(ds, 'yyyymmdd'), + 1, 'dd'),
                'yyyymmdd'
            ) AS ds,
            user_id,
            MAX(visit_frequency_level) AS visit_frequency_level,
            MAX(purchase_frequency_level) AS purchase_frequency_level,
            MAX(purchase_order_frequency_level) AS purchase_order_frequency_level,
            MAX(clm_level) AS clm_level
        FROM lazada_ads.ads_lzd_usr_uid_clm_d
        WHERE 1 = 1 --
            -- AND     ds >= 20211231 --< Start
            AND ds >= TO_CHAR(
                DATEADD(TO_DATE('${bizdate}', 'yyyymmdd'), -15, 'dd'),
                'yyyymmdd'
            ) --<< Daily
            AND venture = 'VN'
            AND clm_level IS NOT NULL
            AND clm_level NOT IN ('default')
        GROUP BY TO_CHAR(
                DATEADD(TO_DATE(ds, 'yyyymmdd'), + 1, 'dd'),
                'yyyymmdd'
            ),
            user_id
    )
WHERE 1 = 1 --
    AND ds < TO_CHAR(GETDATE(), 'yyyymmdd');
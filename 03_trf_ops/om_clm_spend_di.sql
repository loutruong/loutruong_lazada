-- MaxCompute SQL 
-- ********************************************************************--
-- author:vanthanh.truong
-- create time:2023-10-04 20:44:00
-- ********************************************************************--
--@@ Input = lazada_ads.ads_lzd_mkt_pol_channel_social_adset_perf_1d
--@@ Input = lazada_ads.ads_lzd_mkt_pol_channel_sem_campaign_1d
--@@ Input = lazada_ads.ads_lzd_mkt_pol_channel_display_overview_perf_1d
--@@ Input = lazada_ads.ads_lzd_mkt_pol_channel_tiktok_overview_perf_1d
--@@ Input = lazada_ads.ads_lzd_mkt_pol_channel_apple_search_overview_perf_1d
--@@ Input = lazada_ads.ads_lzd_mkt_retail_affiliate_adset_overview_mi
--@@ Output = lazada_analyst_dev.loutruong_ppc_spend_di
----------------------------------------------------------------------------------------------------------------------------
-- PARTITION_NOTE_1
-- AND ds >= 20230301 --<< Controller partition - Starting point, always >= 20230301, do not change
-- AND ds >= TO_CHAR(DATEADD(TO_DATE(${bizdate},'yyyymmdd'),-2,'dd'),'yyyymmdd') --<< Controller partition - Daily operation
-- AND ds >= TO_CHAR(DATEADD(TO_DATE(${bizdate},'yyyymmdd'),-360,'dd'),'yyyymmdd') --<< Controller partition - Maxout life cycle *Test before take action*
----------------------------------------------------------------------------------------------------------------------------
----------------------------------------------------------------------------------------------------------------------------
-- PARTITION_NOTE_2
-- AND report_date >= 20230301 --<< Controller partition - Starting point, always >= 20230301, do not change
-- AND report_date >= TO_CHAR(DATEADD(TO_DATE(${bizdate},'yyyymmdd'),-2,'dd'),'yyyymmdd') --<< Controller partition - Daily operation
-- AND report_date >= TO_CHAR(DATEADD(TO_DATE(${bizdate},'yyyymmdd'),-360,'dd'),'yyyymmdd') --<< Controller partition - Maxout life cycle *Test before take action*
----------------------------------------------------------------------------------------------------------------------------
-- DROP TABLE IF EXISTS loutruong_ppc_spend_di
-- ;
-- CREATE TABLE IF NOT EXISTS loutruong_ppc_spend_di
-- (
--     source       STRING COMMENT 'lazada om'
--     ,sub_source  STRING COMMENT 'ppc'
--     ,channel     STRING COMMENT 'social, sem, display, apple search, tiktok, arm'
--     ,bucket      STRING COMMENT 'install, non_install'
--     ,sub_channel STRING COMMENT 'channel - xxx name'
--     ,campaign    STRING COMMENT 'campaign name'
--     ,spend       DOUBLE
-- )
-- COMMENT 'Table contains all spending of VN Ppc up-to campaign level'
-- PARTITIONED BY 
-- (
--     ds           STRING
-- )
-- LIFECYCLE 3600
-- ;
INSERT OVERWRITE TABLE lazada_analyst_dev.loutruong_ppc_spend_di PARTITION (ds)
SELECT source,
    sub_source,
    COALESCE(channel, 'all') AS channel,
    COALESCE(bucket, 'all') AS bucket,
    COALESCE(sub_channel, 'all') AS sub_channel,
    COALESCE(campaign, 'all') AS campaign,
    COALESCE(SUM(spend), 0) AS spend,
    ds
FROM (
        SELECT ds --<< 1 OM - PPC - Social spend
,
            'lazada om' AS source,
            'ppc' AS sub_source,
            'social' AS channel,
            CASE
                WHEN TOLOWER(sub_channel) LIKE '%install%' THEN 'social_install'
                ELSE 'social_non_install'
            END AS bucket,
            CASE
                WHEN TOLOWER(rt_audience) IN ('allcat', 'gm', 'elL', 'fmcg', 'fa') THEN 'social - cluster'
                WHEN TOLOWER(rt_audience) IN ('churn buyer') THEN 'social - d0 churn buyer'
                WHEN TOLOWER(rt_audience) IN ('churn non buyer') THEN 'social - d0 churn non buyer'
                ELSE TOLOWER(sub_channel)
            END AS sub_channel,
            TOLOWER(campaign) AS campaign,
            SUM(COALESCE(spend, 0)) AS spend
        FROM lazada_ads.ads_lzd_mkt_pol_channel_social_adset_perf_1d
        WHERE 1 = 1 --
            -- AND     ds >= 20230301 --<< Controller partition - Starting point, always >= 20230301, do not change
            AND ds >= TO_CHAR(
                DATEADD(TO_DATE($ { bizdate }, 'yyyymmdd'), -2, 'dd'),
                'yyyymmdd'
            ) --<< Controller partition - Daily operation
            -- AND     ds >= TO_CHAR(DATEADD(TO_DATE(${bizdate},'yyyymmdd'),-360,'dd'),'yyyymmdd') --<< Controller partition - Maxout life cycle *Test before take action*
            AND venture = 'VN'
            AND TOLOWER(funding_bucket) IN ('lazada om')
            AND TOLOWER(funding_type) IN ('om')
            AND TOLOWER(campaign) LIKE '%vn%'
        GROUP BY ds,
            'lazada om',
            'ppc',
            'social',
            CASE
                WHEN TOLOWER(sub_channel) LIKE '%install%' THEN 'social_install'
                ELSE 'social_non_install'
            END,
            CASE
                WHEN TOLOWER(rt_audience) IN ('allcat', 'gm', 'elL', 'fmcg', 'fa') THEN 'social - cluster'
                WHEN TOLOWER(rt_audience) IN ('churn buyer') THEN 'social - d0 churn buyer'
                WHEN TOLOWER(rt_audience) IN ('churn non buyer') THEN 'social - d0 churn non buyer'
                ELSE TOLOWER(sub_channel)
            END,
            TOLOWER(campaign)
        UNION ALL
        --<< Break point
        SELECT ds --<< 2 OM - PPC - Sem spend
,
            'lazada om' AS source,
            'ppc' AS sub_source,
            'sem' AS channel,
            CASE
                WHEN TOLOWER(sub_channel) LIKE '%install%' THEN 'sem_install'
                ELSE 'sem_non_install'
            END AS bucket,
            TOLOWER(sub_channel) AS sub_channel,
            TOLOWER(campaign) AS campaign,
            SUM(COALESCE(spend, 0)) / 2 AS spend
        FROM lazada_ads.ads_lzd_mkt_pol_channel_sem_campaign_1d
        WHERE 1 = 1 --
            -- AND     ds >= 20230301 --<< Controller partition - Starting point, always >= 20230301, do not change
            AND ds >= TO_CHAR(
                DATEADD(TO_DATE($ { bizdate }, 'yyyymmdd'), -2, 'dd'),
                'yyyymmdd'
            ) --<< Controller partition - Daily operation
            -- AND     ds >= TO_CHAR(DATEADD(TO_DATE(${bizdate},'yyyymmdd'),-360,'dd'),'yyyymmdd') --<< Controller partition - Maxout life cycle *Test before take action*
            AND venture = 'VN'
            AND TOLOWER(funding_bucket) IN ('lazada om')
            AND TOLOWER(funding_type) IN ('om')
        GROUP BY ds,
            'lazada om',
            'ppc',
            'sem',
            CASE
                WHEN TOLOWER(sub_channel) LIKE '%install%' THEN 'sem_install'
                ELSE 'sem_non_install'
            END,
            TOLOWER(sub_channel),
            TOLOWER(campaign)
        UNION ALL
        --<< Break point
        SELECT ds --<< 3 OM - PPC - Display spend
,
            'lazada om' AS source,
            'ppc' AS sub_source,
            'display' AS channel,
            CASE
                WHEN TOLOWER(sub_channel) LIKE '%install%' THEN 'display_install'
                ELSE 'display_non_install'
            END AS bucket,
            TOLOWER(sub_channel) AS sub_channel,
            TOLOWER(campaign) AS campaign,
            COALESCE(spend, 0) AS spend
        FROM lazada_ads.ads_lzd_mkt_pol_channel_display_overview_perf_1d
        WHERE 1 = 1 --
            -- AND     ds >= 20230301 --<< Controller partition - Starting point, always >= 20230301, do not change
            AND ds >= TO_CHAR(
                DATEADD(TO_DATE($ { bizdate }, 'yyyymmdd'), -2, 'dd'),
                'yyyymmdd'
            ) --<< Controller partition - Daily operation
            -- AND     ds >= TO_CHAR(DATEADD(TO_DATE(${bizdate},'yyyymmdd'),-360,'dd'),'yyyymmdd') --<< Controller partition - Maxout life cycle *Test before take action*
            AND venture = 'VN'
            AND TOLOWER(funding_bucket) IN ('lazada om')
            AND TOLOWER(funding_type) IN ('om')
            AND TOLOWER(platform) IN ('all')
            AND TOLOWER(sub_channel) NOT IN ('all')
            AND TOLOWER(rt_audience) IN ('all')
            AND TOLOWER(adset) IN ('all')
            AND TOLOWER(campaign) NOT IN ('all')
        GROUP BY ds,
            'lazada om',
            'ppc',
            'display',
            CASE
                WHEN TOLOWER(sub_channel) LIKE '%install%' THEN 'display_install'
                ELSE 'display_non_install'
            END,
            TOLOWER(sub_channel),
            TOLOWER(campaign),
            COALESCE(spend, 0)
        UNION ALL
        --<< Break point
        SELECT ds --<< 4 OM - PPC - Tiktok spend
,
            'lazada om' AS source,
            'ppc' AS sub_source,
            'tiktok' AS channel,
            CASE
                WHEN TOLOWER(sub_channel) LIKE '%install%' THEN 'tiktok_install'
                ELSE 'tiktok_non_install'
            END AS bucket,
            TOLOWER(sub_channel) AS sub_channel,
            TOLOWER(campaign) AS campaign,
            COALESCE(spend, 0) AS spend
        FROM lazada_ads.ads_lzd_mkt_pol_channel_tiktok_overview_perf_1d
        WHERE 1 = 1 --
            -- AND     ds >= 20230301 --<< Controller partition - Starting point, always >= 20230301, do not change
            AND ds >= TO_CHAR(
                DATEADD(TO_DATE($ { bizdate }, 'yyyymmdd'), -2, 'dd'),
                'yyyymmdd'
            ) --<< Controller partition - Daily operation
            -- AND     ds >= TO_CHAR(DATEADD(TO_DATE(${bizdate},'yyyymmdd'),-360,'dd'),'yyyymmdd') --<< Controller partition - Maxout life cycle *Test before take action*
            AND venture = 'VN'
            AND TOLOWER(funding_bucket) IN ('lazada om')
            AND TOLOWER(funding_type) IN ('om')
            AND TOLOWER(platform) IN ('all')
            AND TOLOWER(sub_channel) NOT IN ('all')
            AND TOLOWER(rt_audience) IN ('all')
            AND TOLOWER(partner_name) IN ('all')
            AND TOLOWER(adset) IN ('all')
            AND TOLOWER(campaign) NOT IN ('all')
        GROUP BY ds,
            'lazada om',
            'ppc',
            'tiktok',
            CASE
                WHEN TOLOWER(sub_channel) LIKE '%install%' THEN 'tiktok_install'
                ELSE 'tiktok_non_install'
            END,
            TOLOWER(sub_channel),
            TOLOWER(campaign),
            COALESCE(spend, 0)
        UNION ALL
        --<< Break point
        SELECT ds --<< 5 OM - PPC - Apple search spend
,
            'lazada om' AS source,
            'ppc' AS sub_source,
            'apple search' AS channel,
            'apple_search_install' AS bucket,
            TOLOWER(sub_channel) AS sub_channel,
            TOLOWER(campaign) AS campaign,
            COALESCE(spend, 0) AS spend
        FROM lazada_ads.ads_lzd_mkt_pol_channel_apple_search_overview_perf_1d
        WHERE 1 = 1 --
            -- AND     ds >= 20230301 --<< Controller partition - Starting point, always >= 20230301, do not change
            AND ds >= TO_CHAR(
                DATEADD(TO_DATE($ { bizdate }, 'yyyymmdd'), -2, 'dd'),
                'yyyymmdd'
            ) --<< Controller partition - Daily operation
            -- AND     ds >= TO_CHAR(DATEADD(TO_DATE(${bizdate},'yyyymmdd'),-360,'dd'),'yyyymmdd') --<< Controller partition - Maxout life cycle *Test before take action*
            AND venture = 'VN'
            AND TOLOWER(funding_bucket) IN ('lazada om')
            AND TOLOWER(funding_type) IN ('om')
            AND TOLOWER(platform) IN ('all')
            AND TOLOWER(sub_channel) NOT IN ('all')
            AND TOLOWER(rt_audience) IN ('all')
            AND TOLOWER(adset) IN ('all')
            AND TOLOWER(campaign) NOT IN ('all')
        GROUP BY ds,
            'lazada om',
            'ppc',
            'apple search',
            'apple_search_install',
            TOLOWER(sub_channel),
            TOLOWER(campaign),
            COALESCE(spend, 0)
        UNION ALL
        --<< Break point
        SELECT report_date AS ds --<< 6 OM - PPC - Arm 1 spend
,
            'lazada om' AS source,
            'ppc' AS sub_source,
            'arm' channel,
            CASE
                WHEN TOLOWER(external_channel) IN ('facebook') THEN 'social'
                WHEN TOLOWER(external_channel) IN ('google') THEN 'sem'
                ELSE TOLOWER(external_channel)
            END AS bucket,
            CASE
                WHEN TOLOWER(external_channel) IN ('facebook') THEN 'social - arm'
                WHEN TOLOWER(external_channel) IN ('google') THEN 'sem - arm'
                ELSE TOLOWER(external_channel)
            END AS sub_channel,
            TOLOWER(campaign) AS campaign,
            COALESCE(cost_usd_loc, 0) AS spend
        FROM lazada_ads.ads_lzd_mkt_retail_affiliate_adset_overview_mi
        WHERE 1 = 1 --
            -- AND     report_date >= 20230301 --<< Controller partition - Starting point, always >= 20230301, do not change
            AND report_date >= TO_CHAR(
                DATEADD(TO_DATE($ { bizdate }, 'yyyymmdd'), -2, 'dd'),
                'yyyymmdd'
            ) --<< Controller partition - Daily operation
            -- AND     report_date >= TO_CHAR(DATEADD(TO_DATE(${bizdate},'yyyymmdd'),-360,'dd'),'yyyymmdd') --<< Controller partition - Maxout life cycle *Test before take action*
            AND venture = 'VN'
            AND TOLOWER(is_paid) IN ('paid')
            AND TOLOWER(attr_model) IN ('ft_1d_np')
            AND TOLOWER(platform) IN ('all')
            AND TOLOWER(os) IN ('all')
            AND TOLOWER(partner) IN ('all')
            AND TOLOWER(external_channel) NOT IN ('all')
            AND TOLOWER(campaign) NOT IN ('all')
            AND TOLOWER(campaign) NOT LIKE '%sbd%'
            AND TOLOWER(adset) IN ('all')
    )
GROUP BY GROUPING SETS (
        (
            ds,
            source,
            sub_source,
            channel,
            bucket,
            sub_channel,
            campaign
        ),
        (
            ds,
            source,
            sub_source,
            channel,
            bucket,
            sub_channel
        ),
        (ds, source, sub_source, channel, bucket),
        (ds, source, sub_source, channel),
        (ds, source, sub_source)
    )
ORDER BY ds,
    source,
    sub_source,
    channel,
    bucket,
    sub_channel,
    campaign;
--@@ Input = lazada_ads.ads_lzd_mkt_steering_sub_channel_cost_mi
--@@ Output = lazada_analyst_dev.loutruong_rta_spend_di
----------------------------------------------------------------------------------------------------------------------------
-- PARTITION_NOTE_1
-- AND ds >= 20230301 --<< Controller partition - Starting point, always >= 20230301, do not change
-- AND ds >= TO_CHAR(DATEADD(TO_DATE(${bizdate},'yyyymmdd'),-2,'dd'),'yyyymmdd') --<< Controller partition - Daily operation
-- AND ds >= TO_CHAR(DATEADD(TO_DATE(${bizdate},'yyyymmdd'),-360,'dd'),'yyyymmdd') --<< Controller partition - Maxout life cycle *Test before take action*
----------------------------------------------------------------------------------------------------------------------------
-- DROP TABLE IF EXISTS lazada_analyst_dev.loutruong_rta_spend_di
-- ;
-- CREATE TABLE IF NOT EXISTS lazada_analyst_dev.loutruong_rta_spend_di
-- (
--     source       STRING COMMENT 'lazada om'
--     ,sub_source  STRING COMMENT 'rta'
--     ,channel     STRING COMMENT 'rta'
--     ,bucket      STRING COMMENT 'install, non_install'
--     ,sub_channel STRING COMMENT 'channel - xxx name'
--     ,campaign    STRING COMMENT 'channel - xxx name'
--     ,spend       DOUBLE
-- )
-- COMMENT 'Table contains all spending of VN Rta up-to campaign level'
-- PARTITIONED BY 
-- (
--     ds           STRING
-- )
-- LIFECYCLE 3600
-- ;
INSERT OVERWRITE TABLE lazada_analyst_dev.loutruong_rta_spend_di PARTITION (ds)
SELECT source,
    sub_source,
    COALESCE(channel, 'all') AS channel,
    COALESCE(bucket, 'all') AS bucket,
    COALESCE(sub_channel, 'all') AS sub_channel,
    COALESCE(campaign, 'all') AS campaign,
    COALESCE(SUM(spend), 0) AS spend,
    ds
FROM (
        SELECT ds,
            'lazada om' AS source,
            'rta' AS sub_source,
            'rta' AS channel,
            CASE
                WHEN TOLOWER(sub_channel) LIKE '%install%' THEN 'rta_install'
                ELSE 'rta_non_install'
            END AS bucket,
            TOLOWER(sub_channel) AS sub_channel,
            TOLOWER(sub_channel) AS campaign,
            COALESCE(spend, 0) AS spend
        FROM lazada_ads.ads_lzd_mkt_steering_sub_channel_cost_mi
        WHERE 1 = 1 --
            -- AND     ds >= 20230301 --<< Controller partition - Starting point, always >= 20230301, do not change
            AND ds >= TO_CHAR(
                DATEADD(TO_DATE($ { bizdate }, 'yyyymmdd'), -2, 'dd'),
                'yyyymmdd'
            ) --<< Controller partition - Daily operation
            -- AND     ds >= TO_CHAR(DATEADD(TO_DATE(${bizdate},'yyyymmdd'),-360,'dd'),'yyyymmdd') --<< Controller partition - Maxout life cycle *Test before take action*
            AND venture = 'VN'
            AND TOLOWER(funding_type) IN ('om')
            AND TOLOWER(platform) IN ('all')
            AND TOLOWER(sub_channel) NOT IN (
                'all',
                'all ex rta',
                'unknown',
                'cps affiliate',
                'cpi affiliate'
            )
            AND TOLOWER(sub_channel) LIKE '%rta%'
            AND TOLOWER(rt_audience) IN ('all')
    )
GROUP BY GROUPING SETS (
        (
            ds,
            source,
            sub_source,
            channel,
            bucket,
            sub_channel,
            campaign
        ),
        (
            ds,
            source,
            sub_source,
            channel,
            bucket,
            sub_channel
        ),
        (ds, source, sub_source, channel, bucket),
        (ds, source, sub_source, channel),
        (ds, source, sub_source)
    )
ORDER BY ds,
    source,
    sub_source,
    channel,
    bucket,
    sub_channel,
    campaign;
--@@ Input = lazada_ads.ads_lzd_marketing_cps_conversion_report_mi
--@@ Input = lazada_analyst.loutruong_affiliate_offline_operation_cost
--@@ Input = lazada_cdm.dim_lzd_exchange_rate
--@@ Output = lazada_analyst_dev.loutruong_affiliate_spend_di
----------------------------------------------------------------------------------------------------------------------------
-- PARTITION_NOTE_1
-- AND REPLACE(SUBSTR(conversion_time,1,10),'-','') >= 20230301 --<< Controller partition - Starting point, always >= 20230301, do not change
-- AND REPLACE(SUBSTR(conversion_time,1,10),'-','') >= TO_CHAR(DATEADD(TO_DATE(${bizdate},'yyyymmdd'),-100,'dd'),'yyyymmdd') --<< Controller partition - Daily operation
-- AND REPLACE(SUBSTR(conversion_time,1,10),'-','') >= TO_CHAR(DATEADD(TO_DATE(${bizdate},'yyyymmdd'),-732,'dd'),'yyyymmdd') --<< Controller partition - Maxout life cycle *Test before take action*
----------------------------------------------------------------------------------------------------------------------------
----------------------------------------------------------------------------------------------------------------------------
-- PARTITION_NOTE_2
-- AND ds >= 20230301 --<< Controller partition - Starting point, always >= 20230301, do not change
-- AND ds >= TO_CHAR(DATEADD(TO_DATE(${bizdate},'yyyymmdd'),-100,'dd'),'yyyymmdd') --<< Controller partition - Daily operation
-- AND ds >= TO_CHAR(DATEADD(TO_DATE(${bizdate},'yyyymmdd'),-732,'dd'),'yyyymmdd') --<< Controller partition - Maxout life cycle *Test before take action*
----------------------------------------------------------------------------------------------------------------------------
-- DROP TABLE IF EXISTS lazada_analyst_dev.loutruong_affiliate_spend_di
-- ;
-- CREATE TABLE IF NOT EXISTS lazada_analyst_dev.loutruong_affiliate_spend_di
-- (
--     source       STRING COMMENT 'lazada om'
--     ,sub_source  STRING COMMENT 'affiliate'
--     ,channel     STRING COMMENT 'affiliate'
--     ,bucket      STRING COMMENT 'affiliate'
--     ,sub_channel STRING COMMENT 'affiliate'
--     ,campaign    STRING COMMENT 'affiliate'
--     ,spend       DOUBLE
-- )
-- COMMENT 'Table contains all spending of VN Affiliate up-to campaign level'
-- PARTITIONED BY 
-- (
--     ds           STRING
-- )
-- LIFECYCLE 3600
-- ;
INSERT OVERWRITE TABLE lazada_analyst_dev.loutruong_affiliate_spend_di PARTITION (ds)
SELECT source,
    sub_source,
    COALESCE(channel, 'all') AS channel,
    COALESCE(bucket, 'all') AS bucket,
    COALESCE(sub_channel, 'all') AS sub_channel,
    COALESCE(campaign, 'all') AS campaign,
    COALESCE(SUM(spend), 0) AS spend,
    ds
FROM (
        SELECT t1.ds AS ds,
            'lazada om' AS source,
            'affiliate' AS sub_source,
            'affiliate' AS channel,
            'affiliate' AS bucket,
            'affiliate' AS sub_channel,
            'affiliate' AS campaign,
            COALESCE(
                SUM(t1.platform_commission_fee * t2.to_usd) + COALESCE(t3.spend, 0),
                0
            ) AS spend
        FROM (
                SELECT REPLACE(SUBSTR(conversion_time, 1, 10), '-', '') AS ds,
                    COALESCE(platform_commission_fee, 0) AS platform_commission_fee
                FROM lazada_ads.ads_lzd_marketing_cps_conversion_report_mi
                WHERE 1 = 1 --
                    -- AND     REPLACE(SUBSTR(conversion_time,1,10),'-','') >= 20230301 --<< Controller partition - Starting point, always >= 20230301, do not change
                    AND REPLACE(SUBSTR(conversion_time, 1, 10), '-', '') >= TO_CHAR(
                        DATEADD(TO_DATE($ { bizdate }, 'yyyymmdd'), -100, 'dd'),
                        'yyyymmdd'
                    ) --<< Controller partition - Daily operation
                    -- AND     REPLACE(SUBSTR(conversion_time,1,10),'-','') >= TO_CHAR(DATEADD(TO_DATE(${bizdate},'yyyymmdd'),-732,'dd'),'yyyymmdd') --<< Controller partition - Maxout life cycle *Test before take action*
                    AND venture = 'VN'
                    AND TOLOWER(source) IN ('cps', 'brand', 'seller', 'om', 'brand/seller')
                    AND TOLOWER(adjust_type) NOT IN ('stop_first_order')
                    AND TOLOWER(STATUS) IN ('delivered')
                    AND (
                        is_fraud = 0
                        OR is_fraud IS NULL
                    )
                    AND member_id NOT IN ('221351306') --<< PPC ARM member ID
            ) AS t1
            JOIN (
                SELECT ds,
                    to_usd
                FROM lazada_cdm.dim_lzd_exchange_rate
                WHERE 1 = 1 --
                    -- AND     ds >= 20230301 --<< Controller partition - Starting point, always >= 20230301, do not change
                    AND ds >= TO_CHAR(
                        DATEADD(TO_DATE($ { bizdate }, 'yyyymmdd'), -100, 'dd'),
                        'yyyymmdd'
                    ) --<< Controller partition - Daily operation
                    -- AND     ds >= TO_CHAR(DATEADD(TO_DATE(${bizdate},'yyyymmdd'),-732,'dd'),'yyyymmdd') --<< Controller partition - Maxout life cycle *Test before take action*
                    AND venture = 'VN'
                GROUP BY ds,
                    to_usd
            ) AS t2 ON t1.ds = t2.ds
            LEFT JOIN (
                SELECT ds,
                    COALESCE(CAST(spend AS DOUBLE), 0) AS spend
                FROM lazada_analyst.loutruong_affiliate_offline_operation_spend
                WHERE 1 = 1 --
                    -- AND     ds >= 20230301 --<< Controller partition - Starting point, always >= 20230301, do not change
                    AND ds >= TO_CHAR(
                        DATEADD(TO_DATE($ { bizdate }, 'yyyymmdd'), -100, 'dd'),
                        'yyyymmdd'
                    ) --<< Controller partition - Daily operation
                    -- AND     ds >= TO_CHAR(DATEADD(TO_DATE(${bizdate},'yyyymmdd'),-732,'dd'),'yyyymmdd') --<< Controller partition - Maxout life cycle *Test before take action*
            ) AS t3 ON t1.ds = t3.ds
        GROUP BY t1.ds,
            'lazada om',
            'affiliate',
            t3.spend
    )
GROUP BY GROUPING SETS (
        (
            ds,
            source,
            sub_source,
            channel,
            bucket,
            sub_channel,
            campaign
        ),
        (
            ds,
            source,
            sub_source,
            channel,
            bucket,
            sub_channel
        ),
        (ds, source, sub_source, channel, bucket),
        (ds, source, sub_source, channel),
        (ds, source, sub_source)
    )
ORDER BY ds,
    source,
    sub_source,
    channel,
    bucket,
    sub_channel,
    campaign;
--@@ Input = lazada_ads.ads_lzd_marketing_cps_conversion_report_mi
--@@ Input = lazada_cdm.dwd_lzd_trd_core_fulfill_di
--@@ Output = lazada_analyst_dev.loutruong_trn_order_app_di
----------------------------------------------------------------------------------------------------------------------------
-- PARTITION_NOTE_1
-- AND REPLACE(SUBSTR(conversion_time,1,10),'-','') >= 20230301 --<< Controller partition - Starting point, always >= 20230301, do not change
-- AND REPLACE(SUBSTR(conversion_time,1,10),'-','') >= TO_CHAR(DATEADD(TO_DATE(${bizdate},'yyyymmdd'),-100,'dd'),'yyyymmdd') --<< Controller partition - Daily operation
-- AND REPLACE(SUBSTR(conversion_time,1,10),'-','') >= TO_CHAR(DATEADD(TO_DATE(${bizdate},'yyyymmdd'),-732,'dd'),'yyyymmdd') --<< Controller partition - Maxout life cycle *Test before take action*
----------------------------------------------------------------------------------------------------------------------------
----------------------------------------------------------------------------------------------------------------------------
-- PARTITION_NOTE_2
-- AND TO_CHAR(order_create_date,'yyyymmdd') >= 20230301 --<< Controller partition - Starting point, always >= 20230301, do not change
-- AND TO_CHAR(order_create_date,'yyyymmdd') >= TO_CHAR(DATEADD(TO_DATE(${bizdate},'yyyymmdd'),-100,'dd'),'yyyymmdd') --<< Controller partition - Daily operation
-- AND TO_CHAR(order_create_date,'yyyymmdd') >= TO_CHAR(DATEADD(TO_DATE(${bizdate},'yyyymmdd'),-732,'dd'),'yyyymmdd') --<< Controller partition - Maxout life cycle *Test before take action*
----------------------------------------------------------------------------------------------------------------------------
-- DROP TABLE IF EXISTS lazada_analyst_dev.loutruong_trn_order_app_di
-- ;
-- CREATE TABLE IF NOT EXISTS lazada_analyst_dev.loutruong_trn_order_app_di
-- (
--     source         STRING COMMENT 'lazada om'
--     ,sub_source    STRING COMMENT 'affiliate'
--     ,channel       STRING COMMENT 'affiliate'
--     ,bucket        STRING COMMENT 'affiliate'
--     ,sub_channel   STRING COMMENT 'affiliate'
--     ,campaign      STRING COMMENT 'affiliate'
--     ,utdid         STRING
--     ,buyer_id      STRING
--     ,check_out_id  STRING
--     ,order_app_cnt DOUBLE
-- )
-- COMMENT 'Table contains all utdid ordered of VN affiliate up-to campaign level'
-- PARTITIONED BY 
-- (
--     ds             STRING
-- )
-- LIFECYCLE 3600
-- ;
INSERT OVERWRITE TABLE lazada_analyst_dev.loutruong_trn_order_app_di PARTITION (ds)
SELECT 'lazada om' AS source,
    'affiliate' AS sub_source,
    'affiliate' AS channel,
    'affiliate' AS bucket,
    'affiliate' AS sub_channel,
    'affiliate' AS campaign,
    t2.utdid AS utdid,
    t2.buyer_id AS buyer_id,
    t2.check_out_id AS check_out_id,
    COUNT(DISTINCT t2.order_id) AS order_app_cnt,
    t1.ds AS ds
FROM (
        SELECT REPLACE(SUBSTR(conversion_time, 1, 10), '-', '') AS ds,
            sub_order_id AS sales_order_item_id
        FROM lazada_ads.ads_lzd_marketing_cps_conversion_report_mi
        WHERE 1 = 1 --
            -- AND     REPLACE(SUBSTR(conversion_time,1,10),'-','') >= 20230301 --<< Controller partition - Starting point, always >= 20230301, do not change
            AND REPLACE(SUBSTR(conversion_time, 1, 10), '-', '') >= TO_CHAR(
                DATEADD(TO_DATE($ { bizdate }, 'yyyymmdd'), -100, 'dd'),
                'yyyymmdd'
            ) --<< Controller partition - Daily operation
            -- AND     REPLACE(SUBSTR(conversion_time,1,10),'-','') >= TO_CHAR(DATEADD(TO_DATE(${bizdate},'yyyymmdd'),-732,'dd'),'yyyymmdd') --<< Controller partition - Maxout life cycle *Test before take action*
            AND venture = 'VN'
            AND TOLOWER(platform) IN ('app')
            AND TOLOWER(source) IN ('cps', 'brand', 'seller', 'om', 'brand/seller')
            AND TOLOWER(adjust_type) NOT IN ('stop_first_order')
            AND TOLOWER(STATUS) IN ('delivered')
            AND (
                is_fraud = 0
                OR is_fraud IS NULL
            )
            AND member_id NOT IN ('221351306') --<< PPC ARM member
        GROUP BY REPLACE(SUBSTR(conversion_time, 1, 10), '-', ''),
            sub_order_id
    ) AS t1
    INNER JOIN (
        SELECT TO_CHAR(order_create_date, 'yyyymmdd') AS ds,
            usertrack_id AS utdid,
            buyer_id,
            check_out_id,
            order_id,
            sales_order_item_id
        FROM lazada_cdm.dwd_lzd_trd_core_fulfill_di
        WHERE 1 = 1 --
            -- AND     TO_CHAR(order_create_date,'yyyymmdd') >= 20230301 --<< Controller partition - Starting point, always >= 20230301, do not change
            AND TO_CHAR(order_create_date, 'yyyymmdd') >= TO_CHAR(
                DATEADD(TO_DATE($ { bizdate }, 'yyyymmdd'), -100, 'dd'),
                'yyyymmdd'
            ) --<< Controller partition - Daily operation
            -- AND     TO_CHAR(order_create_date,'yyyymmdd') >= TO_CHAR(DATEADD(TO_DATE(${bizdate},'yyyymmdd'),-732,'dd'),'yyyymmdd') --<< Controller partition - Maxout life cycle *Test before take action*
            AND venture = 'VN'
            AND TOLOWER(device_type) IN ('mobile')
            AND usertrack_id IS NOT NULL
            AND is_revenue = 1
            AND is_fulfilled = 1
        GROUP BY TO_CHAR(order_create_date, 'yyyymmdd'),
            usertrack_id,
            buyer_id,
            check_out_id,
            order_id,
            sales_order_item_id
    ) AS t2 ON t1.sales_order_item_id = t2.sales_order_item_id
GROUP BY 'lazada om',
    'affiliate',
    t2.utdid,
    t2.buyer_id,
    t2.check_out_id,
    t1.ds
ORDER BY ds,
    source,
    sub_source,
    channel,
    bucket,
    sub_channel,
    campaign;
--@@ Input = lazada_ads.ads_lzd_mkt_app_utdid_channel_dims_1d
--@@ Output = lazada_analyst_dev.loutruong_trf_touch_app_di
----------------------------------------------------------------------------------------------------------------------------
-- PARTITION_NOTE_1
-- AND ds >= 20230301 --<< Controller partition - Starting point, always >= 20230301, do not change
-- AND ds >= TO_CHAR(DATEADD(TO_DATE(${bizdate},'yyyymmdd'),0,'dd'),'yyyymmdd') --<< Controller partition - Daily operation
-- AND ds >= TO_CHAR(DATEADD(TO_DATE(${bizdate},'yyyymmdd'),-1110,'dd'),'yyyymmdd') --<< Controller partition - Maxout life cycle *Test before take action*
----------------------------------------------------------------------------------------------------------------------------
-- DROP TABLE IF EXISTS lazada_analyst_dev.loutruong_trf_touch_app_di
-- ;
-- CREATE TABLE IF NOT EXISTS lazada_analyst_dev.loutruong_trf_touch_app_di
-- (
--     source       STRING COMMENT 'lazada om, crm, direct, lazada branding, marketing solution, mmm, partnerships, retail affiliates, seller, seo, user share'
--     ,sub_source  STRING COMMENT 'ppc, rta, affiliate, crm, direct, lazada branding, marketing solution, mmm, partnerships, retail affiliates, seller, seo, user share'
--     ,channel     STRING COMMENT 'social, sem, display, apple search, tiktok, arm, crm, direct, lazada branding, marketing solution, mmm, partnerships, retail affiliates, seller, seo, user share'
--     ,bucket      STRING COMMENT 'install, non_install'
--     ,sub_channel STRING COMMENT 'channel - xxx name'
--     ,campaign    STRING COMMENT 'campaign name'
--     ,utdid       STRING COMMENT 'mobile app device id of user either login or not'
-- )
-- COMMENT 'Table contains all utdid touched of VN up-to campaign level'
-- PARTITIONED BY 
-- (
--     ds           STRING
-- )
-- LIFECYCLE 3600
-- ;
INSERT OVERWRITE TABLE lazada_analyst_dev.loutruong_trf_touch_app_di PARTITION (ds)
SELECT source,
    sub_source,
    channel,
    bucket,
    sub_channel,
    campaign,
    utdid,
    ds
FROM (
        SELECT DISTINCT ds --<< 1 OM - PPC - Social touch
,
            TOLOWER(source) AS source,
            'ppc' AS sub_source,
            TOLOWER(channel) AS channel,
            CASE
                WHEN TOLOWER(sub_channel) LIKE '%install%' THEN 'social_install'
                ELSE 'social_non_install'
            END AS bucket,
            CASE
                WHEN TOLOWER(rt_audience) IN ('allcat', 'gm', 'elL', 'fmcg', 'fa') THEN 'social - cluster'
                WHEN TOLOWER(rt_audience) IN ('churn buyer') THEN 'social - d0 churn buyer'
                WHEN TOLOWER(rt_audience) IN ('churn non buyer') THEN 'social - d0 churn non buyer'
                ELSE TOLOWER(sub_channel)
            END AS sub_channel,
            TOLOWER(campaign_name) AS campaign,
            utdid
        FROM lazada_ads.ads_lzd_mkt_app_utdid_channel_dims_1d
        WHERE 1 = 1 --
            -- AND     ds >= 20230301 --<< Controller partition - Starting point, always >= 20230301, do not change
            AND ds >= TO_CHAR(
                DATEADD(TO_DATE($ { bizdate }, 'yyyymmdd'), 0, 'dd'),
                'yyyymmdd'
            ) --<< Controller partition - Daily operation
            -- AND     ds >= TO_CHAR(DATEADD(TO_DATE(${bizdate},'yyyymmdd'),-1110,'dd'),'yyyymmdd') --<< Controller partition - Maxout life cycle *Test before take action*
            AND venture = 'VN'
            AND TOLOWER(source) IN ('lazada om') --<< Controller
            AND TOLOWER(funding_type) IN ('om') --<< Controller
            AND TOLOWER(channel) IN ('social') --<< Controller
            AND TOLOWER(campaign_name) LIKE '%vn%' --
            -- AND     traffic_attr_model IN ('First_Touch') --<< Active when need attr_model IN ('ft_1d_np')
            AND traffic_attr_model IN ('Any_Touch')
            AND utdid IS NOT NULL
        UNION ALL
        --<< Break point
        SELECT DISTINCT ds --<< 2 OM - PPC - Sem touch
,
            TOLOWER(source) AS source,
            'ppc' AS sub_source,
            TOLOWER(channel) AS channel,
            CASE
                WHEN TOLOWER(sub_channel) LIKE '%install%' THEN 'sem_install'
                ELSE 'sem_non_install'
            END AS bucket,
            TOLOWER(sub_channel) AS sub_channel,
            TOLOWER(campaign_name) AS campaign,
            utdid
        FROM lazada_ads.ads_lzd_mkt_app_utdid_channel_dims_1d
        WHERE 1 = 1 --
            -- AND     ds >= 20230301 --<< Controller partition - Starting point, always >= 20230301, do not change
            AND ds >= TO_CHAR(
                DATEADD(TO_DATE($ { bizdate }, 'yyyymmdd'), 0, 'dd'),
                'yyyymmdd'
            ) --<< Controller partition - Daily operation
            -- AND     ds >= TO_CHAR(DATEADD(TO_DATE(${bizdate},'yyyymmdd'),-1110,'dd'),'yyyymmdd') --<< Controller partition - Maxout life cycle *Test before take action*
            AND venture = 'VN'
            AND TOLOWER(source) IN ('lazada om') --<< Controller
            AND TOLOWER(funding_type) IN ('om') --<< Controller
            AND TOLOWER(channel) IN ('sem') --<< Controller
            -- AND     traffic_attr_model IN ('First_Touch') --<< Active when need attr_model IN ('ft_1d_np')
            AND traffic_attr_model IN ('Any_Touch')
            AND utdid IS NOT NULL
        UNION ALL
        --<< Break point
        SELECT DISTINCT ds --<< 3 OM - PPC - Display touch
,
            TOLOWER(source) AS source,
            'ppc' AS sub_source,
            TOLOWER(channel) AS channel,
            CASE
                WHEN TOLOWER(sub_channel) LIKE '%install%' THEN 'display_install'
                ELSE 'display_non_install'
            END AS bucket,
            TOLOWER(sub_channel) AS sub_channel,
            TOLOWER(campaign_name) AS campaign,
            utdid
        FROM lazada_ads.ads_lzd_mkt_app_utdid_channel_dims_1d
        WHERE 1 = 1 --
            -- AND     ds >= 20230301 --<< Controller partition - Starting point, always >= 20230301, do not change
            AND ds >= TO_CHAR(
                DATEADD(TO_DATE($ { bizdate }, 'yyyymmdd'), 0, 'dd'),
                'yyyymmdd'
            ) --<< Controller partition - Daily operation
            -- AND     ds >= TO_CHAR(DATEADD(TO_DATE(${bizdate},'yyyymmdd'),-1110,'dd'),'yyyymmdd') --<< Controller partition - Maxout life cycle *Test before take action*
            AND venture = 'VN'
            AND TOLOWER(source) IN ('lazada om') --<< Controller
            AND TOLOWER(funding_type) IN ('om') --<< Controller
            AND TOLOWER(channel) IN ('display') --<< Controller
            -- AND     traffic_attr_model IN ('First_Touch') --<< Active when need attr_model IN ('ft_1d_np')
            AND traffic_attr_model IN ('Any_Touch')
            AND utdid IS NOT NULL
        UNION ALL
        --<< Break point
        SELECT DISTINCT ds --<< 4 OM - PPC - Tiktok touch
,
            TOLOWER(source) AS source,
            'ppc' AS sub_source,
            TOLOWER(channel) AS channel,
            CASE
                WHEN TOLOWER(sub_channel) LIKE '%install%' THEN 'tiktok_install'
                ELSE 'tiktok_non_install'
            END AS bucket,
            TOLOWER(sub_channel) AS sub_channel,
            TOLOWER(campaign_name) AS campaign,
            utdid
        FROM lazada_ads.ads_lzd_mkt_app_utdid_channel_dims_1d
        WHERE 1 = 1 --
            -- AND     ds >= 20230301 --<< Controller partition - Starting point, always >= 20230301, do not change
            AND ds >= TO_CHAR(
                DATEADD(TO_DATE($ { bizdate }, 'yyyymmdd'), 0, 'dd'),
                'yyyymmdd'
            ) --<< Controller partition - Daily operation
            -- AND     ds >= TO_CHAR(DATEADD(TO_DATE(${bizdate},'yyyymmdd'),-1110,'dd'),'yyyymmdd') --<< Controller partition - Maxout life cycle *Test before take action*
            AND venture = 'VN'
            AND TOLOWER(source) IN ('lazada om') --<< Controller
            AND TOLOWER(funding_type) IN ('om') --<< Controller
            AND TOLOWER(channel) IN ('tiktok') --<< Controller
            -- AND     traffic_attr_model IN ('First_Touch') --<< Active when need attr_model IN ('ft_1d_np')
            AND traffic_attr_model IN ('Any_Touch')
            AND utdid IS NOT NULL
        UNION ALL
        --<< Break point
        SELECT DISTINCT ds --<< 5 OM - PPC - Apple search touch
,
            TOLOWER(source) AS source,
            'ppc' AS sub_source,
            TOLOWER(channel) AS channel,
            'apple_search_install' AS bucket,
            TOLOWER(sub_channel) AS sub_channel,
            TOLOWER(campaign_name) AS campaign,
            utdid
        FROM lazada_ads.ads_lzd_mkt_app_utdid_channel_dims_1d
        WHERE 1 = 1 --
            -- AND     ds >= 20230301 --<< Controller partition - Starting point, always >= 20230301, do not change
            AND ds >= TO_CHAR(
                DATEADD(TO_DATE($ { bizdate }, 'yyyymmdd'), 0, 'dd'),
                'yyyymmdd'
            ) --<< Controller partition - Daily operation
            -- AND     ds >= TO_CHAR(DATEADD(TO_DATE(${bizdate},'yyyymmdd'),-1110,'dd'),'yyyymmdd') --<< Controller partition - Maxout life cycle *Test before take action*
            AND venture = 'VN'
            AND TOLOWER(source) IN ('lazada om') --<< Controller
            AND TOLOWER(funding_type) IN ('om') --<< Controller
            AND TOLOWER(channel) IN ('apple search') --<< Controller
            -- AND     traffic_attr_model IN ('First_Touch') --<< Active when need attr_model IN ('ft_1d_np')
            AND traffic_attr_model IN ('Any_Touch')
            AND utdid IS NOT NULL
        UNION ALL
        --<< Break point
        SELECT DISTINCT ds --<< 6 OM - RTA - Rta touch
,
            TOLOWER(source) AS source,
            TOLOWER(sub_source) AS sub_source,
            TOLOWER(channel) AS channel,
            CASE
                WHEN TOLOWER(sub_channel) LIKE '%install%' THEN 'rta_install'
                ELSE 'rta_non_install'
            END AS bucket,
            TOLOWER(sub_channel) AS sub_channel,
            TOLOWER(sub_channel) AS campaign,
            utdid
        FROM lazada_ads.ads_lzd_mkt_app_utdid_channel_dims_1d
        WHERE 1 = 1 --
            -- AND     ds >= 20230301 --<< Controller partition - Starting point, always >= 20230301, do not change
            AND ds >= TO_CHAR(
                DATEADD(TO_DATE($ { bizdate }, 'yyyymmdd'), 0, 'dd'),
                'yyyymmdd'
            ) --<< Controller partition - Daily operation
            -- AND     ds >= TO_CHAR(DATEADD(TO_DATE(${bizdate},'yyyymmdd'),-1110,'dd'),'yyyymmdd') --<< Controller partition - Maxout life cycle *Test before take action*
            AND venture = 'VN'
            AND TOLOWER(source) IN ('lazada om') --<< Controller
            AND TOLOWER(funding_type) IN ('om') --<< Controller
            AND TOLOWER(channel) IN ('rta') --<< Controller
            -- AND     traffic_attr_model IN ('First_Touch') --<< Active when need attr_model IN ('ft_1d_np')
            AND traffic_attr_model IN ('Any_Touch')
            AND utdid IS NOT NULL
        UNION ALL
        --<< Break point
        SELECT DISTINCT ds --<< 7 OM - Affiliate - Affiliate touch
,
            TOLOWER(source) AS source,
            'affiliate' AS sub_source,
            'affiliate' AS channel,
            'affiliate' AS bucket,
            'affiliate' AS sub_channel,
            'affiliate' AS campaign,
            utdid
        FROM lazada_ads.ads_lzd_mkt_app_utdid_channel_dims_1d
        WHERE 1 = 1 --
            -- AND     ds >= 20230301 --<< Controller partition - Starting point, always >= 20230301, do not change
            AND ds >= TO_CHAR(
                DATEADD(TO_DATE($ { bizdate }, 'yyyymmdd'), 0, 'dd'),
                'yyyymmdd'
            ) --<< Controller partition - Daily operation
            -- AND     ds >= TO_CHAR(DATEADD(TO_DATE(${bizdate},'yyyymmdd'),-1110,'dd'),'yyyymmdd') --<< Controller partition - Maxout life cycle *Test before take action*
            AND venture = 'VN'
            AND TOLOWER(source) IN ('lazada om') --<< Controller
            AND TOLOWER(funding_type) IN ('om') --<< Controller
            AND TOLOWER(channel) IN ('cpi affiliate', 'cps affiliate') --<< Controller
            -- AND     traffic_attr_model IN ('First_Touch') --<< Active when need attr_model IN ('ft_1d_np')
            AND traffic_attr_model IN ('Any_Touch')
            AND utdid IS NOT NULL
        UNION ALL
        --<< Break point
        SELECT DISTINCT ds --<< 8 OM - PPC - funding_type not in ('om','ams') touch
,
            'direct' AS source,
            'direct' AS sub_source,
            'direct' AS channel,
            'direct' AS bucket,
            'direct' AS sub_channel,
            'direct' AS campaign,
            utdid
        FROM lazada_ads.ads_lzd_mkt_app_utdid_channel_dims_1d
        WHERE 1 = 1 --
            -- AND     ds >= 20230301 --<< Controller partition - Starting point, always >= 20230301, do not change
            AND ds >= TO_CHAR(
                DATEADD(TO_DATE($ { bizdate }, 'yyyymmdd'), 0, 'dd'),
                'yyyymmdd'
            ) --<< Controller partition - Daily operation
            -- AND     ds >= TO_CHAR(DATEADD(TO_DATE(${bizdate},'yyyymmdd'),-1110,'dd'),'yyyymmdd') --<< Controller partition - Maxout life cycle *Test before take action*
            AND venture = 'VN'
            AND TOLOWER(source) IN ('lazada om') --<< Controller
            AND TOLOWER(funding_type) NOT IN ('om', 'ams') --<< Controller
            -- AND     traffic_attr_model IN ('First_Touch') --<< Active when need attr_model IN ('ft_1d_np')
            AND traffic_attr_model IN ('Any_Touch')
            AND utdid IS NOT NULL
        UNION ALL
        --<< Break point
        SELECT DISTINCT ds --<< 9 OM - PPC - ARM1 campaign_name not like '%sbd%' touch
,
            'lazada om' AS source,
            'ppc' AS sub_source,
            'arm' AS channel,
            CASE
                WHEN TOLOWER(campaign_name) LIKE '%armsocial%' THEN 'social'
                WHEN TOLOWER(campaign_name) LIKE '%armsem%' THEN 'sem'
                ELSE 'unknown_arm1_bucket'
            END AS bucket,
            CASE
                WHEN TOLOWER(campaign_name) LIKE '%armsocial%' THEN 'social - arm'
                WHEN TOLOWER(campaign_name) LIKE '%armsem%' THEN 'sem - arm'
                ELSE 'unknown_arm1_sub_channel'
            END AS sub_channel,
            TOLOWER(campaign_name) AS campaign,
            utdid
        FROM lazada_ads.ads_lzd_mkt_app_utdid_channel_dims_1d
        WHERE 1 = 1 --
            -- AND     ds >= 20230301 --<< Controller partition - Starting point, always >= 20230301, do not change
            AND ds >= TO_CHAR(
                DATEADD(TO_DATE($ { bizdate }, 'yyyymmdd'), 0, 'dd'),
                'yyyymmdd'
            ) --<< Controller partition - Daily operation
            -- AND     ds >= TO_CHAR(DATEADD(TO_DATE(${bizdate},'yyyymmdd'),-1110,'dd'),'yyyymmdd') --<< Controller partition - Maxout life cycle *Test before take action*
            AND venture = 'VN'
            AND TOLOWER(source) IN ('retail affiliates') --<< Controller
            AND TOLOWER(funding_type) IN ('ams') --<< Controller
            AND TOLOWER(campaign_name) NOT LIKE '%sbd%'
            AND TOLOWER(campaign_name) LIKE '%vn%' -- AND     traffic_attr_model IN ('First_Touch') --<< Active when need attr_model IN ('ft_1d_np')
            AND traffic_attr_model IN ('Any_Touch')
            AND utdid IS NOT NULL
        UNION ALL
        --<< Break point
        SELECT DISTINCT ds --<< 10 OM - PPC - ARM1 campaign_name like '%sbd%' touch
,
            'direct' AS source,
            'direct' AS sub_source,
            'direct' AS channel,
            'direct' AS bucket,
            'direct' AS sub_channel,
            'direct' AS campaign,
            utdid
        FROM lazada_ads.ads_lzd_mkt_app_utdid_channel_dims_1d
        WHERE 1 = 1 --
            -- AND     ds >= 20230301 --<< Controller partition - Starting point, always >= 20230301, do not change
            AND ds >= TO_CHAR(
                DATEADD(TO_DATE($ { bizdate }, 'yyyymmdd'), 0, 'dd'),
                'yyyymmdd'
            ) --<< Controller partition - Daily operation
            -- AND     ds >= TO_CHAR(DATEADD(TO_DATE(${bizdate},'yyyymmdd'),-1110,'dd'),'yyyymmdd') --<< Controller partition - Maxout life cycle *Test before take action*
            AND venture = 'VN'
            AND TOLOWER(source) IN ('retail affiliates') --<< Controller
            AND TOLOWER(funding_type) IN ('ams') --<< Controller
            AND TOLOWER(campaign_name) LIKE '%sbd%'
            AND TOLOWER(campaign_name) LIKE '%vn%' -- AND     traffic_attr_model IN ('First_Touch') --<< Active when need attr_model IN ('ft_1d_np')
            AND traffic_attr_model IN ('Any_Touch')
            AND utdid IS NOT NULL
        UNION ALL
        --<< Break point
        SELECT DISTINCT ds --<< 11 OM - PPC - ARM2 touch
,
            TOLOWER(source) AS source,
            'ppc' AS sub_source,
            TOLOWER(channel) AS channel,
            CASE
                WHEN TOLOWER(campaign_name) LIKE '%armsocial%' THEN 'social'
                WHEN TOLOWER(campaign_name) LIKE '%armsem%' THEN 'sem'
                ELSE 'unknown_arm2_bucket'
            END AS bucket,
            CASE
                WHEN TOLOWER(campaign_name) LIKE '%armsocial%' THEN 'social - arm'
                WHEN TOLOWER(campaign_name) LIKE '%armsem%' THEN 'sem - arm'
                ELSE 'unknown_arm2_sub_channel'
            END AS sub_channel,
            TOLOWER(campaign_name) AS campaign,
            utdid
        FROM lazada_ads.ads_lzd_mkt_app_utdid_channel_dims_1d
        WHERE 1 = 1 --
            -- AND     ds >= 20230301 --<< Controller partition - Starting point, always >= 20230301, do not change
            AND ds >= TO_CHAR(
                DATEADD(TO_DATE($ { bizdate }, 'yyyymmdd'), 0, 'dd'),
                'yyyymmdd'
            ) --<< Controller partition - Daily operation
            -- AND     ds >= TO_CHAR(DATEADD(TO_DATE(${bizdate},'yyyymmdd'),-1110,'dd'),'yyyymmdd') --<< Controller partition - Maxout life cycle *Test before take action*
            AND venture = 'VN'
            AND TOLOWER(source) IN ('lazada om') --<< Controller
            AND TOLOWER(sub_source) IN ('arm') --<< Controller
            AND TOLOWER(campaign_name) LIKE '%vn%' -- AND     traffic_attr_model IN ('First_Touch') --<< Active when need attr_model IN ('ft_1d_np')
            AND traffic_attr_model IN ('Any_Touch')
            AND utdid IS NOT NULL
        UNION ALL
        --<< Break point
        SELECT DISTINCT ds --<< 12 Total platform - source not in ('lazada om') touch
,
            TOLOWER(source) AS source,
            TOLOWER(source) AS sub_source,
            TOLOWER(source) AS channel,
            TOLOWER(source) AS bucket,
            TOLOWER(source) AS sub_channel,
            TOLOWER(source) AS campaign,
            utdid
        FROM lazada_ads.ads_lzd_mkt_app_utdid_channel_dims_1d
        WHERE 1 = 1 --
            -- AND     ds >= 20230301 --<< Controller partition - Starting point, always >= 20230301, do not change
            AND ds >= TO_CHAR(
                DATEADD(TO_DATE($ { bizdate }, 'yyyymmdd'), 0, 'dd'),
                'yyyymmdd'
            ) --<< Controller partition - Daily operation
            -- AND     ds >= TO_CHAR(DATEADD(TO_DATE(${bizdate},'yyyymmdd'),-1110,'dd'),'yyyymmdd') --<< Controller partition - Maxout life cycle *Test before take action*
            AND venture = 'VN'
            AND TOLOWER(source) NOT IN ('lazada om') --<< Controller
            -- AND     traffic_attr_model IN ('First_Touch') --<< Active when need attr_model IN ('ft_1d_np')
            AND traffic_attr_model IN ('Any_Touch')
            AND utdid IS NOT NULL
    )
GROUP BY source,
    sub_source,
    channel,
    bucket,
    sub_channel,
    campaign,
    utdid,
    ds
ORDER BY ds,
    source,
    sub_source,
    channel,
    bucket,
    sub_channel,
    campaign;
--@@ Input = lazada_analyst_dev.loutruong_ppc_spend_di
--@@ Input = lazada_analyst_dev.loutruong_rta_spend_di
--@@ Input = lazada_analyst_dev.loutruong_affiliate_spend_di
--@@ Input = lazada_analyst_dev.loutruong_trf_touch_app_di
--@@ Input = lazada_analyst_dev.loutruong_trn_order_app_di
--@@ Output = lazada_analyst_dev.loutruong_price_model
-- DROP TABLE IF EXISTS lazada_analyst_dev.loutruong_price_model
-- ;
-- CREATE TABLE IF NOT EXISTS lazada_analyst_dev.loutruong_price_model
-- (
--     source             STRING COMMENT 'lazada om with funding_type = om'
--     ,sub_source        STRING COMMENT 'affiliate, rta, ppc'
--     ,channel           STRING COMMENT 'affiliate, rta, social, sem, display, apple search, tiktok, arm'
--     ,bucket            STRING COMMENT 'install, non_install'
--     ,sub_channel       STRING COMMENT 'channel - xxx'
--     ,campaign          STRING COMMENT 'campaign'
--     ,price_model_full  STRING COMMENT 'order_cnt, touch_cnt'
--     ,price_model_short STRING COMMENT 'cpo, cpt'
--     ,spend_all_device  DOUBLE COMMENT 'All device'
--     ,performance_app   DOUBLE COMMENT 'App only'
--     ,price_app         DOUBLE COMMENT 'The value of cpo_app & cpt_app'
-- )
-- COMMENT 'Table of VN pricing model for each paid channel'
-- PARTITIONED BY 
-- (
--     ds                 STRING
-- )
-- LIFECYCLE 3600
-- ;
INSERT OVERWRITE TABLE lazada_analyst_dev.loutruong_price_model PARTITION (ds)
SELECT source,
    sub_source,
    channel,
    bucket,
    sub_channel,
    campaign,
    price_model_full,
    price_model_short,
    spend AS spend_all_device,
    performance_app,
    COALESCE(spend / performance_app, 0) AS price_app,
    ds
FROM (
        SELECT ds,
            source,
            sub_source,
            channel,
            bucket,
            sub_channel,
            campaign,
            price_model_full,
            CASE
                WHEN TOLOWER(price_model_full) IN ('touch_app_cnt') THEN 'cpt_app'
                ELSE 'cpo_app'
            END AS price_model_short,
            COALESCE(SUM(spend), 0) AS spend,
            COALESCE(SUM(performance_app), 0) AS performance_app
        FROM (
                SELECT ds,
                    source,
                    sub_source,
                    channel,
                    bucket,
                    sub_channel,
                    campaign,
                    price_model_full,
                    COALESCE(SUM(spend), 0) AS spend,
                    0 AS performance_app
                FROM (
                        SELECT ds,
                            source,
                            sub_source,
                            channel,
                            bucket,
                            sub_channel,
                            campaign,
                            'touch_app_cnt' AS price_model_full,
                            spend
                        FROM lazada_analyst_dev.loutruong_ppc_spend_di
                        UNION ALL
                        --<< Break point spend all device
                        SELECT ds,
                            source,
                            sub_source,
                            channel,
                            bucket,
                            sub_channel,
                            campaign,
                            'touch_app_cnt' AS price_model_full,
                            spend
                        FROM lazada_analyst_dev.loutruong_rta_spend_di
                        UNION ALL
                        --<< Break point spend all device
                        SELECT ds,
                            source,
                            sub_source,
                            channel,
                            bucket,
                            sub_channel,
                            campaign,
                            'order_app_cnt' AS price_model_full,
                            spend
                        FROM lazada_analyst_dev.loutruong_affiliate_spend_di
                    )
                GROUP BY ds,
                    source,
                    sub_source,
                    channel,
                    bucket,
                    sub_channel,
                    campaign,
                    price_model_full
                UNION ALL
                --<< BIG BREAK POINT
                SELECT ds,
                    source,
                    sub_source,
                    channel,
                    bucket,
                    sub_channel,
                    campaign,
                    price_model_full,
                    0 AS spend,
                    performance_app
                FROM (
                        SELECT ds,
                            source,
                            sub_source,
                            COALESCE(channel, 'all') AS channel,
                            COALESCE(bucket, 'all') AS bucket,
                            COALESCE(sub_channel, 'all') AS sub_channel,
                            COALESCE(campaign, 'all') AS campaign,
                            COALESCE(SUM(touch_app_cnt), 0) AS touch_app_cnt,
                            COALESCE(SUM(order_app_cnt), 0) AS order_app_cnt
                        FROM (
                                SELECT ds,
                                    source,
                                    sub_source,
                                    channel,
                                    bucket,
                                    sub_channel,
                                    campaign,
                                    COUNT(DISTINCT utdid) AS touch_app_cnt,
                                    0 AS order_app_cnt
                                FROM lazada_analyst_dev.loutruong_trf_touch_app_di
                                WHERE 1 = 1
                                    AND TOLOWER(source) IN ('lazada om') --<< Select only lazada om
                                    AND TOLOWER(sub_source) NOT IN ('affiliate') --<< Separate the affiliate's touch
                                GROUP BY ds,
                                    source,
                                    sub_source,
                                    channel,
                                    bucket,
                                    sub_channel,
                                    campaign,
                                    order_app_cnt
                                UNION ALL
                                --<< Break point
                                SELECT ds,
                                    source,
                                    sub_source,
                                    channel,
                                    bucket,
                                    sub_channel,
                                    campaign,
                                    0 AS touch_app_cnt,
                                    SUM(order_app_cnt) AS order_app_cnt
                                FROM lazada_analyst_dev.loutruong_trn_order_app_di
                                GROUP BY ds,
                                    source,
                                    sub_source,
                                    channel,
                                    bucket,
                                    sub_channel,
                                    campaign,
                                    touch_app_cnt
                            )
                        GROUP BY GROUPING SETS (
                                (
                                    ds,
                                    source,
                                    sub_source,
                                    channel,
                                    bucket,
                                    sub_channel,
                                    campaign
                                ),
                                (
                                    ds,
                                    source,
                                    sub_source,
                                    channel,
                                    bucket,
                                    sub_channel
                                ),
                                (ds, source, sub_source, channel, bucket),
                                (ds, source, sub_source, channel),
                                (ds, source, sub_source)
                            )
                    ) UNPIVOT (
                        performance_app FOR price_model_full IN (touch_app_cnt, order_app_cnt)
                    )
                WHERE 1 = 1
                    AND performance_app <> 0
            )
        GROUP BY ds,
            source,
            sub_source,
            channel,
            bucket,
            sub_channel,
            campaign,
            price_model_full,
            CASE
                WHEN TOLOWER(price_model_full) IN ('touch_app_cnt') THEN 'cpt_app'
                ELSE 'cpo_app'
            END
    );
--@@ Input = lazada_analyst_dev.loutruong_price_model
--@@ Input = lazada_analyst_dev.loutruong_trf_touch_app_di
--@@ Input = lazada_analyst_dev.loutruong_trn_order_app_di
--@@ Input = lazada_analyst_dev.loutruong_utdid_clm_di
--@@ Input = lazada_analyst_dev.datx_ug_traffic_funnel_d_1d
--@@ Input = lazada_analyst_dev.lazada_analyst.datx_campaign_list
--@@ Output = lazada_analyst_dev.loutruong_om_clm_spend_di
-- DROP TABLE IF EXISTS lazada_analyst_dev.loutruong_om_clm_spend_di
-- ;
-- CREATE TABLE IF NOT EXISTS lazada_analyst_dev.loutruong_om_clm_spend_di
-- (
--     camp_type             STRING
--     ,master_campaign_name STRING
--     ,period               STRING
--     ,source               STRING COMMENT 'lazada om with funding_type = om'
--     ,sub_source           STRING COMMENT 'affiliate, rta, ppc'
--     ,channel              STRING COMMENT 'affiliate, rta, social, sem, display, apple search, tiktok, arm'
--     ,bucket               STRING COMMENT 'install, non_install'
--     ,sub_channel          STRING COMMENT 'channel - xxx'
--     ,campaign             STRING COMMENT 'campaign'
--     ,clm_level            STRING COMMENT 'd segment as business definition'
--     ,spend                DOUBLE
--     ,dau_app              DOUBLE
-- )
-- COMMENT 'Table contains the spending of VN paid channel by d segment up-to campaign level'
-- PARTITIONED BY 
-- (
--     ds                    STRING
-- )
-- LIFECYCLE 3600
-- ;
INSERT OVERWRITE TABLE lazada_analyst_dev.loutruong_om_clm_spend_di PARTITION (ds)
SELECT DISTINCT CASE
        WHEN TOLOWER(t3.camp_type) IS NULL THEN 'bau'
        ELSE TOLOWER(t3.camp_type)
    END AS camp_type,
    CASE
        WHEN TOLOWER(t3.master_campaign_name) IS NULL THEN 'bau'
        ELSE TOLOWER(t3.master_campaign_name)
    END AS master_campaign_name,
    CASE
        WHEN TOLOWER(t3.period) IS NULL THEN 'bau'
        ELSE TOLOWER(t3.period)
    END AS period,
    t1.source AS source,
    t1.sub_source AS sub_source,
    t1.channel AS channel,
    t1.bucket AS bucket,
    t1.sub_channel AS sub_channel,
    t1.campaign AS campaign,
    t1.clm_level AS clm_level,
    t1.spend AS spend,
    t2.dau_app AS dau_app,
    t1.ds AS ds
FROM (
        SELECT ds,
            source,
            COALESCE(sub_source, 'all') AS sub_source,
            COALESCE(channel, 'all') AS channel,
            COALESCE(bucket, 'all') AS bucket,
            COALESCE(sub_channel, 'all') AS sub_channel,
            COALESCE(campaign, 'all') AS campaign,
            clm_level,
            COALESCE(SUM(spend), 0) AS spend
        FROM (
                SELECT t1.ds AS ds --<< clm spend ppc
,
                    t1.source AS source,
                    t1.sub_source AS sub_source,
                    t1.channel AS channel,
                    t1.bucket AS bucket,
                    t1.sub_channel AS sub_channel,
                    t1.campaign AS campaign,
                    CASE
                        WHEN t3.clm_level IS NULL THEN 'Guest'
                        ELSE t3.clm_level
                    END AS clm_level,
                    COALESCE(SUM(t2.price_app), 0) AS spend
                FROM (
                        SELECT ds --<< Performance app ppc
,
                            source,
                            sub_source,
                            channel,
                            bucket,
                            sub_channel,
                            campaign,
                            utdid
                        FROM lazada_analyst_dev.loutruong_trf_touch_app_di
                        WHERE 1 = 1
                            AND TOLOWER(source) IN ('lazada om')
                            AND TOLOWER(sub_source) IN ('ppc')
                        GROUP BY ds,
                            source,
                            sub_source,
                            channel,
                            bucket,
                            sub_channel,
                            campaign,
                            utdid
                    ) AS t1
                    INNER JOIN (
                        SELECT ds --<< Spend ppc
,
                            source,
                            sub_source,
                            channel,
                            bucket,
                            sub_channel,
                            campaign,
                            price_app
                        FROM lazada_analyst_dev.loutruong_price_model
                        WHERE 1 = 1
                            AND TOLOWER(sub_source) IN ('ppc')
                            AND TOLOWER(campaign) NOT IN ('all')
                        GROUP BY ds,
                            source,
                            sub_source,
                            channel,
                            bucket,
                            sub_channel,
                            campaign,
                            price_app
                    ) AS t2 ON t1.ds = t2.ds
                    AND t1.sub_channel = t2.sub_channel
                    AND t1.campaign = t2.campaign
                    LEFT JOIN (
                        SELECT ds --<< Clm app ppc
,
                            utdid,
                            clm_level
                        FROM lazada_analyst_dev.loutruong_utdid_clm_di
                        GROUP BY ds,
                            utdid,
                            clm_level
                    ) AS t3 ON t1.ds = t3.ds
                    AND t1.utdid = t3.utdid
                GROUP BY t1.ds,
                    t1.source,
                    t1.sub_source,
                    t1.channel,
                    t1.bucket,
                    t1.sub_channel,
                    t1.campaign,
                    CASE
                        WHEN t3.clm_level IS NULL THEN 'Guest'
                        ELSE t3.clm_level
                    END
                UNION ALL
                --<< Break point
                SELECT t1.ds AS ds --<< clm spend rta
,
                    t1.source AS source,
                    t1.sub_source AS sub_source,
                    t1.channel AS channel,
                    t1.bucket AS bucket,
                    t1.sub_channel AS sub_channel,
                    t1.campaign AS campaign,
                    CASE
                        WHEN t3.clm_level IS NULL THEN 'Guest'
                        ELSE t3.clm_level
                    END AS clm_level,
                    COALESCE(SUM(t2.price_app), 0) AS spend
                FROM (
                        SELECT ds --<< Performance app rta
,
                            source,
                            sub_source,
                            channel,
                            bucket,
                            sub_channel,
                            campaign,
                            utdid
                        FROM lazada_analyst_dev.loutruong_trf_touch_app_di
                        WHERE 1 = 1
                            AND TOLOWER(source) IN ('lazada om')
                            AND TOLOWER(sub_source) IN ('rta')
                        GROUP BY ds,
                            source,
                            sub_source,
                            channel,
                            bucket,
                            sub_channel,
                            campaign,
                            utdid
                    ) AS t1
                    INNER JOIN (
                        SELECT ds --<< Spend rta
,
                            source,
                            sub_source,
                            channel,
                            bucket,
                            sub_channel,
                            campaign,
                            price_app
                        FROM lazada_analyst_dev.loutruong_price_model
                        WHERE 1 = 1
                            AND TOLOWER(sub_source) IN ('rta')
                            AND TOLOWER(campaign) NOT IN ('all')
                        GROUP BY ds,
                            source,
                            sub_source,
                            channel,
                            bucket,
                            sub_channel,
                            campaign,
                            price_app
                    ) AS t2 ON t1.ds = t2.ds
                    AND t1.sub_channel = t2.sub_channel
                    AND t1.campaign = t2.campaign
                    LEFT JOIN (
                        SELECT ds --<< Clm app rta
,
                            utdid,
                            clm_level
                        FROM lazada_analyst_dev.loutruong_utdid_clm_di
                        GROUP BY ds,
                            utdid,
                            clm_level
                    ) AS t3 ON t1.ds = t3.ds
                    AND t1.utdid = t3.utdid
                GROUP BY t1.ds,
                    t1.source,
                    t1.sub_source,
                    t1.channel,
                    t1.bucket,
                    t1.sub_channel,
                    t1.campaign,
                    CASE
                        WHEN t3.clm_level IS NULL THEN 'Guest'
                        ELSE t3.clm_level
                    END
                UNION ALL
                --<< Break point
                SELECT t1.ds AS ds --<< clm spend affiliate
,
                    t1.source AS source,
                    t1.sub_source AS sub_source,
                    t1.channel AS channel,
                    t1.bucket AS bucket,
                    t1.sub_channel AS sub_channel,
                    t1.campaign AS campaign,
                    CASE
                        WHEN t3.clm_level IS NULL THEN 'Guest'
                        ELSE t3.clm_level
                    END AS clm_level,
                    COALESCE(SUM(t1.order_app_cnt * t2.price_app), 0) AS spend
                FROM (
                        SELECT ds --<< Performance app affiliate
,
                            source,
                            sub_source,
                            channel,
                            bucket,
                            sub_channel,
                            campaign,
                            utdid,
                            buyer_id,
                            check_out_id,
                            order_app_cnt
                        FROM lazada_analyst_dev.loutruong_trn_order_app_di
                        GROUP BY ds,
                            source,
                            sub_source,
                            channel,
                            bucket,
                            sub_channel,
                            campaign,
                            utdid,
                            buyer_id,
                            check_out_id,
                            order_app_cnt
                    ) AS t1
                    INNER JOIN (
                        SELECT ds --<< Spend affiliate
,
                            source,
                            sub_source,
                            channel,
                            bucket,
                            sub_channel,
                            campaign,
                            price_app
                        FROM lazada_analyst_dev.loutruong_price_model
                        WHERE 1 = 1
                            AND TOLOWER(sub_source) IN ('affiliate')
                            AND TOLOWER(campaign) NOT IN ('all')
                        GROUP BY ds,
                            source,
                            sub_source,
                            channel,
                            bucket,
                            sub_channel,
                            campaign,
                            price_app
                    ) AS t2 ON t1.ds = t2.ds
                    AND t1.sub_channel = t2.sub_channel
                    AND t1.campaign = t2.campaign
                    LEFT JOIN (
                        SELECT ds --<< Clm app affiliate
,
                            utdid,
                            clm_level
                        FROM lazada_analyst_dev.loutruong_utdid_clm_di
                        GROUP BY ds,
                            utdid,
                            clm_level
                    ) AS t3 ON t1.ds = t3.ds
                    AND t1.utdid = t3.utdid
                GROUP BY t1.ds,
                    t1.source,
                    t1.sub_source,
                    t1.channel,
                    t1.bucket,
                    t1.sub_channel,
                    t1.campaign,
                    CASE
                        WHEN t3.clm_level IS NULL THEN 'Guest'
                        ELSE t3.clm_level
                    END
            )
        GROUP BY GROUPING SETS (
                (
                    ds,
                    source,
                    sub_source,
                    channel,
                    bucket,
                    sub_channel,
                    campaign,
                    clm_level
                ),
                (
                    ds,
                    source,
                    sub_source,
                    channel,
                    bucket,
                    sub_channel,
                    clm_level
                ),
                (
                    ds,
                    source,
                    sub_source,
                    channel,
                    bucket,
                    clm_level
                ),
                (ds, source, sub_source, channel, clm_level),
                (ds, source, sub_source, clm_level),
                (ds, source, clm_level)
            )
    ) AS t1
    INNER JOIN (
        SELECT DISTINCT ds,
            CASE
                WHEN clm_level IN ('NA') THEN 'Guest'
                ELSE clm_level
            END AS clm_level,
            app_uv_cnt AS dau_app
        FROM lazada_analyst_dev.datx_ug_traffic_funnel_d_1d
        WHERE 1 = 1
            AND ds >= 20230301 --<< Always >= 20230301, Do not change
            AND TOLOWER(source) IN ('all')
            AND TOLOWER(sub_source) IN ('all')
            AND clm_level NOT IN ('All')
    ) AS t2 ON t1.ds = t2.ds
    AND t1.clm_level = t2.clm_level
    LEFT JOIN (
        SELECT ds,
            camp_type,
            master_campaign_name,
            period
        FROM lazada_analyst.datx_campaign_list
        WHERE 1 = 1
            AND ds >= 20230301 --<< Always >= 20230301, Do not change
        GROUP BY ds,
            camp_type,
            master_campaign_name,
            period
    ) AS t3 ON t1.ds = t3.ds;
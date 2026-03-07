-- MaxCompute SQL 
-- ********************************************************************--
-- author:Truong, Van Thanh
-- create time:2024-03-25 15:08:00
-- ********************************************************************--
--@@ Input = lazada_cdm.dim_lzd_mkt_pid_info_df
--@@ Input = lazada_analyst.hg_affiliate_pc25_monthly
--@@ Input = lazada_ads_data.ads_lzd_cps_partner_info_df
--@@ Input = lazada_analyst_dev.hg_aff_plm_mixed
--@@ Input = lazada_analyst.loutruong_aff_mem_id_excl
--@@ Input = lazada_ads.dim_lzd_mkt_aff_blacklist_df
--@@ Input = lazada_analyst_dev.loutruong_trd_core_fulfill_ext_trf_di
--@@ Input = lazada_analyst_dev.loutruong_aff_console_fulfill_di
--@@ Input = lazada_analyst_dev.loutruong_aff_console_fulfill_df
-- DROP TABLE IF EXISTS lazada_analyst_dev.tmp_loutruong_aff_console_fulfill_di
-- ;
-- CREATE TABLE IF NOT EXISTS lazada_analyst_dev.tmp_loutruong_aff_console_fulfill_di_2
-- LIFECYCLE 180 AS
-- SELECT  *
-- FROM    lazada_analyst_dev.loutruong_aff_console_fulfill_di
-- WHERE   1 = 1
-- AND     ds BETWEEN '20240501' AND '20240723' --<< Change
-- ;
-- DROP TABLE IF EXISTS lazada_analyst_dev.tmp_hg_aff_positive_pc25_monthly
-- ;
-- CREATE TABLE IF NOT EXISTS lazada_analyst_dev.tmp_hg_aff_positive_pc25_monthly
-- LIFECYCLE 180 AS
-- SELECT  month
--         ,seller_id
-- FROM    lazada_analyst_dev.hg_aff_positive_pc25_monthly
-- WHERE   1 = 1
-- AND     ds = 20240724 --<< Change
-- AND     month IN (TO_CHAR(DATEADD(DATEADD(GETDATE(),-1,'dd'),-1,'mm'),'yyyymm'))
-- ;
SELECT data_range,
    data_duration,
    member_id,
    member_name,
    CASE
        WHEN order_accumulate >= 9001 THEN 'Strategic'
        WHEN order_accumulate BETWEEN 901 AND 9000 THEN 'Strategic'
        WHEN order_accumulate BETWEEN 91 AND 900 THEN 'Non-strategic'
        WHEN order_accumulate BETWEEN 61 AND 90 THEN 'Non-strategic'
        WHEN order_accumulate BETWEEN 1 AND 60 THEN 'Non-strategic'
        WHEN order_accumulate = 0 THEN 'Non-strategic'
        ELSE 'Others'
    END AS current_base_cms_tier,
    CASE
        WHEN order_accumulate >= 9001 THEN 'B2C-Retention' --<< 3k/month
        WHEN order_accumulate BETWEEN 901 AND 9000 THEN 'B2C-Retention' --<< 3k/month
        WHEN order_accumulate BETWEEN 91 AND 900 THEN 'B2C-Incubation'
        WHEN order_accumulate BETWEEN 61 AND 90 THEN 'B2C-Incubation'
        WHEN order_accumulate BETWEEN 1 AND 60 THEN 'B2C-Incubation'
        WHEN order_accumulate = 0 THEN 'B2C-Acquisition'
        ELSE 'Others'
    END AS new_team_manage,
    CASE
        WHEN order_accumulate >= 9001 THEN '1.KA'
        WHEN order_accumulate BETWEEN 901 AND 9000 THEN '2.SM'
        WHEN order_accumulate BETWEEN 91 AND 900 THEN '3.LT'
        WHEN order_accumulate BETWEEN 61 AND 90 THEN '5.Hipo'
        WHEN order_accumulate BETWEEN 1 AND 60 THEN '6.Mass'
        WHEN order_accumulate = 0 THEN '8.Inactive'
        ELSE 'Others'
    END AS new_tier_lv2,
    '' AS new_pic,
    old_team_manage,
    old_tier_lv2,
    old_pic,
    gmv_accumulate,
    gmv_accumulate_no_fr,
    gmv_accumulate_fr,
    gmv_accumulate_positive_pc25,
    gmv_accumulate_negative_pc25,
    gmv_average,
    gmv_average_no_fr,
    gmv_average_fr,
    gmv_average_positive_pc25,
    gmv_average_negative_pc25,
    order_accumulate,
    order_accumulate_no_fr,
    order_accumulate_fr,
    order_accumulate_positive_pc25,
    order_accumulate_negative_pc25,
    order_average,
    order_average_no_fr,
    order_average_fr,
    order_average_positive_pc25,
    order_average_negative_pc25
FROM (
        SELECT CONCAT(
                'Data range:',
                ' ',
                ds_start,
                ' ',
                '-->>',
                ' ',
                ds_end
            ) AS data_range,
            data_duration,
            member_id,
            member_name,
            old_team_manage,
            old_tier_lv2,
            old_pic,
            SUM(gmv_usd) AS gmv_accumulate,
            SUM(
                CASE
                    WHEN is_fraud = 0 THEN gmv_usd
                    ELSE 0
                END
            ) AS gmv_accumulate_no_fr,
            SUM(
                CASE
                    WHEN is_fraud = 1 THEN gmv_usd
                    ELSE 0
                END
            ) AS gmv_accumulate_fr,
            SUM(
                CASE
                    WHEN is_slr_negative = 0 THEN gmv_usd
                    ELSE 0
                END
            ) AS gmv_accumulate_positive_pc25,
            SUM(
                CASE
                    WHEN is_slr_negative = 1 THEN gmv_usd
                    ELSE 0
                END
            ) AS gmv_accumulate_negative_pc25,
            SUM(gmv_usd) / data_duration AS gmv_average,
            SUM(
                CASE
                    WHEN is_fraud = 0 THEN gmv_usd
                    ELSE 0
                END
            ) / data_duration AS gmv_average_no_fr,
            SUM(
                CASE
                    WHEN is_fraud = 1 THEN gmv_usd
                    ELSE 0
                END
            ) / data_duration AS gmv_average_fr,
            SUM(
                CASE
                    WHEN is_slr_negative = 0 THEN gmv_usd
                    ELSE 0
                END
            ) / data_duration AS gmv_average_positive_pc25,
            SUM(
                CASE
                    WHEN is_slr_negative = 1 THEN gmv_usd
                    ELSE 0
                END
            ) / data_duration AS gmv_average_negative_pc25,
            COUNT(DISTINCT order_id) AS order_accumulate,
            COUNT(
                DISTINCT CASE
                    WHEN is_fraud = 0 THEN order_id
                    ELSE NULL
                END
            ) AS order_accumulate_no_fr,
            COUNT(
                DISTINCT CASE
                    WHEN is_fraud = 1 THEN order_id
                    ELSE NULL
                END
            ) AS order_accumulate_fr,
            COUNT(
                DISTINCT CASE
                    WHEN is_slr_negative = 0 THEN order_id
                    ELSE NULL
                END
            ) AS order_accumulate_positive_pc25,
            COUNT(
                DISTINCT CASE
                    WHEN is_slr_negative = 1 THEN order_id
                    ELSE NULL
                END
            ) AS order_accumulate_negative_pc25,
            COUNT(DISTINCT order_id) / data_duration AS order_average,
            COUNT(
                DISTINCT CASE
                    WHEN is_fraud = 0 THEN order_id
                    ELSE NULL
                END
            ) / data_duration AS order_average_no_fr,
            COUNT(
                DISTINCT CASE
                    WHEN is_fraud = 1 THEN order_id
                    ELSE NULL
                END
            ) / data_duration AS order_average_fr,
            COUNT(
                DISTINCT CASE
                    WHEN is_slr_negative = 0 THEN order_id
                    ELSE NULL
                END
            ) / data_duration AS order_average_positive_pc25,
            COUNT(
                DISTINCT CASE
                    WHEN is_slr_negative = 1 THEN order_id
                    ELSE NULL
                END
            ) / data_duration AS order_average_negative_pc25
        FROM (
                SELECT t1.member_id AS member_id,
                    t1.member_name AS member_name,
                    t5.team_manage AS old_team_manage,
                    t5.member_plm_local_lv2 AS old_tier_lv2,
                    t5.member_pic_local AS old_pic,
                    t2.sales_order_item_id AS sales_order_item_id,
                    t2.order_id AS order_id,
                    COALESCE(t2.gmv_usd, 0) AS gmv_usd,
                    COALESCE(t3.is_fraud, 0) AS is_fraud,
                    t2.seller_id AS seller_id,
                    CASE
                        WHEN t4.seller_id IS NULL THEN 1
                        ELSE 0
                    END AS is_slr_negative,
                    t3.adjust_type AS adjust_type,
                    t3.status AS status,
                    COUNT(DISTINCT t2.ds) OVER () AS data_duration,
                    MIN(t2.ds) OVER () AS ds_start,
                    MAX(t2.ds) OVER () AS ds_end
                FROM (
                        SELECT member_id,
                            MAX(member_name) AS member_name
                        FROM lazada_cdm.dim_lzd_mkt_pid_info_df
                        WHERE 1 = 1
                            AND ds = 20240724 --<< Change
                            AND venture = 'VN'
                            AND TOLOWER(channel) IN ('cps affiliate') --<< All pid belongs to affiliate
                            AND member_id NOT IN (
                                SELECT member_id
                                FROM lazada_analyst.loutruong_aff_mem_id_excl
                            )
                        GROUP BY member_id
                    ) AS t1
                    LEFT JOIN (
                        SELECT ds,
                            member_id,
                            gmv_usd,
                            order_id,
                            sales_order_item_id,
                            seller_id
                        FROM lazada_analyst_dev.loutruong_trd_core_fulfill_ext_trf_di
                        WHERE 1 = 1
                            AND ds BETWEEN '20240501' AND '20240723' --<< Change
                            AND TOLOWER(funding_bucket) IN ('lazada om')
                            AND TOLOWER(funding_type) IN ('om', 'ams')
                            AND TOLOWER(sub_channel) IN ('cps affiliate')
                    ) AS t2 ON t1.member_id = t2.member_id
                    LEFT JOIN (
                        SELECT sales_order_item_id,
                            is_fraud,
                            adjust_type,
                            status
                        FROM lazada_analyst_dev.tmp_loutruong_aff_console_fulfill_di
                    ) AS t3 ON t2.sales_order_item_id = t3.sales_order_item_id
                    LEFT JOIN (
                        SELECT seller_id
                        FROM lazada_analyst_dev.tmp_hg_aff_positive_pc25_monthly
                    ) AS t4 ON t2.seller_id = t4.seller_id
                    LEFT JOIN (
                        SELECT team_manage,
                            member_plm_local_lv1,
                            member_plm_local_lv2,
                            member_pic_local,
                            pc25_tier,
                            member_id
                        FROM lazada_analyst.loutruong_aff_mem_id_offline_info
                        WHERE 1 = 1
                            AND month_apply = 202407
                    ) AS t5 ON t1.member_id = t5.member_id
                GROUP BY t1.member_id,
                    t1.member_name,
                    t5.team_manage,
                    t5.member_plm_local_lv2,
                    t5.member_pic_local,
                    t2.sales_order_item_id,
                    t2.order_id,
                    COALESCE(t2.gmv_usd, 0),
                    COALESCE(t3.is_fraud, 0),
                    t2.seller_id,
                    CASE
                        WHEN t4.seller_id IS NULL THEN 1
                        ELSE 0
                    END,
                    t3.adjust_type,
                    t3.status,
                    t2.ds
            )
        GROUP BY CONCAT(
                'Data range:',
                ' ',
                ds_start,
                ' ',
                '-->>',
                ' ',
                ds_end
            ),
            data_duration,
            member_id,
            member_name,
            old_team_manage,
            old_tier_lv2,
            old_pic
    )
ORDER BY gmv_accumulate DESC,
    gmv_accumulate_no_fr DESC;
-- MaxCompute SQL 
-- ********************************************************************--
-- author:Truong, Van Thanh
-- create time:2024-03-24 11:09:37
-- ********************************************************************--
--@@ Input = lazada_cdm.dws_lzd_mkt_trn_uam_di
--@@ Input = lazada_cdm.dwd_lzd_trd_core_fulfill_di
--@@ Input = lazada_analyst.hg_affiliate_pc25_monthly
--@@ Input = lazada_analyst_dev.tmp_loutruong_dim_slr_brd_sku_cms
SELECT *,
    ROW_NUMBER() OVER (
        ORDER BY gmv_usd DESC
    ) AS top_100_scale,
    CASE
        WHEN aff_commission > 0 THEN ROW_NUMBER() OVER (
            PARTITION BY aff_commission > 0
            ORDER BY gmv_usd DESC
        )
        ELSE 0
    END AS top_100_scale_and_cms
FROM (
        SELECT t1.seller_id AS seller_id,
            t4.seller_short_code AS seller_short_code,
            t4.seller_name AS seller_name,
            t3.shop_url AS seller_link,
            t4.business_type AS bu_lv1,
            t4.business_type_level2 AS bu_lv2,
            t4.industry_name AS cluster,
            t4.regional_category1_name AS cat_lv1,
            COALESCE(t5.ssc_commission, 0) AS ssc_commission,
            COALESCE(t5.aff_commission, 0) AS aff_commission,
            COUNT(DISTINCT t1.order_id) AS order_cnt,
            SUM(
                COALESCE(t1.actual_gmv, 0) * COALESCE(t1.exchange_rate, 0)
            ) AS gmv_usd
        FROM (
                SELECT *
                FROM lazada_cdm.dwd_lzd_trd_core_fulfill_di
                WHERE 1 = 1
                    AND ds BETWEEN '20240722' AND '20240728' --<< Change
                    AND venture = 'VN'
                    AND is_revenue = 1
                    AND COALESCE(business_application, 'LZD') IN ('LZD,ZAL', 'LZD')
                    AND seller_id NOT IN ('1000014049') --<< lazada supermarket
            ) AS t1
            INNER JOIN (
                SELECT seller_id
                FROM lazada_analyst_dev.hg_aff_positive_pc25_monthly
                WHERE 1 = 1
                    AND ds = MAX_PT(
                        'lazada_analyst_dev.hg_aff_positive_pc25_monthly'
                    )
                    AND month IN (
                        TO_CHAR(
                            DATEADD(DATEADD(GETDATE(), -1, 'dd'), -1, 'mm'),
                            'yyyymm'
                        )
                    )
            ) AS t2 ON t1.seller_id = t2.seller_id
            LEFT JOIN (
                SELECT seller_id,
                    shop_url
                FROM lazada_cdm.dim_lzd_slr_seller_vn
                WHERE 1 = 1
                    AND ds = MAX_PT('lazada_cdm.dim_lzd_slr_seller')
                    AND venture = 'VN'
            ) AS t3 ON t1.seller_id = t3.seller_id
            LEFT JOIN (
                SELECT seller_id,
                    seller_short_code,
                    seller_name,
                    business_type,
                    business_type_level2,
                    industry_name,
                    regional_category1_name
                FROM lazada_cdm.dim_lzd_slr_seller_extra_vn
                WHERE 1 = 1
                    AND ds = MAX_PT('lazada_cdm.dim_lzd_slr_seller_extra')
            ) AS t4 ON t1.seller_id = t4.seller_id
            LEFT JOIN (
                SELECT seller_id,
                    MIN(seller_ssc_commission) AS ssc_commission,
                    MIN(seller_aff_commission) AS aff_commission
                FROM lazada_analyst_dev.loutruong_dim_slr_brd_sku_cms
                WHERE 1 = 1
                    AND ds = MAX_PT(
                        'lazada_analyst_dev.loutruong_dim_slr_brd_sku_cms'
                    )
                GROUP BY seller_id
            ) AS t5 ON t1.seller_id = t5.seller_id
        GROUP BY t1.seller_id,
            t4.seller_short_code,
            t4.seller_name,
            t3.shop_url,
            t4.business_type,
            t4.business_type_level2,
            t4.industry_name,
            t4.regional_category1_name,
            COALESCE(t5.ssc_commission, 0),
            COALESCE(t5.aff_commission, 0)
    );
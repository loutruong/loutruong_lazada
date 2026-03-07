-- MaxCompute SQL 
-- ********************************************************************--
-- author:Truong, Van Thanh
-- create time:2024-02-21 12:48:01
-- ********************************************************************--
--@@ Input = lazada_cdm.dim_lzd_pro_collectibles
--@@ Input = lazada_cdm.dim_lzd_pro_voucher_rule
--@@ Input = lazada_analyst.loutruong_aff_vc_mapping
--@@ Input = lazada_cdm.dwd_lzd_pro_promotion_item_di
--@@ Input = lazada_cdm.dwd_lzd_trd_core_df
SELECT t1.ds_start_collect AS ds_start_collect,
    t1.ds_end_collect AS ds_end_collect,
    t1.ds_start_redeem AS ds_start_redeem,
    t1.ds_end_redeem AS ds_end_redeem,
    t1.promo_id AS promo_id,
    t1.promo_name AS promo_name,
    t1.promo_desc AS promo_desc,
    t1.promo_type AS promo_type,
CASE
        WHEN t1.promo_type IN ('shippingFeeCoupon')
        AND (
            TOLOWER(t1.promo_name) LIKE '%platform_clm_seller boost program%'
            OR TOLOWER(t1.promo_desc) LIKE '%platform_clm_seller boost program%'
        ) THEN 'FSM_Flag'
        WHEN t1.promo_type IN ('shippingFeeCoupon') THEN 'FS_Platform'
        WHEN t1.promo_type IN ('purchaseIncentive', 'lpiCoupon') THEN 'LPI'
        WHEN t1.promo_type IN ('cashbackBalance') THEN 'EDC_Redeem'
        WHEN t1.promo_type IN ('cashbackCoupon') THEN 'EDC_Earn'
        WHEN t1.promo_type IN ('categoryCoupon', 'collectibleCoupon') THEN CASE
            WHEN TOLOWER(t1.promo_name) LIKE '%clm_clm%'
            OR TOLOWER(t1.promo_desc) LIKE '%clm_clm%' THEN 'PDV_CLM'
            WHEN TOLOWER(t1.promo_name) LIKE '%platform_clm_mega_%'
            OR TOLOWER(t1.promo_desc) LIKE '%platform_clm_mega_%'
            OR TOLOWER(t1.promo_name) LIKE '%platform_clm_a+%'
            OR TOLOWER(t1.promo_desc) LIKE '%platform_clm_a+%'
            OR TOLOWER(t1.promo_name) LIKE '%platform_clm_bau order%'
            OR TOLOWER(t1.promo_desc) LIKE '%platform_clm_bau order%'
            OR TOLOWER(t1.promo_name) LIKE '%platform_clm_bau%'
            OR TOLOWER(t1.promo_desc) LIKE '%platform_clm_bau%'
            OR TOLOWER(t1.promo_name) LIKE '%win cart for free%'
            OR TOLOWER(t1.promo_desc) LIKE '%win cart for free%'
            OR TOLOWER(t1.promo_name) LIKE '%platform_clm_win%'
            OR TOLOWER(t1.promo_desc) LIKE '%platform_clm_win%'
            OR TOLOWER(t1.promo_name) LIKE '%lzdwcf%'
            OR TOLOWER(t1.promo_desc) LIKE '%lzdwcf%' THEN 'PDV_Platform_Mega'
            WHEN TOLOWER(t1.promo_name) LIKE '%top seller vouchers%'
            OR TOLOWER(t1.promo_desc) LIKE '%top seller vouchers%'
            OR TOLOWER(t1.promo_name) LIKE '%lazmall%'
            OR TOLOWER(t1.promo_desc) LIKE '%lazmall%'
            OR TOLOWER(t1.promo_name) LIKE '%long-tail booster%'
            OR TOLOWER(t1.promo_desc) LIKE '%long-tail booster%'
            OR TOLOWER(t1.promo_name) LIKE '%_fmcg%'
            OR TOLOWER(t1.promo_desc) LIKE '%_fmcg%'
            OR TOLOWER(t1.promo_name) LIKE '%tcf%'
            OR TOLOWER(t1.promo_desc) LIKE '%tcf%'
            OR TOLOWER(t1.promo_name) LIKE '%laztop%'
            OR TOLOWER(t1.promo_desc) LIKE '%laztop%'
            OR TOLOWER(t1.promo_name) LIKE '%lazbeauty%'
            OR TOLOWER(t1.promo_desc) LIKE '%lazbeauty%'
            OR TOLOWER(t1.promo_name) LIKE '%_fa%'
            OR TOLOWER(t1.promo_desc) LIKE '%_fa%'
            OR TOLOWER(t1.promo_name) LIKE '%_gm%'
            OR TOLOWER(t1.promo_desc) LIKE '%_gm%'
            OR TOLOWER(t1.promo_name) LIKE '%_hb%'
            OR TOLOWER(t1.promo_desc) LIKE '%_hb%'
            OR TOLOWER(t1.promo_name) LIKE '%_grmb%'
            OR TOLOWER(t1.promo_desc) LIKE '%_grmb%'
            OR TOLOWER(t1.promo_name) LIKE '%_gcmb%'
            OR TOLOWER(t1.promo_desc) LIKE '%_gcmb%'
            OR TOLOWER(t1.promo_name) LIKE '%_cgmb%'
            OR TOLOWER(t1.promo_desc) LIKE '%_cgmb%'
            OR TOLOWER(t1.promo_name) LIKE '%_el%'
            OR TOLOWER(t1.promo_desc) LIKE '%_el%'
            OR TOLOWER(t1.promo_name) LIKE '%_lazmall%'
            OR TOLOWER(t1.promo_desc) LIKE '%_lazmall%'
            OR TOLOWER(t1.promo_name) LIKE '%_bo%'
            OR TOLOWER(t1.promo_desc) LIKE '%_bo%' THEN 'PDV_Top_Seller'
            WHEN TOLOWER(t1.promo_name) LIKE '%voucher%plus%'
            OR TOLOWER(t1.promo_desc) LIKE '%voucher%plus%' THEN 'PDV_VCP'
            ELSE 'PDV_Others'
        END
        WHEN t1.promo_type IN ('mockedMillionSubsidy') THEN 'LazSubsidy'
        WHEN t1.promo_type IN ('voucherCoupon') THEN 'Unique_code'
    END AS promo_bucket,
CASE
        WHEN t1.promo_type IN ('shippingFeeCoupon')
        AND (
            TOLOWER(t1.promo_name) LIKE '%platform_clm_seller boost program%'
            OR TOLOWER(t1.promo_desc) LIKE '%platform_clm_seller boost program%'
        )
        AND (
            TOLOWER(t1.promo_name) LIKE '%affiliate%'
            OR TOLOWER(t1.promo_desc) LIKE '%affiliate%'
            OR TOLOWER(t1.promo_name) LIKE '%om_affiliate%'
            OR TOLOWER(t1.promo_desc) LIKE '%om_affiliate%'
        ) THEN 'FSM_Flag_Aff'
        WHEN t1.promo_type IN ('shippingFeeCoupon')
        AND (
            TOLOWER(t1.promo_name) LIKE '%affiliate%'
            OR TOLOWER(t1.promo_desc) LIKE '%affiliate%'
            OR TOLOWER(t1.promo_name) LIKE '%om_affiliate%'
            OR TOLOWER(t1.promo_desc) LIKE '%om_affiliate%'
        ) THEN 'FS_Platform_Aff'
        WHEN t1.promo_type IN ('purchaseIncentive')
        AND (
            TOLOWER(t1.promo_name) LIKE '%affiliate%'
            OR TOLOWER(t1.promo_desc) LIKE '%affiliate%'
            OR TOLOWER(t1.promo_name) LIKE '%om_affiliate%'
            OR TOLOWER(t1.promo_desc) LIKE '%om_affiliate%'
        ) THEN 'LPI_Aff'
        WHEN t1.promo_type IN ('cashbackBalance')
        AND (
            TOLOWER(t1.promo_name) LIKE '%affiliate%'
            OR TOLOWER(t1.promo_desc) LIKE '%affiliate%'
            OR TOLOWER(t1.promo_name) LIKE '%om_affiliate%'
            OR TOLOWER(t1.promo_desc) LIKE '%om_affiliate%'
        ) THEN 'EDC_Redeem_Aff'
        WHEN t1.promo_type IN ('cashbackCoupon')
        AND (
            TOLOWER(t1.promo_name) LIKE '%affiliate%'
            OR TOLOWER(t1.promo_desc) LIKE '%affiliate%'
            OR TOLOWER(t1.promo_name) LIKE '%om_affiliate%'
            OR TOLOWER(t1.promo_desc) LIKE '%om_affiliate%'
        ) THEN 'EDC_Earn_Aff'
        WHEN t1.promo_type IN ('categoryCoupon', 'collectibleCoupon')
        AND (
            TOLOWER(t1.promo_name) LIKE '%affiliate%'
            OR TOLOWER(t1.promo_desc) LIKE '%affiliate%'
            OR TOLOWER(t1.promo_name) LIKE '%om_affiliate%'
            OR TOLOWER(t1.promo_desc) LIKE '%om_affiliate%'
        ) THEN CASE
            WHEN TOLOWER(t1.promo_name) LIKE '%clm_clm%'
            OR TOLOWER(t1.promo_desc) LIKE '%clm_clm%' THEN 'PDV_CLM_Aff'
            WHEN TOLOWER(t1.promo_name) LIKE '%platform_clm_mega_%'
            OR TOLOWER(t1.promo_desc) LIKE '%platform_clm_mega_%'
            OR TOLOWER(t1.promo_name) LIKE '%platform_clm_a+%'
            OR TOLOWER(t1.promo_desc) LIKE '%platform_clm_a+%'
            OR TOLOWER(t1.promo_name) LIKE '%platform_clm_bau order%'
            OR TOLOWER(t1.promo_desc) LIKE '%platform_clm_bau order%'
            OR TOLOWER(t1.promo_name) LIKE '%platform_clm_bau%'
            OR TOLOWER(t1.promo_desc) LIKE '%platform_clm_bau%'
            OR TOLOWER(t1.promo_name) LIKE '%win cart for free%'
            OR TOLOWER(t1.promo_desc) LIKE '%win cart for free%'
            OR TOLOWER(t1.promo_name) LIKE '%platform_clm_win%'
            OR TOLOWER(t1.promo_desc) LIKE '%platform_clm_win%'
            OR TOLOWER(t1.promo_name) LIKE '%lzdwcf%'
            OR TOLOWER(t1.promo_desc) LIKE '%lzdwcf%' THEN 'PDV_Platform_Mega_Aff'
            WHEN TOLOWER(t1.promo_name) LIKE '%top seller vouchers%'
            OR TOLOWER(t1.promo_desc) LIKE '%top seller vouchers%'
            OR TOLOWER(t1.promo_name) LIKE '%lazmall%'
            OR TOLOWER(t1.promo_desc) LIKE '%lazmall%'
            OR TOLOWER(t1.promo_name) LIKE '%long-tail booster%'
            OR TOLOWER(t1.promo_desc) LIKE '%long-tail booster%'
            OR TOLOWER(t1.promo_name) LIKE '%_fmcg%'
            OR TOLOWER(t1.promo_desc) LIKE '%_fmcg%'
            OR TOLOWER(t1.promo_name) LIKE '%tcf%'
            OR TOLOWER(t1.promo_desc) LIKE '%tcf%'
            OR TOLOWER(t1.promo_name) LIKE '%laztop%'
            OR TOLOWER(t1.promo_desc) LIKE '%laztop%'
            OR TOLOWER(t1.promo_name) LIKE '%lazbeauty%'
            OR TOLOWER(t1.promo_desc) LIKE '%lazbeauty%'
            OR TOLOWER(t1.promo_name) LIKE '%_fa%'
            OR TOLOWER(t1.promo_desc) LIKE '%_fa%'
            OR TOLOWER(t1.promo_name) LIKE '%_gm%'
            OR TOLOWER(t1.promo_desc) LIKE '%_gm%'
            OR TOLOWER(t1.promo_name) LIKE '%_hb%'
            OR TOLOWER(t1.promo_desc) LIKE '%_hb%'
            OR TOLOWER(t1.promo_name) LIKE '%_grmb%'
            OR TOLOWER(t1.promo_desc) LIKE '%_grmb%'
            OR TOLOWER(t1.promo_name) LIKE '%_gcmb%'
            OR TOLOWER(t1.promo_desc) LIKE '%_gcmb%'
            OR TOLOWER(t1.promo_name) LIKE '%_cgmb%'
            OR TOLOWER(t1.promo_desc) LIKE '%_cgmb%'
            OR TOLOWER(t1.promo_name) LIKE '%_el%'
            OR TOLOWER(t1.promo_desc) LIKE '%_el%'
            OR TOLOWER(t1.promo_name) LIKE '%_lazmall%'
            OR TOLOWER(t1.promo_desc) LIKE '%_lazmall%'
            OR TOLOWER(t1.promo_name) LIKE '%_bo%'
            OR TOLOWER(t1.promo_desc) LIKE '%_bo%' THEN 'PDV_Top_Seller_Aff'
            WHEN TOLOWER(t1.promo_name) LIKE '%voucher%plus%'
            OR TOLOWER(t1.promo_desc) LIKE '%voucher%plus%' THEN 'PDV_VCP_Aff'
            ELSE 'PDV_Others_Aff'
        END
        WHEN t1.promo_type IN ('mockedMillionSubsidy')
        AND (
            TOLOWER(t1.promo_name) LIKE '%affiliate%'
            OR TOLOWER(t1.promo_desc) LIKE '%affiliate%'
            OR TOLOWER(t1.promo_name) LIKE '%om_affiliate%'
            OR TOLOWER(t1.promo_desc) LIKE '%om_affiliate%'
        ) THEN 'LazSubsidy_Aff'
        WHEN t1.promo_type IN ('voucherCoupon')
        AND (
            TOLOWER(t1.promo_name) LIKE '%affiliate%'
            OR TOLOWER(t1.promo_desc) LIKE '%affiliate%'
            OR TOLOWER(t1.promo_name) LIKE '%om_affiliate%'
            OR TOLOWER(t1.promo_desc) LIKE '%om_affiliate%'
        ) THEN 'Unique_code_Aff'
    END AS promo_bucket_aff,
    COALESCE(t2.promo_req_month, 'not_input') AS promo_req_month,
    COALESCE(t2.promo_req_group_segment, 'not_input') AS promo_req_group_segment,
    COALESCE(t2.promo_req_segment, 'not_input') AS promo_req_segment,
    COALESCE(t2.promo_funded_src, 'not_input') AS promo_funded_src,
    COALESCE(t2.promo_reward_type, 'not_input') AS promo_reward_type,
    MAX(t1.discount_value) AS discount_value,
    MAX(t1.discount_percentage) AS discount_percentage,
    MAX(t1.min_order_value) AS min_order_value,
    MAX(t1.stock_value) AS stock_value,
    MAX(t1.collected_value) AS collected_value,
    COUNT(DISTINCT t3.check_out_id) AS redeemed_value
FROM (
        SELECT DISTINCT t1.ds_start_collect AS ds_start_collect,
            t1.ds_end_collect AS ds_end_collect,
            t1.ds_start_redeem AS ds_start_redeem,
            t1.ds_end_redeem AS ds_end_redeem,
            COALESCE(t1.promo_id, t2.promo_id) AS promo_id,
            COALESCE(t1.promo_name, t2.promo_name) AS promo_name,
            t1.promo_desc AS promo_desc,
            t2.promo_type AS promo_type,
            COALESCE(t1.discount_value, 0) AS discount_value,
            COALESCE(t1.discount_percentage, 0) AS discount_percentage,
            COALESCE(t1.min_order_value, 0) AS min_order_value,
            COALESCE(t1.stock_value, 0) AS stock_value,
            COALESCE(t1.collected_value, 0) AS collected_value,
            t2.sales_order_item_id AS sales_order_item_id
        FROM (
                SELECT TO_CHAR(MAX(teasing_start_date), 'yyyymmdd') AS ds_start_collect,
                    TO_CHAR(MAX(teasing_end_date), 'yyyymmdd') AS ds_end_collect,
                    TO_CHAR(MAX(promotion_start_date), 'yyyymmdd') AS ds_start_redeem,
                    TO_CHAR(MAX(promotion_end_date), 'yyyymmdd') AS ds_end_redeem,
                    promotion_id AS promo_id,
                    voucher_name AS promo_name,
                    description AS promo_desc --
                    -- ,NULL AS promo_code --<< Consider to open
,
                    MAX(coupon_value) AS discount_value,
                    NULL AS discount_percentage,
                    MAX(min_order_value) AS min_order_value,
                    MAX(total_value) AS stock_value,
                    MAX(collected_count) AS collected_value
                FROM lazada_cdm.dim_lzd_pro_collectibles
                WHERE 1 = 1
                    AND ds = MAX_PT('lazada_cdm.dim_lzd_pro_collectibles')
                    AND venture = 'VN'
                    AND TOLOWER(sponsor) IN ('platform')
                    AND (
                        TOLOWER(retail_sponsor) IN ('platform')
                        OR retail_sponsor IS NULL
                    )
                    AND status NOT IN (-1)
                GROUP BY promotion_id,
                    voucher_name,
                    description
                UNION ALL
                --<< Break point
                SELECT NULL AS ds_start_collect,
                    NULL AS ds_end_collect,
                    TO_CHAR(MAX(voucher_start_date), 'yyyymmdd') AS ds_start_redeem,
                    TO_CHAR(MAX(voucher_end_date), 'yyyymmdd') AS ds_end_redeem,
                    promotion_id AS promo_id,
                    voucher_name AS promo_name,
                    voucher_desc AS promo_desc --
                    -- ,voucher_code AS promo_code --<< Consider to open
,
                    MAX(voucher_amount) AS discount_value,
                    MAX(discount_percentage) AS discount_percentage,
                    MAX(minimum_spend) AS min_order_value,
                    MAX(usage_limit) AS stock_value,
                    NULL AS collected_value
                FROM lazada_cdm.dim_lzd_pro_voucher_rule
                WHERE 1 = 1
                    AND ds = MAX_PT('lazada_cdm.dim_lzd_pro_voucher_rule')
                    AND venture = 'VN'
                    AND TOLOWER(voucher_sponsor) IN ('lazada')
                    AND (
                        TOLOWER(retail_sponsor) IN ('platform')
                        OR retail_sponsor IS NULL
                    )
                GROUP BY promotion_id,
                    voucher_name,
                    voucher_desc
            ) AS t1
            FULL OUTER JOIN (
                SELECT sales_order_item_id,
                    promotion_id AS promo_id,
                    promotion_name AS promo_name,
                    promotion_type AS promo_type
                FROM lazada_cdm.dwd_lzd_pro_promotion_item_di
                WHERE 1 = 1
                    AND venture = 'VN'
                    AND is_fulfilled = 1
                    AND promotion_type IN (
                        'shippingFeeCoupon',
                        'purchaseIncentive',
                        'lpiCoupon',
                        'cashbackBalance',
                        'cashbackCoupon',
                        'categoryCoupon',
                        'collectibleCoupon',
                        'mockedMillionSubsidy',
                        'voucherCoupon'
                    )
                    AND TOLOWER(promotion_role) IN ('platform')
                    AND (
                        TOLOWER(retail_sponsor) IN ('platform')
                        OR retail_sponsor IS NULL
                    )
            ) AS t2 ON t1.promo_id = t2.promo_id
    ) AS t1
    LEFT JOIN (
        SELECT DISTINCT promo_id,
            promo_req_month,
            promo_req_group_segment,
            promo_req_segment,
            promo_funded_src,
            reward_type AS promo_reward_type
        FROM lazada_analyst.loutruong_aff_vc_mapping
    ) AS t2 ON t1.promo_id = t2.promo_id
    LEFT JOIN (
        SELECT sales_order_item_id,
            check_out_id
        FROM lazada_cdm.dwd_lzd_trd_core_df
        WHERE 1 = 1
            AND ds = MAX_PT('lazada_cdm.dwd_lzd_trd_core_df')
            AND venture = 'VN'
            AND is_revenue = 1
            AND is_fulfilled = 1
            AND business_application IN ('LZD,ZAL', 'LZD')
            AND TOLOWER(item_status_esm) IN (
                'delivered',
                'exportable',
                'return_denied',
                'shipped'
            )
    ) AS t3 ON t1.sales_order_item_id = t3.sales_order_item_id
WHERE 1 = 1
    AND (
        TOLOWER(t1.promo_name) LIKE '%affiliate%'
        OR TOLOWER(t1.promo_desc) LIKE '%affiliate%'
        OR TOLOWER(t1.promo_name) LIKE '%om_affiliate%'
        OR TOLOWER(t1.promo_desc) LIKE '%om_affiliate%'
    )
GROUP BY t1.ds_start_collect,
    t1.ds_end_collect,
    t1.ds_start_redeem,
    t1.ds_end_redeem,
    t1.promo_id,
    t1.promo_name,
    t1.promo_desc,
    t1.promo_type,
CASE
        WHEN t1.promo_type IN ('shippingFeeCoupon')
        AND (
            TOLOWER(t1.promo_name) LIKE '%platform_clm_seller boost program%'
            OR TOLOWER(t1.promo_desc) LIKE '%platform_clm_seller boost program%'
        ) THEN 'FSM_Flag'
        WHEN t1.promo_type IN ('shippingFeeCoupon') THEN 'FS_Platform'
        WHEN t1.promo_type IN ('purchaseIncentive', 'lpiCoupon') THEN 'LPI'
        WHEN t1.promo_type IN ('cashbackBalance') THEN 'EDC_Redeem'
        WHEN t1.promo_type IN ('cashbackCoupon') THEN 'EDC_Earn'
        WHEN t1.promo_type IN ('categoryCoupon', 'collectibleCoupon') THEN CASE
            WHEN TOLOWER(t1.promo_name) LIKE '%clm_clm%'
            OR TOLOWER(t1.promo_desc) LIKE '%clm_clm%' THEN 'PDV_CLM'
            WHEN TOLOWER(t1.promo_name) LIKE '%platform_clm_mega_%'
            OR TOLOWER(t1.promo_desc) LIKE '%platform_clm_mega_%'
            OR TOLOWER(t1.promo_name) LIKE '%platform_clm_a+%'
            OR TOLOWER(t1.promo_desc) LIKE '%platform_clm_a+%'
            OR TOLOWER(t1.promo_name) LIKE '%platform_clm_bau order%'
            OR TOLOWER(t1.promo_desc) LIKE '%platform_clm_bau order%'
            OR TOLOWER(t1.promo_name) LIKE '%platform_clm_bau%'
            OR TOLOWER(t1.promo_desc) LIKE '%platform_clm_bau%'
            OR TOLOWER(t1.promo_name) LIKE '%win cart for free%'
            OR TOLOWER(t1.promo_desc) LIKE '%win cart for free%'
            OR TOLOWER(t1.promo_name) LIKE '%platform_clm_win%'
            OR TOLOWER(t1.promo_desc) LIKE '%platform_clm_win%'
            OR TOLOWER(t1.promo_name) LIKE '%lzdwcf%'
            OR TOLOWER(t1.promo_desc) LIKE '%lzdwcf%' THEN 'PDV_Platform_Mega'
            WHEN TOLOWER(t1.promo_name) LIKE '%top seller vouchers%'
            OR TOLOWER(t1.promo_desc) LIKE '%top seller vouchers%'
            OR TOLOWER(t1.promo_name) LIKE '%lazmall%'
            OR TOLOWER(t1.promo_desc) LIKE '%lazmall%'
            OR TOLOWER(t1.promo_name) LIKE '%long-tail booster%'
            OR TOLOWER(t1.promo_desc) LIKE '%long-tail booster%'
            OR TOLOWER(t1.promo_name) LIKE '%_fmcg%'
            OR TOLOWER(t1.promo_desc) LIKE '%_fmcg%'
            OR TOLOWER(t1.promo_name) LIKE '%tcf%'
            OR TOLOWER(t1.promo_desc) LIKE '%tcf%'
            OR TOLOWER(t1.promo_name) LIKE '%laztop%'
            OR TOLOWER(t1.promo_desc) LIKE '%laztop%'
            OR TOLOWER(t1.promo_name) LIKE '%lazbeauty%'
            OR TOLOWER(t1.promo_desc) LIKE '%lazbeauty%'
            OR TOLOWER(t1.promo_name) LIKE '%_fa%'
            OR TOLOWER(t1.promo_desc) LIKE '%_fa%'
            OR TOLOWER(t1.promo_name) LIKE '%_gm%'
            OR TOLOWER(t1.promo_desc) LIKE '%_gm%'
            OR TOLOWER(t1.promo_name) LIKE '%_hb%'
            OR TOLOWER(t1.promo_desc) LIKE '%_hb%'
            OR TOLOWER(t1.promo_name) LIKE '%_grmb%'
            OR TOLOWER(t1.promo_desc) LIKE '%_grmb%'
            OR TOLOWER(t1.promo_name) LIKE '%_gcmb%'
            OR TOLOWER(t1.promo_desc) LIKE '%_gcmb%'
            OR TOLOWER(t1.promo_name) LIKE '%_cgmb%'
            OR TOLOWER(t1.promo_desc) LIKE '%_cgmb%'
            OR TOLOWER(t1.promo_name) LIKE '%_el%'
            OR TOLOWER(t1.promo_desc) LIKE '%_el%'
            OR TOLOWER(t1.promo_name) LIKE '%_lazmall%'
            OR TOLOWER(t1.promo_desc) LIKE '%_lazmall%'
            OR TOLOWER(t1.promo_name) LIKE '%_bo%'
            OR TOLOWER(t1.promo_desc) LIKE '%_bo%' THEN 'PDV_Top_Seller'
            WHEN TOLOWER(t1.promo_name) LIKE '%voucher%plus%'
            OR TOLOWER(t1.promo_desc) LIKE '%voucher%plus%' THEN 'PDV_VCP'
            ELSE 'PDV_Others'
        END
        WHEN t1.promo_type IN ('mockedMillionSubsidy') THEN 'LazSubsidy'
        WHEN t1.promo_type IN ('voucherCoupon') THEN 'Unique_code'
    END,
CASE
        WHEN t1.promo_type IN ('shippingFeeCoupon')
        AND (
            TOLOWER(t1.promo_name) LIKE '%platform_clm_seller boost program%'
            OR TOLOWER(t1.promo_desc) LIKE '%platform_clm_seller boost program%'
        )
        AND (
            TOLOWER(t1.promo_name) LIKE '%affiliate%'
            OR TOLOWER(t1.promo_desc) LIKE '%affiliate%'
            OR TOLOWER(t1.promo_name) LIKE '%om_affiliate%'
            OR TOLOWER(t1.promo_desc) LIKE '%om_affiliate%'
        ) THEN 'FSM_Flag_Aff'
        WHEN t1.promo_type IN ('shippingFeeCoupon')
        AND (
            TOLOWER(t1.promo_name) LIKE '%affiliate%'
            OR TOLOWER(t1.promo_desc) LIKE '%affiliate%'
            OR TOLOWER(t1.promo_name) LIKE '%om_affiliate%'
            OR TOLOWER(t1.promo_desc) LIKE '%om_affiliate%'
        ) THEN 'FS_Platform_Aff'
        WHEN t1.promo_type IN ('purchaseIncentive')
        AND (
            TOLOWER(t1.promo_name) LIKE '%affiliate%'
            OR TOLOWER(t1.promo_desc) LIKE '%affiliate%'
            OR TOLOWER(t1.promo_name) LIKE '%om_affiliate%'
            OR TOLOWER(t1.promo_desc) LIKE '%om_affiliate%'
        ) THEN 'LPI_Aff'
        WHEN t1.promo_type IN ('cashbackBalance')
        AND (
            TOLOWER(t1.promo_name) LIKE '%affiliate%'
            OR TOLOWER(t1.promo_desc) LIKE '%affiliate%'
            OR TOLOWER(t1.promo_name) LIKE '%om_affiliate%'
            OR TOLOWER(t1.promo_desc) LIKE '%om_affiliate%'
        ) THEN 'EDC_Redeem_Aff'
        WHEN t1.promo_type IN ('cashbackCoupon')
        AND (
            TOLOWER(t1.promo_name) LIKE '%affiliate%'
            OR TOLOWER(t1.promo_desc) LIKE '%affiliate%'
            OR TOLOWER(t1.promo_name) LIKE '%om_affiliate%'
            OR TOLOWER(t1.promo_desc) LIKE '%om_affiliate%'
        ) THEN 'EDC_Earn_Aff'
        WHEN t1.promo_type IN ('categoryCoupon', 'collectibleCoupon')
        AND (
            TOLOWER(t1.promo_name) LIKE '%affiliate%'
            OR TOLOWER(t1.promo_desc) LIKE '%affiliate%'
            OR TOLOWER(t1.promo_name) LIKE '%om_affiliate%'
            OR TOLOWER(t1.promo_desc) LIKE '%om_affiliate%'
        ) THEN CASE
            WHEN TOLOWER(t1.promo_name) LIKE '%clm_clm%'
            OR TOLOWER(t1.promo_desc) LIKE '%clm_clm%' THEN 'PDV_CLM_Aff'
            WHEN TOLOWER(t1.promo_name) LIKE '%platform_clm_mega_%'
            OR TOLOWER(t1.promo_desc) LIKE '%platform_clm_mega_%'
            OR TOLOWER(t1.promo_name) LIKE '%platform_clm_a+%'
            OR TOLOWER(t1.promo_desc) LIKE '%platform_clm_a+%'
            OR TOLOWER(t1.promo_name) LIKE '%platform_clm_bau order%'
            OR TOLOWER(t1.promo_desc) LIKE '%platform_clm_bau order%'
            OR TOLOWER(t1.promo_name) LIKE '%platform_clm_bau%'
            OR TOLOWER(t1.promo_desc) LIKE '%platform_clm_bau%'
            OR TOLOWER(t1.promo_name) LIKE '%win cart for free%'
            OR TOLOWER(t1.promo_desc) LIKE '%win cart for free%'
            OR TOLOWER(t1.promo_name) LIKE '%platform_clm_win%'
            OR TOLOWER(t1.promo_desc) LIKE '%platform_clm_win%'
            OR TOLOWER(t1.promo_name) LIKE '%lzdwcf%'
            OR TOLOWER(t1.promo_desc) LIKE '%lzdwcf%' THEN 'PDV_Platform_Mega_Aff'
            WHEN TOLOWER(t1.promo_name) LIKE '%top seller vouchers%'
            OR TOLOWER(t1.promo_desc) LIKE '%top seller vouchers%'
            OR TOLOWER(t1.promo_name) LIKE '%lazmall%'
            OR TOLOWER(t1.promo_desc) LIKE '%lazmall%'
            OR TOLOWER(t1.promo_name) LIKE '%long-tail booster%'
            OR TOLOWER(t1.promo_desc) LIKE '%long-tail booster%'
            OR TOLOWER(t1.promo_name) LIKE '%_fmcg%'
            OR TOLOWER(t1.promo_desc) LIKE '%_fmcg%'
            OR TOLOWER(t1.promo_name) LIKE '%tcf%'
            OR TOLOWER(t1.promo_desc) LIKE '%tcf%'
            OR TOLOWER(t1.promo_name) LIKE '%laztop%'
            OR TOLOWER(t1.promo_desc) LIKE '%laztop%'
            OR TOLOWER(t1.promo_name) LIKE '%lazbeauty%'
            OR TOLOWER(t1.promo_desc) LIKE '%lazbeauty%'
            OR TOLOWER(t1.promo_name) LIKE '%_fa%'
            OR TOLOWER(t1.promo_desc) LIKE '%_fa%'
            OR TOLOWER(t1.promo_name) LIKE '%_gm%'
            OR TOLOWER(t1.promo_desc) LIKE '%_gm%'
            OR TOLOWER(t1.promo_name) LIKE '%_hb%'
            OR TOLOWER(t1.promo_desc) LIKE '%_hb%'
            OR TOLOWER(t1.promo_name) LIKE '%_grmb%'
            OR TOLOWER(t1.promo_desc) LIKE '%_grmb%'
            OR TOLOWER(t1.promo_name) LIKE '%_gcmb%'
            OR TOLOWER(t1.promo_desc) LIKE '%_gcmb%'
            OR TOLOWER(t1.promo_name) LIKE '%_cgmb%'
            OR TOLOWER(t1.promo_desc) LIKE '%_cgmb%'
            OR TOLOWER(t1.promo_name) LIKE '%_el%'
            OR TOLOWER(t1.promo_desc) LIKE '%_el%'
            OR TOLOWER(t1.promo_name) LIKE '%_lazmall%'
            OR TOLOWER(t1.promo_desc) LIKE '%_lazmall%'
            OR TOLOWER(t1.promo_name) LIKE '%_bo%'
            OR TOLOWER(t1.promo_desc) LIKE '%_bo%' THEN 'PDV_Top_Seller_Aff'
            WHEN TOLOWER(t1.promo_name) LIKE '%voucher%plus%'
            OR TOLOWER(t1.promo_desc) LIKE '%voucher%plus%' THEN 'PDV_VCP_Aff'
            ELSE 'PDV_Others_Aff'
        END
        WHEN t1.promo_type IN ('mockedMillionSubsidy')
        AND (
            TOLOWER(t1.promo_name) LIKE '%affiliate%'
            OR TOLOWER(t1.promo_desc) LIKE '%affiliate%'
            OR TOLOWER(t1.promo_name) LIKE '%om_affiliate%'
            OR TOLOWER(t1.promo_desc) LIKE '%om_affiliate%'
        ) THEN 'LazSubsidy_Aff'
        WHEN t1.promo_type IN ('voucherCoupon')
        AND (
            TOLOWER(t1.promo_name) LIKE '%affiliate%'
            OR TOLOWER(t1.promo_desc) LIKE '%affiliate%'
            OR TOLOWER(t1.promo_name) LIKE '%om_affiliate%'
            OR TOLOWER(t1.promo_desc) LIKE '%om_affiliate%'
        ) THEN 'Unique_code_Aff'
    END,
    COALESCE(t2.promo_req_month, 'not_input'),
    COALESCE(t2.promo_req_group_segment, 'not_input'),
    COALESCE(t2.promo_req_segment, 'not_input'),
    COALESCE(t2.promo_funded_src, 'not_input'),
    COALESCE(t2.promo_reward_type, 'not_input');
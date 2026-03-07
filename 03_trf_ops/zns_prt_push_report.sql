-- MaxCompute SQL 
-- ********************************************************************--
-- author:Truong, Van Thanh
-- create time:2024-07-26 17:42:04
-- ********************************************************************--
SELECT t1.batch AS batch,
    t1.seller_name AS seller_name,
    t1.seller_id AS seller_id,
    t1.promo_id AS promo_id,
    t1.scheme AS scheme,
    t2.stock_value AS stock_value,
    t2.collected_value AS collected_value,
    t2.redeemed_value AS redeemed_value,
    COUNT(DISTINCT t3.clickid) AS click,
    COUNT(DISTINCT t3.utdid) AS click_uv,
    COUNT(
        DISTINCT CASE
            WHEN t4.member_id IS NOT NULL
            AND TOLOWER(t4.channel) IN ('cps affiliate') THEN t3.clickid
            ELSE NULL
        END
    ) AS click_aff_guided,
    COUNT(
        DISTINCT CASE
            WHEN t4.member_id IS NOT NULL
            AND TOLOWER(t4.channel) IN ('cps affiliate') THEN t3.utdid
            ELSE NULL
        END
    ) AS click_uv_aff_guided,
    COUNT(
        DISTINCT CASE
            WHEN t4.member_id IS NOT NULL
            AND TOLOWER(t4.channel) IN ('cps affiliate')
            AND t5.member_id IS NOT NULL THEN t3.clickid
            ELSE NULL
        END
    ) AS click_aff_um_zns,
    COUNT(
        DISTINCT CASE
            WHEN t4.member_id IS NOT NULL
            AND TOLOWER(t4.channel) IN ('cps affiliate')
            AND t5.member_id IS NOT NULL THEN t3.utdid
            ELSE NULL
        END
    ) AS click_uv_aff_um_zns,
    COUNT(DISTINCT t3.member_id) AS total_partner_push,
    COUNT(
        DISTINCT CASE
            WHEN t4.member_id IS NOT NULL
            AND TOLOWER(t4.channel) IN ('cps affiliate') THEN t3.member_id
            ELSE NULL
        END
    ) AS total_aff_partner_push,
    COUNT(
        DISTINCT CASE
            WHEN t4.member_id IS NOT NULL
            AND TOLOWER(t4.channel) IN ('cps affiliate')
            AND t5.member_id IS NOT NULL THEN t3.member_id
            ELSE NULL
        END
    ) AS total_aff_partner_um_push
FROM (
        SELECT *
        FROM lazada_analyst.loutruong_seller_voucher_map
        WHERE 1 = 1
            AND batch IN ("LMS July'24") --<< Change
    ) AS t1
    LEFT JOIN (
        SELECT promotion_id AS promo_id,
            voucher_name AS promo_name,
            description AS promo_desc,
            MAX(coupon_value) AS discount_value,
            MAX(min_order_value) AS min_order_value,
            MAX(total_value) AS stock_value,
            MAX(collected_count) AS collected_value,
            MAX(redeemed_count) AS redeemed_value
        FROM lazada_cdm.dim_lzd_pro_collectibles
        WHERE 1 = 1
            AND ds = MAX_PT('lazada_cdm.dim_lzd_pro_collectibles')
            AND venture = 'VN'
            AND promotion_id IN (
                SELECT DISTINCT promo_id
                FROM lazada_analyst.loutruong_seller_voucher_map
                WHERE 1 = 1
                    AND batch IN ("LMS July'24") --<< Change
            )
        GROUP BY promotion_id,
            voucher_name,
            description
    ) AS t2 ON t1.promo_id = t2.promo_id
    LEFT JOIN (
        SELECT ds,
            member_id,
            lp_url,
            SUBSTRING_INDEX(
                SUBSTRING(
                    lp_url,
                    LOCATE('&voucherId=', lp_url) + LENGTH('&voucherId='),
                    LENGTH(lp_url)
                ),
                '&',
                1
            ) AS seller_promo_id,
            utdid,
            clickid
        FROM lazada_analyst_dev.loutruong_dwd_lzd_clickserver_log_di
        WHERE 1 = 1
            AND ds >= 20240701
            AND is_app = 1
    ) AS t3 ON t1.seller_promo_id = t3.seller_promo_id
    LEFT JOIN (
        SELECT channel,
            member_id,
            MAX(member_name) AS member_name
        FROM lazada_cdm.dim_lzd_mkt_pid_info_df
        WHERE 1 = 1
            AND ds = MAX_PT('lazada_cdm.dim_lzd_mkt_pid_info_df')
            AND venture = 'VN'
        GROUP BY channel,
            member_id
    ) AS t4 ON t3.member_id = t4.member_id
    LEFT JOIN (
        SELECT *
        FROM lazada_analyst.loutruong_partner_push_zns
        WHERE 1 = 1
            AND batch IN ("LMS July'24") --<< Change
    ) AS t5 ON t3.member_id = t5.member_id
GROUP BY t1.batch,
    t1.seller_name,
    t1.seller_id,
    t1.promo_id,
    t1.scheme,
    t2.stock_value,
    t2.collected_value,
    t2.redeemed_value;
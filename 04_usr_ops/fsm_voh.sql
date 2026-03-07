-- MaxCompute SQL 
-- ********************************************************************--
-- author:Truong, Van Thanh
-- create time:2024-08-29 11:02:48
-- ********************************************************************--
WITH t_usr_seg AS (
    SELECT ds_month,
        user_id,
        user_contribution_tag AS user_contribution_tag_old,
        CASE
            WHEN user_contribution_tag IN ('high') THEN '01_high'
            WHEN user_contribution_tag IN ('middle') THEN '02_middle'
            WHEN user_contribution_tag IN ('low') THEN '03_low'
            WHEN user_contribution_tag IN ('lto 3-') THEN '04_lto 3-'
            WHEN user_contribution_tag IN ('undefined') THEN '05_undefined'
        END AS user_contribution_tag
    FROM lazada_ads.dws_lzd_cdp_usr_uid_contribute_value_v3
    WHERE 1 = 1 --
        -- AND     ds_month = MAX_PT('lazada_ads.dws_lzd_cdp_usr_uid_contribute_value_v3') --<< TEST
        AND ds_month >= 202403 --<< Start
        AND venture = 'VN'
),
t_usr_region AS (
    SELECT user_id,
        CASE
            WHEN region2 LIKE '%Hồ Chí Minh%' THEN '01_metro'
            WHEN region2 LIKE '%Hà Nội%' THEN '01_metro'
            WHEN region2 LIKE '%Đà Nẵng%' THEN '01_metro'
            WHEN region2 LIKE '%Cần Thơ%' THEN '02_metro_plus'
            WHEN region2 LIKE '%Hải Phòng%' THEN '02_metro_plus'
            WHEN region2 LIKE '%Bắc Ninh%' THEN '02_metro_plus'
            WHEN region2 LIKE '%Bình Dương%' THEN '02_metro_plus'
            WHEN region2 LIKE '%Đồng Nai%' THEN '02_metro_plus'
            WHEN region2 LIKE '%Long An%' THEN '02_metro_plus'
            WHEN region2 LIKE '%Khánh Hòa%' THEN '02_metro_plus'
        END AS region
    FROM lazada_cdm.dim_lzd_usr
    WHERE 1 = 1
        AND ds = MAX_PT("lazada_cdm.dim_lzd_usr")
        AND venture = 'VN'
),
t_dim_promo AS (
    SELECT *,
        CASE
            WHEN voucher_type IN ('fixed_amount') THEN CONCAT(
                CAST(COALESCE(coupon_value, 0) / 1000 AS BIGINT),
                'k',
                '/',
                CAST(COALESCE(min_order_value, 0) / 1000 AS BIGINT),
                'k'
            )
            WHEN voucher_type IN ('percent_amount') THEN CONCAT(
                CAST(COALESCE(coupon_value, 0) AS BIGINT),
                '%',
                '/',
                CAST(COALESCE(min_order_value, 0) / 1000 AS BIGINT),
                'k',
                ' ',
                'Cap',
                ' ',
                CAST(
                    COALESCE(promotion_cap_value, 0) / 1000 AS BIGINT
                ),
                'k'
            )
        END AS promotion_scheme
    FROM lazada_cdm.dim_lzd_pro_collectibles
    WHERE 1 = 1
        AND ds = MAX_PT('lazada_cdm.dim_lzd_pro_collectibles')
        AND venture = 'VN'
        AND sponsor = 'platform'
        AND (
            TOLOWER(retail_sponsor) IN ('platform')
            OR retail_sponsor IS NULL
        )
),
t_voh AS (
    SELECT t1.ds AS ds,
        t1.utdid AS utdid,
        t1.user_id AS user_id,
        t1.url_type AS url_type,
        COALESCE(is_fsm_clt, 0) AS is_fsm_voh,
        COALESCE(is_lpi_clt, 0) AS is_lpi_voh
    FROM (
            SELECT ds,
                utdid,
                user_id,
                url_type
            FROM lazada_cdm.dws_lzd_mkt_app_ad_mp_di
            WHERE 1 = 1 --
                -- AND     ds = '${bizdate}' --<< Test
                AND ds BETWEEN 20240701 AND 20240831 --<< Change cycle
                AND venture = 'VN'
                AND attr_model IN ('ft_1d_np')
                AND is_active_utdid = 1
        ) AS t1
        LEFT JOIN (
            SELECT t1.*,
                CASE
                    WHEN t2.prom_sub_type IN ('FS Max') THEN 1
                    ELSE 0
                END AS is_fsm_clt,
                CASE
                    WHEN t2.product_code IN ('categoryCoupon', 'collectibleCoupon')
                    AND (
                        TOLOWER(t2.voucher_name) LIKE '%voucher%bonus%'
                        OR TOLOWER(t2.description) LIKE '%voucher%bonus%'
                    ) THEN 1
                    ELSE 0
                END AS is_lpi_clt,
                t2.voucher_name AS promotion_name,
                t2.description AS promotion_desc
            FROM (
                    SELECT *
                    FROM lazada_cdm.dwd_lzd_pro_voucher_collect_di
                    WHERE 1 = 1 -- AND     ds = '${bizdate}' --<< Test
                        AND ds >= 20240301
                        AND venture = 'VN'
                        AND start_local_date >= 20240301
                ) AS t1
                LEFT JOIN (
                    SELECT *
                    FROM t_dim_promo
                ) AS t2 ON t1.promotion_id = t2.promotion_id
            WHERE 1 = 1
                AND (
                    t2.prom_sub_type IN ('FS Max')
                    OR t2.product_code IN ('categoryCoupon', 'collectibleCoupon')
                    AND (
                        TOLOWER(t2.voucher_name) LIKE '%voucher%bonus%'
                        OR TOLOWER(t2.description) LIKE '%voucher%bonus%'
                    )
                )
        ) AS t2 ON t1.ds >= t2.ds
        AND t1.ds BETWEEN t2.start_local_date AND t2.end_local_date
        AND t1.user_id = t2.buyer_id
),
t_trn_dwd AS (
    SELECT t1.*,
        COALESCE(t2.is_fsm_voh, 0) AS is_fsm_voh,
        CASE
            WHEN t3.seller_id IS NOT NULL THEN 1
            ELSE 0
        END AS is_fsm_eli,
        COALESCE(t4.is_fsm_promo, 0) AS is_fsm_promo,
        COALESCE(t4.is_lpi_promo, 0) AS is_lpi_promo
    FROM (
            SELECT *
            FROM lazada_cdm.dwd_lzd_trd_core_fulfill_di
            WHERE 1 = 1 --
                -- AND     ds = '${bizdate}' --<< Test
                AND ds BETWEEN 20240701 AND 20240831 --<< Change cycle
                AND venture = 'VN'
                AND is_revenue = 1
                AND COALESCE(business_application, 'LZD') IN ('LZD,ZAL', 'LZD')
        ) AS t1
        LEFT JOIN (
            SELECT ds,
                user_id,
                MAX(is_fsm_voh) AS is_fsm_voh
            FROM t_voh
            GROUP BY ds,
                user_id
        ) AS t2 ON t1.ds = t2.ds
        AND t1.buyer_id = t2.user_id
        LEFT JOIN (
            SELECT DISTINCT ds,
                seller_id
            FROM lazada_cdm.dwd_lzd_pro_fsmax_program_seller_detail_df
            WHERE 1 = 1 --
                -- AND     ds = MAX_PT("lazada_cdm.dwd_lzd_pro_fsmax_program_seller_detail_df") --<< Get full list within 6 months rolling
                -- AND     ds = '${bizdate}' --<< Test
                AND ds BETWEEN 20240701 AND 20240831 --<< Change cycle
                AND ds >= TO_CHAR(
                    TO_DATE(join_time, 'yyyy-mm-dd hh:mi:ss'),
                    'yyyymmdd'
                ) --<< Joined
                AND joined = 1
                AND venture = 'VN'
                AND ds BETWEEN TO_CHAR(
                    TO_DATE(start_time, 'yyyy-mm-dd hh:mi:ss'),
                    'yyyymmdd'
                ) AND TO_CHAR(
                    TO_DATE(end_time, 'yyyy-mm-dd hh:mi:ss'),
                    'yyyymmdd'
                ) --<< Running
                AND suspend_time IS NULL --<< Not shut down
                AND program_type = 1 --<< Exclude seller account test
                AND seller_id NOT IN (202204217328, 200180502014) --<< Exclude seller account test, seller account bug
        ) AS t3 ON t1.ds = t3.ds
        AND t1.seller_id = t3.seller_id
        LEFT JOIN (
            SELECT t1.sales_order_item_id AS sales_order_item_id,
                MAX(
                    CASE
                        WHEN t2.prom_sub_type IN ('FS Max') THEN 1
                        ELSE 0
                    END
                ) AS is_fsm_promo,
                MAX(
                    CASE
                        WHEN t1.promotion_type IN ('purchaseIncentive', 'lpiCoupon')
                        OR (
                            t1.promotion_type IN ('categoryCoupon', 'collectibleCoupon')
                            AND (
                                TOLOWER(t1.promotion_name) LIKE '%voucher%bonus%'
                                OR TOLOWER(t2.voucher_name) LIKE '%voucher%bonus%'
                                OR TOLOWER(t2.description) LIKE '%voucher%bonus%'
                            )
                        ) THEN 1
                        ELSE 0
                    END
                ) AS is_lpi_promo
            FROM (
                    SELECT *
                    FROM lazada_cdm.dwd_lzd_pro_promotion_item_di
                    WHERE 1 = 1 --
                        -- AND     ds = '${bizdate}' --<< Test
                        AND ds >= 20240401 --<< Change cycle
                        AND venture = 'VN'
                        AND is_fulfilled = 1
                        AND promotion_type IN (
                            'shippingFeeCoupon',
                            'purchaseIncentive',
                            'lpiCoupon',
                            'categoryCoupon',
                            'collectibleCoupon'
                        )
                        AND TOLOWER(promotion_role) IN ('platform')
                        AND (
                            TOLOWER(retail_sponsor) IN ('platform')
                            OR retail_sponsor IS NULL
                        )
                ) AS t1
                LEFT JOIN (
                    SELECT *
                    FROM t_dim_promo
                ) AS t2 ON t1.promotion_id = t2.promotion_id
            GROUP BY t1.sales_order_item_id
        ) AS t4 ON t1.sales_order_item_id = t4.sales_order_item_id
),
t_trf_ads AS (
    SELECT t1.ds AS ds,
        COALESCE(t2.user_contribution_tag, '05_undefined') AS user_contribution_tag,
        COALESCE(t3.region, '03_non_metro') AS region,
        COUNT(DISTINCT t1.utdid) AS dau,
        COUNT(
            DISTINCT CASE
                WHEN t1.is_fsm_voh = 1 THEN t1.utdid
                ELSE NULL
            END
        ) AS dau_voh,
        COUNT(
            DISTINCT CASE
                WHEN t1.is_fsm_voh = 0 THEN t1.utdid
                ELSE NULL
            END
        ) AS dau_no_voh,
        COUNT(
            DISTINCT CASE
                WHEN is_pdp = 1 THEN t1.utdid
                ELSE NULL
            END
        ) AS pdp_uv,
        COUNT(
            DISTINCT CASE
                WHEN t1.is_fsm_voh = 1
                AND is_pdp = 1 THEN t1.utdid
                ELSE NULL
            END
        ) AS pdp_uv_voh,
        COUNT(
            DISTINCT CASE
                WHEN t1.is_fsm_voh = 0
                AND is_pdp = 1 THEN t1.utdid
                ELSE NULL
            END
        ) AS pdp_uv_no_voh
    FROM (
            SELECT ds,
                utdid,
                user_id,
                MAX(
                    MAX(
                        CASE
                            WHEN url_type = 'ipv' THEN 1
                            ELSE 0
                        END
                    )
                ) OVER (PARTITION BY ds, utdid) AS is_pdp,
                MAX(MAX(is_fsm_voh)) OVER (PARTITION BY ds, utdid) AS is_fsm_voh
            FROM t_voh
            GROUP BY ds,
                utdid,
                user_id
        ) AS t1
        LEFT JOIN (
            SELECT *
            FROM t_usr_seg
        ) AS t2 ON TO_CHAR(TO_DATE(t1.ds, 'yyyymmdd'), 'yyyymm') = TO_CHAR(
            DATEADD(TO_DATE(t2.ds_month, 'yyyymm'), + 1, 'mm'),
            'yyyymm'
        )
        AND t1.user_id = t2.user_id
        LEFT JOIN (
            SELECT *
            FROM t_usr_region
        ) AS t3 ON t1.user_id = t3.user_id
    GROUP BY t1.ds,
        COALESCE(t2.user_contribution_tag, '05_undefined'),
        COALESCE(t3.region, '03_non_metro')
),
t_trn_ads AS (
    SELECT t1.ds,
        COALESCE(t2.user_contribution_tag, '05_undefined') AS user_contribution_tag,
        COALESCE(t3.region, '03_non_metro') AS region,
        COUNT(DISTINCT t1.buyer_id) AS byr,
        COUNT(
            DISTINCT CASE
                WHEN t1.is_fsm_voh = 1 THEN t1.buyer_id
                ELSE NULL
            END
        ) AS byr_voh,
        COUNT(
            DISTINCT CASE
                WHEN t1.is_fsm_voh = 0 THEN t1.buyer_id
                ELSE NULL
            END
        ) AS byr_no_voh --
        -- ORDER
,
        COUNT(DISTINCT t1.order_id) AS ord --
        --
,
        COUNT(
            DISTINCT CASE
                WHEN t1.is_fsm_eli = 1 THEN t1.order_id
                ELSE NULL
            END
        ) AS ord_eli,
        COUNT(
            DISTINCT CASE
                WHEN t1.is_fsm_eli = 1
                AND is_fsm_promo = 1 THEN t1.order_id
                ELSE NULL
            END
        ) AS ord_eli_gui --
        -- ,COUNT(DISTINCT CASE    WHEN t1.is_fsm_eli = 1 AND is_fsm_promo = 0 THEN t1.order_id ELSE NULL END) AS ord_eli_no_gui --
        --
,
        COUNT(
            DISTINCT CASE
                WHEN t1.is_fsm_eli = 0 THEN t1.order_id
                ELSE NULL
            END
        ) AS ord_no_eli --
        -- ,COUNT(DISTINCT CASE    WHEN t1.is_fsm_eli = 0 AND is_fsm_promo = 1 THEN t1.order_id ELSE NULL END) AS ord_no_eli_gui
        -- ,COUNT(DISTINCT CASE    WHEN t1.is_fsm_eli = 0 AND is_fsm_promo = 0 THEN t1.order_id ELSE NULL END) AS ord_no_eli_no_gui --
        --
,
        COUNT(
            DISTINCT CASE
                WHEN t1.is_fsm_voh = 1 THEN t1.order_id
                ELSE NULL
            END
        ) AS ord_voh,
        COUNT(
            DISTINCT CASE
                WHEN t1.is_fsm_voh = 1
                AND t1.is_fsm_eli = 1 THEN t1.order_id
                ELSE NULL
            END
        ) AS ord_voh_eli,
        COUNT(
            DISTINCT CASE
                WHEN t1.is_fsm_voh = 1
                AND t1.is_fsm_eli = 1
                AND is_fsm_promo = 1 THEN t1.order_id
                ELSE NULL
            END
        ) AS ord_voh_eli_gui,
        COUNT(
            DISTINCT CASE
                WHEN t1.is_fsm_voh = 1
                AND t1.is_fsm_eli = 0 THEN t1.order_id
                ELSE NULL
            END
        ) AS ord_voh_no_eli --
        --
,
        COUNT(
            DISTINCT CASE
                WHEN t1.is_fsm_voh = 0 THEN t1.order_id
                ELSE NULL
            END
        ) AS ord_no_voh,
        COUNT(
            DISTINCT CASE
                WHEN t1.is_fsm_voh = 0
                AND t1.is_fsm_eli = 1 THEN t1.order_id
                ELSE NULL
            END
        ) AS ord_no_voh_eli,
        COUNT(
            DISTINCT CASE
                WHEN t1.is_fsm_voh = 0
                AND t1.is_fsm_eli = 1
                AND is_fsm_promo = 1 THEN t1.order_id
                ELSE NULL
            END
        ) AS ord_no_voh_eli_gui,
        COUNT(
            DISTINCT CASE
                WHEN t1.is_fsm_voh = 0
                AND t1.is_fsm_eli = 0 THEN t1.order_id
                ELSE NULL
            END
        ) AS ord_no_voh_no_eli --
        -- GMV
,
        SUM(t1.actual_gmv * t1.exchange_rate) AS gmv --
        --
,
        SUM(
            CASE
                WHEN t1.is_fsm_eli = 1 THEN t1.actual_gmv * t1.exchange_rate
                ELSE 0
            END
        ) AS gmv_eli,
        SUM(
            CASE
                WHEN t1.is_fsm_eli = 1
                AND is_fsm_promo = 1 THEN t1.actual_gmv * t1.exchange_rate
                ELSE 0
            END
        ) AS gmv_eli_gui --
        -- ,SUM(
        --     CASE    WHEN t1.is_fsm_eli = 1 AND is_fsm_promo = 0 THEN t1.actual_gmv * t1.exchange_rate ELSE 0 END
        -- ) AS gmv_eli_no_gui --
        --
,
        SUM(
            CASE
                WHEN t1.is_fsm_eli = 0 THEN t1.actual_gmv * t1.exchange_rate
                ELSE 0
            END
        ) AS gmv_no_eli --
        -- ,SUM(
        --     CASE    WHEN t1.is_fsm_eli = 0 AND is_fsm_promo = 1 THEN t1.actual_gmv * t1.exchange_rate ELSE 0 END
        -- ) AS gmv_no_eli_gui
        -- ,SUM(
        --     CASE    WHEN t1.is_fsm_eli = 0 AND is_fsm_promo = 0 THEN t1.actual_gmv * t1.exchange_rate ELSE 0 END
        -- ) AS gmv_no_eli_no_gui --
        --
,
        SUM(
            CASE
                WHEN t1.is_fsm_voh = 1 THEN t1.actual_gmv * t1.exchange_rate
                ELSE 0
            END
        ) AS gmv_voh,
        SUM(
            CASE
                WHEN t1.is_fsm_voh = 1
                AND t1.is_fsm_eli = 1 THEN t1.actual_gmv * t1.exchange_rate
                ELSE 0
            END
        ) AS gmv_voh_eli,
        SUM(
            CASE
                WHEN t1.is_fsm_voh = 1
                AND t1.is_fsm_eli = 1
                AND is_fsm_promo = 1 THEN t1.actual_gmv * t1.exchange_rate
                ELSE 0
            END
        ) AS gmv_voh_eli_gui,
        SUM(
            CASE
                WHEN t1.is_fsm_voh = 1
                AND t1.is_fsm_eli = 0 THEN t1.actual_gmv * t1.exchange_rate
                ELSE 0
            END
        ) AS gmv_voh_no_eli --
        --
,
        SUM(
            CASE
                WHEN t1.is_fsm_voh = 0 THEN t1.actual_gmv * t1.exchange_rate
                ELSE 0
            END
        ) AS gmv_no_voh,
        SUM(
            CASE
                WHEN t1.is_fsm_voh = 0
                AND t1.is_fsm_eli = 1 THEN t1.actual_gmv * t1.exchange_rate
                ELSE 0
            END
        ) AS gmv_no_voh_eli,
        SUM(
            CASE
                WHEN t1.is_fsm_voh = 0
                AND t1.is_fsm_eli = 1
                AND is_fsm_promo = 1 THEN t1.actual_gmv * t1.exchange_rate
                ELSE 0
            END
        ) AS gmv_no_voh_eli_gui,
        SUM(
            CASE
                WHEN t1.is_fsm_voh = 0
                AND t1.is_fsm_eli = 0 THEN t1.actual_gmv * t1.exchange_rate
                ELSE 0
            END
        ) AS gmv_no_voh_no_eli --
    FROM (
            SELECT *
            FROM t_trn_dwd
        ) AS t1
        LEFT JOIN (
            SELECT *
            FROM t_usr_seg
        ) AS t2 ON TO_CHAR(TO_DATE(t1.ds, 'yyyymmdd'), 'yyyymm') = TO_CHAR(
            DATEADD(TO_DATE(t2.ds_month, 'yyyymm'), + 1, 'mm'),
            'yyyymm'
        )
        AND t1.buyer_id = t2.user_id
        LEFT JOIN (
            SELECT *
            FROM t_usr_region
        ) AS t3 ON t1.buyer_id = t3.user_id
    GROUP BY t1.ds,
        COALESCE(t2.user_contribution_tag, '05_undefined'),
        COALESCE(t3.region, '03_non_metro')
)
SELECT t1.ds,
    CONCAT(
        'Week:',
        ' ',
        t2.week_start,
        ' ',
        '-->>',
        ' ',
        t2.week_end
    ) AS week,
    t3.camp_type_fixed AS camp_type,
    t1.user_contribution_tag,
    t1.region,
    SUM(t1.dau) AS dau,
    SUM(t1.dau_voh) AS dau_voh,
    SUM(t1.dau_no_voh) AS dau_no_voh,
    SUM(t1.pdp_uv) AS pdp_uv,
    SUM(t1.pdp_uv_voh) AS pdp_uv_voh,
    SUM(t1.pdp_uv_no_voh) AS pdp_uv_no_voh,
    SUM(t1.byr) AS byr,
    SUM(t1.byr_voh) AS byr_voh,
    SUM(t1.byr_no_voh) AS byr_no_voh,
    SUM(t1.ord) AS ord,
    SUM(t1.ord_eli) AS ord_eli,
    SUM(t1.ord_eli_gui) AS ord_eli_gui,
    SUM(t1.ord_no_eli) AS ord_no_eli,
    SUM(t1.ord_voh) AS ord_voh,
    SUM(t1.ord_voh_eli) AS ord_voh_eli,
    SUM(t1.ord_voh_eli_gui) AS ord_voh_eli_gui,
    SUM(t1.ord_voh_no_eli) AS ord_voh_no_eli,
    SUM(t1.ord_no_voh) AS ord_no_voh,
    SUM(t1.ord_no_voh_eli) AS ord_no_voh_eli,
    SUM(t1.ord_no_voh_eli_gui) AS ord_no_voh_eli_gui,
    SUM(t1.ord_no_voh_no_eli) AS ord_no_voh_no_eli,
    SUM(t1.gmv) AS gmv,
    SUM(t1.gmv_eli) AS gmv_eli,
    SUM(t1.gmv_eli_gui) AS gmv_eli_gui,
    SUM(t1.gmv_no_eli) AS gmv_no_eli,
    SUM(t1.gmv_voh) AS gmv_voh,
    SUM(t1.gmv_voh_eli) AS gmv_voh_eli,
    SUM(t1.gmv_voh_eli_gui) AS gmv_voh_eli_gui,
    SUM(t1.gmv_voh_no_eli) AS gmv_voh_no_eli,
    SUM(t1.gmv_no_voh) AS gmv_no_voh,
    SUM(t1.gmv_no_voh_eli) AS gmv_no_voh_eli,
    SUM(t1.gmv_no_voh_eli_gui) AS gmv_no_voh_eli_gui,
    SUM(t1.gmv_no_voh_no_eli) AS gmv_no_voh_no_eli
FROM (
        SELECT ds,
            user_contribution_tag,
            region,
            dau,
            dau_voh,
            dau_no_voh,
            pdp_uv,
            pdp_uv_voh,
            pdp_uv_no_voh,
            0 AS byr,
            0 AS byr_voh,
            0 AS byr_no_voh,
            0 AS ord,
            0 AS ord_eli,
            0 AS ord_eli_gui,
            0 AS ord_no_eli,
            0 AS ord_voh,
            0 AS ord_voh_eli,
            0 AS ord_voh_eli_gui,
            0 AS ord_voh_no_eli,
            0 AS ord_no_voh,
            0 AS ord_no_voh_eli,
            0 AS ord_no_voh_eli_gui,
            0 AS ord_no_voh_no_eli,
            0 AS gmv,
            0 AS gmv_eli,
            0 AS gmv_eli_gui,
            0 AS gmv_no_eli,
            0 AS gmv_voh,
            0 AS gmv_voh_eli,
            0 AS gmv_voh_eli_gui,
            0 AS gmv_voh_no_eli,
            0 AS gmv_no_voh,
            0 AS gmv_no_voh_eli,
            0 AS gmv_no_voh_eli_gui,
            0 AS gmv_no_voh_no_eli
        FROM t_trf_ads
        UNION ALL
        --<< BREAK POINT
        SELECT ds,
            user_contribution_tag,
            region,
            0 AS dau,
            0 AS dau_voh,
            0 AS dau_no_voh,
            0 AS pdp_uv,
            0 AS pdp_uv_voh,
            0 AS pdp_uv_no_voh,
            byr,
            byr_voh,
            byr_no_voh,
            ord,
            ord_eli,
            ord_eli_gui,
            ord_no_eli,
            ord_voh,
            ord_voh_eli,
            ord_voh_eli_gui,
            ord_voh_no_eli,
            ord_no_voh,
            ord_no_voh_eli,
            ord_no_voh_eli_gui,
            ord_no_voh_no_eli,
            gmv,
            gmv_eli,
            gmv_eli_gui,
            gmv_no_eli,
            gmv_voh,
            gmv_voh_eli,
            gmv_voh_eli_gui,
            gmv_voh_no_eli,
            gmv_no_voh,
            gmv_no_voh_eli,
            gmv_no_voh_eli_gui,
            gmv_no_voh_no_eli
        FROM t_trn_ads
    ) AS t1
    LEFT JOIN (
        SELECT ds_day AS ds,
            MAX(ds_day) OVER (PARTITION BY text_week) AS week_end,
            MIN(ds_day) OVER (PARTITION BY text_week) AS week_start
        FROM lazada_cdm.dim_lzd_date
        WHERE 1 = 1
            AND ds_day BETWEEN 20240401 AND TO_CHAR(DATEADD(GETDATE(), + 2, 'mm'), 'yyyymmdd')
        GROUP BY ds_day,
            text_week
    ) AS t2 ON t1.ds = t2.ds
    LEFT JOIN (
        SELECT ds,
            day_type,
            camp_type AS camp_type_original,
            CASE
                WHEN TOLOWER(camp_type) IN ('mega') THEN 'Mega'
                WHEN TOLOWER(camp_type) IN ('pd') THEN 'A+'
                WHEN TOLOWER(camp_type) IN ('holiday') THEN 'BAU'
                ELSE camp_type
            END AS camp_type_fixed
        FROM lazada_analyst.cp_calendar
        WHERE 1 = 1
            AND ds BETWEEN 20240401 AND TO_CHAR(GETDATE(), 'yyyymmdd')
    ) AS t3 ON t1.ds = t3.ds
GROUP BY t1.ds,
    CONCAT(
        'Week:',
        ' ',
        t2.week_start,
        ' ',
        '-->>',
        ' ',
        t2.week_end
    ),
    t3.camp_type_fixed,
    t1.user_contribution_tag,
    t1.region
ORDER BY t1.ds ASC,
    t1.user_contribution_tag ASC,
    t1.region ASC;
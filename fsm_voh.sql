WITH t_dau_voh AS (
    SELECT t1.ds AS ds,
        t1.utdid AS utdid,
        CASE
            WHEN t2.url_type = 'ipv' THEN t1.utdid
            ELSE NULL
        END AS utdid_ipv,
        t1.user_id AS user_id,
        CASE
            WHEN t2.url_type = 'ipv' THEN t1.user_id
            ELSE NULL
        END AS user_id_ipv,
        COALESCE(t3.region, 'Non Metro') AS region,
        COALESCE(t4.user_contribution_tag, 'Non User') AS user_contribution_tag,
        MAX(
            CASE
                WHEN t5.promotion_id IS NOT NULL THEN 1
                ELSE 0
            END
        ) AS is_voh
    FROM (
            SELECT ds,
                active_utdid AS utdid,
                user_id
            FROM alilog.dws_lzd_user_track
            WHERE 1 = 1
                AND ds BETWEEN 20240701 AND 20240707
                AND venture = 'VN' --
                AND TOLOWER(sg_udf :bi_get_aplus_appinfo(app_id, 'code')) IN ('lazada') --<< Data tech function, restricted use
                -- AND     app_id IN ('23867946@aliyunos','23867946@android','23868882@ipad','23868882@iphoneos')
                AND active_utdid IS NOT NULL
        ) AS t1
        LEFT JOIN (
            SELECT *
            FROM lazada_cdm.dws_lzd_mkt_app_ad_mp_di
            WHERE 1 = 1
                AND ds BETWEEN 20240701 AND 20240707
                AND venture = 'VN'
                AND attr_model IN ('ft_1d_np')
                AND is_active_utdid = 1
                AND utdid IS NOT NULL
        ) AS t2 ON t1.ds = t2.ds
        AND t1.utdid = t2.utdid
        LEFT JOIN (
            SELECT utdid,
                user_id,
                region2,
                CASE
                    WHEN region2 LIKE '%Hồ Chí Minh%' THEN 'Metro'
                    WHEN region2 LIKE '%Hà Nội%' THEN 'Metro'
                    WHEN region2 LIKE '%Đà Nẵng%' THEN 'Metro'
                    WHEN region2 LIKE '%Cần Thơ%' THEN 'Metro plus'
                    WHEN region2 LIKE '%Hải Phòng%' THEN 'Metro plus'
                    WHEN region2 LIKE '%Bắc Ninh%' THEN 'Metro plus'
                    WHEN region2 LIKE '%Bình Dương%' THEN 'Metro plus'
                    WHEN region2 LIKE '%Đồng Nai%' THEN 'Metro plus'
                    WHEN region2 LIKE '%Long An%' THEN 'Metro plus'
                    WHEN region2 LIKE '%Khánh Hòa%' THEN 'Metro plus'
                    ELSE 'Non Metro'
                END AS region
            FROM lazada_cdm.dim_lzd_usr
            WHERE 1 = 1
                AND ds = MAX_PT("lazada_cdm.dim_lzd_usr")
                AND venture = 'VN'
        ) AS t3 ON t1.user_id = t3.user_id
        LEFT JOIN (
            SELECT TO_CHAR(
                    DATEADD(TO_DATE(ds_month, 'yyyymm'), + 1, 'mm'),
                    'yyyymm'
                ) AS ds_month,
                user_id,
                user_contribution_tag
            FROM lazada_ads.dws_lzd_cdp_usr_uid_contribute_value_v2
            WHERE 1 = 1
                AND ds_month = MAX_PT(
                    "lazada_ads.dws_lzd_cdp_usr_uid_contribute_value_v2"
                )
                AND venture = 'VN' --
                -- UNION ALL --<< Merge
                -- SELECT  ds_month
                --         ,user_id
                --         ,user_contribution_tag
                -- FROM    lazada_ads.dws_lzd_cdp_usr_uid_contribute_value_v2
                -- WHERE   1 = 1
                -- AND     ds_month >= 202401
                -- AND     venture = 'VN'
        ) AS t4 ON TO_CHAR(TO_DATE(t1.ds, 'yyyymmdd'), 'yyyymm') = t4.ds_month
        AND t1.user_id = t4.user_id
        LEFT JOIN (
            SELECT DISTINCT TO_CHAR(create_date, 'yyyymmdd') AS ds,
                user_id,
                promotion_id,
                collected_count,
                TO_CHAR(promotion_start_date, 'yyyymmdd') AS promo_start_ds,
                TO_CHAR(promotion_end_date, 'yyyymmdd') AS promo_end_ds
            FROM lazada_cdm.dim_lzd_pro_promotion_user
            WHERE 1 = 1
                AND ds = MAX_PT('lazada_cdm.dim_lzd_pro_promotion_user')
                AND TO_CHAR(create_date, 'yyyymmdd') >= 20240101 --<< Collect voucher date
                AND TO_CHAR(promotion_start_date, 'yyyymmdd') >= 20240101 --<< Fsmax có cycle rõ ràng
                -- AND     status IN ('Collected') --<< Only 2 values: Collected; Collected+Used
                AND venture = 'VN'
                AND promotion_id IN (
                    SELECT promotion_id
                    FROM lazada_cdm.dim_lzd_pro_collectibles
                    WHERE 1 = 1
                        AND ds = MAX_PT('lazada_cdm.dim_lzd_pro_collectibles')
                        AND venture = 'VN' --
                        -- AND     prom_sub_type IN ('FS Max') --
                        AND sponsor = 'platform'
                        AND product_code IN ('shippingFeeCoupon')
                        AND (
                            (
                                TOLOWER(voucher_name) LIKE '%platform_clm_seller boost program%'
                                OR TOLOWER(description) LIKE '%platform_clm_seller boost program%'
                            )
                            OR promotion_id IS NOT NULL
                        )
                )
        ) AS t5 ON t1.ds >= t5.ds
        AND t1.user_id = t5.user_id
        AND t1.ds BETWEEN t5.promo_start_ds AND t5.promo_end_ds
    GROUP BY t1.ds,
        t1.utdid,
        CASE
            WHEN t2.url_type = 'ipv' THEN t1.utdid
            ELSE NULL
        END,
        t1.user_id,
        CASE
            WHEN t2.url_type = 'ipv' THEN t1.user_id
            ELSE NULL
        END,
        t5.promotion_id,
        COALESCE(t3.region, 'Non Metro'),
        COALESCE(t4.user_contribution_tag, 'Non User')
)
SELECT t1.ds AS ds,
    t1.region AS region,
    t1.user_contribution_tag AS user_contribution_tag,
    COUNT(DISTINCT t1.utdid) AS dau,
    COUNT(
        DISTINCT CASE
            WHEN COALESCE(t1.is_voh, 0) = 1 THEN t1.utdid
            ELSE NULL
        END
    ) AS dau_with_voh,
    COUNT(
        DISTINCT CASE
            WHEN COALESCE(t1.is_voh, 0) = 0 THEN t1.utdid
            ELSE NULL
        END
    ) AS dau_without_voh,
    COUNT(DISTINCT t1.utdid_ipv) AS ipv_uv,
    COUNT(
        DISTINCT CASE
            WHEN COALESCE(t1.is_voh, 0) = 1 THEN t1.utdid_ipv
            ELSE NULL
        END
    ) AS ipv_uv_with_voh,
    COUNT(
        DISTINCT CASE
            WHEN COALESCE(t1.is_voh, 0) = 0 THEN t1.utdid_ipv
            ELSE NULL
        END
    ) AS ipv_uv_without_voh,
    COUNT(DISTINCT t2.buyer_id) AS buyer,
    COUNT(
        DISTINCT CASE
            WHEN COALESCE(t1.is_voh, 0) = 1 THEN t2.buyer_id
            ELSE NULL
        END
    ) AS buyer_with_voh,
    COUNT(
        DISTINCT CASE
            WHEN COALESCE(t1.is_voh, 0) = 0 THEN t2.buyer_id
            ELSE NULL
        END
    ) AS buyer_without_voh,
    SUM(t2.order_id_cnt) AS order_id_cnt,
    SUM(t2.order_id_eligible_cnt) AS order_id_eligible_cnt,
    SUM(t2.order_id_eligible_guided_cnt) AS order_id_eligible_guided_cnt,
    SUM(t2.order_id_not_eligible_cnt) AS order_id_not_eligible_cnt,
    SUM(
        CASE
            WHEN COALESCE(t1.is_voh, 0) = 1 THEN t2.order_id_cnt
            ELSE 0
        END
    ) AS order_id_cnt_with_voh,
    SUM(
        CASE
            WHEN COALESCE(t1.is_voh, 0) = 1 THEN t2.order_id_eligible_cnt
            ELSE 0
        END
    ) AS order_id_cnt_with_voh_eligible,
    SUM(
        CASE
            WHEN COALESCE(t1.is_voh, 0) = 1 THEN t2.order_id_eligible_guided_cnt
            ELSE 0
        END
    ) AS order_id_cnt_with_voh_eligible_guided,
    SUM(
        CASE
            WHEN COALESCE(t1.is_voh, 0) = 1 THEN t2.order_id_not_eligible_cnt
            ELSE 0
        END
    ) AS order_id_cnt_with_voh_not_eligible,
    SUM(
        CASE
            WHEN COALESCE(t1.is_voh, 0) = 0 THEN t2.order_id_cnt
            ELSE 0
        END
    ) AS order_id_cnt_without_voh,
    SUM(
        CASE
            WHEN COALESCE(t1.is_voh, 0) = 0 THEN t2.order_id_eligible_cnt
            ELSE 0
        END
    ) AS order_id_cnt_without_voh_eligible,
    SUM(
        CASE
            WHEN COALESCE(t1.is_voh, 0) = 0 THEN t2.order_id_eligible_guided_cnt
            ELSE 0
        END
    ) AS order_id_cnt_without_voh_eligible_guided,
    SUM(
        CASE
            WHEN COALESCE(t1.is_voh, 0) = 0 THEN t2.order_id_not_eligible_cnt
            ELSE 0
        END
    ) AS order_id_cnt_without_voh_not_eligible,
    SUM(t2.gmv) AS gmv,
    SUM(t2.gmv_eligible) AS gmv_eligible,
    SUM(t2.gmv_eligible_guided) AS gmv_eligible_guided,
    SUM(t2.gmv_not_eligible) AS gmv_not_eligible,
    SUM(
        CASE
            WHEN COALESCE(t1.is_voh, 0) = 1 THEN t2.gmv
            ELSE 0
        END
    ) AS gmv_with_voh,
    SUM(
        CASE
            WHEN COALESCE(t1.is_voh, 0) = 1 THEN t2.gmv_eligible
            ELSE 0
        END
    ) AS gmv_with_voh_eligible,
    SUM(
        CASE
            WHEN COALESCE(t1.is_voh, 0) = 1 THEN t2.gmv_eligible_guided
            ELSE 0
        END
    ) AS gmv_with_voh_eligible_guided,
    SUM(
        CASE
            WHEN COALESCE(t1.is_voh, 0) = 1 THEN t2.gmv_not_eligible
            ELSE 0
        END
    ) AS gmv_with_voh_not_eligible,
    SUM(
        CASE
            WHEN COALESCE(t1.is_voh, 0) = 0 THEN t2.gmv
            ELSE 0
        END
    ) AS gmv_without_voh,
    SUM(
        CASE
            WHEN COALESCE(t1.is_voh, 0) = 0 THEN t2.gmv_eligible
            ELSE 0
        END
    ) AS gmv_without_voh_eligible,
    SUM(
        CASE
            WHEN COALESCE(t1.is_voh, 0) = 0 THEN t2.gmv_eligible_guided
            ELSE 0
        END
    ) AS gmv_without_voh_eligible_guided,
    SUM(
        CASE
            WHEN COALESCE(t1.is_voh, 0) = 0 THEN t2.gmv_not_eligible
            ELSE 0
        END
    ) AS gmv_without_voh_eligible_not_eligible
FROM (
        SELECT ds,
            utdid,
            utdid_ipv,
            user_id,
            user_id_ipv,
            region,
            user_contribution_tag,
            MAX(is_voh) AS is_voh --<< Whether used or not
        FROM t_dau_voh
        GROUP BY ds,
            utdid,
            utdid_ipv,
            user_id,
            user_id_ipv,
            region,
            user_contribution_tag
    ) AS t1
    LEFT JOIN (
        SELECT t1.ds AS ds,
            t1.usertrack_id AS utdid,
            t1.buyer_id AS buyer_id,
            COUNT(DISTINCT t1.order_id) AS order_id_cnt,
            COUNT(
                DISTINCT CASE
                    WHEN t2.seller_id IS NOT NULL THEN t1.order_id
                    ELSE NULL
                END
            ) AS order_id_eligible_cnt,
            COUNT(
                DISTINCT CASE
                    WHEN t2.seller_id IS NOT NULL
                    AND t3.sales_order_item_id IS NOT NULL THEN t1.order_id
                    ELSE NULL
                END
            ) AS order_id_eligible_guided_cnt,
            COUNT(
                DISTINCT CASE
                    WHEN t2.seller_id IS NULL THEN t1.order_id
                    ELSE NULL
                END
            ) AS order_id_not_eligible_cnt,
            SUM(t1.actual_gmv * t1.exchange_rate) AS gmv,
            SUM(
                CASE
                    WHEN t2.seller_id IS NOT NULL THEN t1.actual_gmv * t1.exchange_rate
                    ELSE 0
                END
            ) AS gmv_eligible,
            SUM(
                CASE
                    WHEN t2.seller_id IS NOT NULL
                    AND t3.sales_order_item_id IS NOT NULL THEN t1.actual_gmv * t1.exchange_rate
                    ELSE 0
                END
            ) AS gmv_eligible_guided,
            SUM(
                CASE
                    WHEN t2.seller_id IS NULL THEN t1.actual_gmv * t1.exchange_rate
                    ELSE 0
                END
            ) AS gmv_not_eligible
        FROM (
                SELECT *
                FROM lazada_cdm.dwd_lzd_trd_core_fulfill_di
                WHERE 1 = 1
                    AND ds BETWEEN 20240701 AND 20240707
                    AND venture = 'VN'
                    AND is_revenue = 1
                    AND COALESCE(business_application, 'LZD') IN ('LZD,ZAL', 'LZD')
            ) AS t1
            LEFT JOIN (
                SELECT ds,
                    seller_id
                FROM lazada_cdm.dwd_lzd_pro_fsmax_program_seller_detail_df
                WHERE 1 = 1 --
                    -- AND     ds = MAX_PT("lazada_cdm.dwd_lzd_pro_fsmax_program_seller_detail_df") --<< Get full list within 6 months rolling
                    AND ds >= TO_CHAR(
                        TO_DATE(join_time, 'yyyy-mm-dd hh:mi:ss'),
                        'yyyymmdd'
                    ) --<< Joined
                    AND ds BETWEEN 20240701 AND 20240707
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
            ) AS t2 ON t1.ds = t2.ds
            AND t1.seller_id = t2.seller_id
            LEFT JOIN (
                SELECT DISTINCT sales_order_item_id
                FROM lazada_cdm.dwd_lzd_pro_promotion_item_di
                WHERE 1 = 1
                    AND ds >= 20240601
                    AND venture = 'VN'
                    AND is_fulfilled = 1
                    AND promotion_id IN (
                        SELECT DISTINCT promotion_id
                        FROM t_dau_voh
                    )
            ) AS t3 ON t1.sales_order_item_id = t3.sales_order_item_id
        GROUP BY t1.ds,
            t1.usertrack_id,
            t1.buyer_id
    ) AS t2 ON t1.ds = t2.ds
    AND t1.utdid = t2.utdid
GROUP BY t1.ds,
    t1.region,
    t1.user_contribution_tag;
-- MaxCompute SQL 
-- ********************************************************************--
-- author:Truong, Van Thanh
-- create time:2024-07-03 12:49:33
-- ********************************************************************--
SELECT COALESCE(seller_id, '00_all') AS seller_id,
    COUNT(DISTINCT member_id) AS partner,
    COUNT(
        DISTINCT CASE
            WHEN member_id IN (
                SELECT member_id
                FROM lazada_analyst.loutruong_aff_mem_id_offline_info
                WHERE 1 = 1
                    AND month_apply IN (
                        SELECT MAX(month_apply)
                        FROM lazada_analyst.loutruong_aff_mem_id_offline_info
                    )
                    AND member_pic_local IN ('UM')
            ) THEN member_id
            ELSE NULL
        END
    ) AS partner_um
FROM (
        SELECT DISTINCT CAST(seller_id AS BIGINT) AS seller_id,
            member_id
        FROM lazada_analyst_dev.loutruong_aff_console_di
        WHERE 1 = 1
            AND TO_CHAR(TO_DATE(ds, 'yyyymmdd'), 'yyyy') = 2024
            AND seller_id IN (
                1000126669,
                1000242544,
                200163633036,
                200164011536,
                1000088919,
                1000285841,
                200165613249,
                1000126660,
                1000132285,
                200239005190,
                200172714133,
                1000070193,
                200371728558,
                1000127552,
                1000035129,
                200168937086,
                200169894299,
                100018952,
                1000126624,
                200159784007,
                200167155234,
                1000175163,
                1000175282,
                1000035113
            )
        UNION ALL
        --<< BREAK POINT
        SELECT DISTINCT seller_id,
            member_id
        FROM lazada_cdm.dwd_lzd_mkt_a2c_uam_core_di
        WHERE 1 = 1 --
            AND TO_CHAR(TO_DATE(ds, 'yyyymmdd'), 'yyyy') = 2024
            AND venture = 'VN'
            AND attr_model IN ('lt_1d_p') --
            AND TOLOWER(
                COALESCE(
                    lazada_cdm.mkt_get_updated_funding_bucket(
                        channel,
                        GET_JSON_OBJECT(campaign_info, '$.funding_bucket'),
                        partner
                    ),
                    'Unknown'
                )
            ) IN ('lazada om')
            AND TOLOWER(
                COALESCE(
                    lazada_cdm.mkt_get_updated_funding_type(
                        channel,
                        GET_JSON_OBJECT(campaign_info, '$.funding_bucket'),
                        GET_JSON_OBJECT(campaign_info, '$.funding_type'),
                        partner
                    ),
                    'Unknown'
                )
            ) IN ('om', 'ams')
            AND TOLOWER(
                lazada_cdm.mkt_get_sub_channel_from_json(
                    sg_udf :bi_put_json_values(
                        '{}',
                        'channel',
                        sg_udf :bi_to_json_string(channel),
                        'funding_bucket',
                        sg_udf :bi_to_json_string(
                            lazada_cdm.mkt_get_updated_funding_bucket(
                                channel,
                                COALESCE(
                                    GET_JSON_OBJECT(campaign_info, '$.funding_bucket'),
                                    'Unknown'
                                ),
                                partner
                            )
                        ),
                        'free_paid',
                        sg_udf :bi_to_json_string('Paid'),
                        'segmentation',
                        sg_udf :bi_to_json_string(segmentation),
                        'rt_bucket',
                        sg_udf :bi_to_json_string(
                            COALESCE(
                                GET_JSON_OBJECT(campaign_info, '$.rt_bucket'),
                                'Unknown'
                            )
                        ),
                        'campaign_type',
                        sg_udf :bi_to_json_string(
                            COALESCE(
                                GET_JSON_OBJECT(campaign_info, '$.campaign_type'),
                                'Unknown'
                            )
                        ),
                        'placement',
                        sg_udf :bi_to_json_string(placement)
                    )
                )
            ) IN ('cps affiliate')
            AND seller_id IN (
                1000126669,
                1000242544,
                200163633036,
                200164011536,
                1000088919,
                1000285841,
                200165613249,
                1000126660,
                1000132285,
                200239005190,
                200172714133,
                1000070193,
                200371728558,
                1000127552,
                1000035129,
                200168937086,
                200169894299,
                100018952,
                1000126624,
                200159784007,
                200167155234,
                1000175163,
                1000175282,
                1000035113
            )
    )
GROUP BY CUBE(seller_id);
WITH t_prt_um_pool_master AS (
    SELECT DISTINCT member_id
    FROM lazada_analyst.loutruong_aff_mem_id_offline_info
    WHERE 1 = 1
        AND month_apply IN (
            SELECT MAX(month_apply)
            FROM lazada_analyst.loutruong_aff_mem_id_offline_info
        )
        AND member_pic_local IN ('UM')
)
SELECT t1.member_id AS member_id,
    t5.member_name AS member_name,
    t1.seller_id AS seller_id,
    t6.seller_name AS seller_name,
    CASE
        WHEN t3.general_phone IS NULL
        OR t3.general_phone = '' THEN t4.general_phone
        ELSE t3.general_phone
    END AS phone
FROM (
        SELECT *
        FROM (
                SELECT member_id,
                    seller_id,
                    order_cnt,
                    ROW_NUMBER() OVER (
                        PARTITION BY member_id
                        ORDER BY order_cnt DESC
                    ) AS r
                FROM (
                        SELECT member_id,
                            seller_id,
                            COUNT(DISTINCT order_id) AS order_cnt
                        FROM lazada_analyst_dev.loutruong_aff_console_di
                        WHERE 1 = 1
                            AND TO_CHAR(TO_DATE(ds, 'yyyymmdd'), 'yyyy') = 2024
                            AND seller_id IN (
                                1000494382,
                                1000126669,
                                1000242544,
                                200163633036,
                                200164011536,
                                1000088919,
                                1000288791,
                                100232407,
                                200168226009,
                                1000495523,
                                1000132285,
                                200230437197,
                                200166930857,
                                200168688064,
                                1000010262,
                                1000248565,
                                1000047470,
                                1000035115,
                                200158218693,
                                1000014072,
                                14449,
                                200174199350,
                                1000127552,
                                200238832509,
                                1000476516,
                                200177997108,
                                100169489,
                                200299536090,
                                16440,
                                1000126624,
                                200166720045,
                                200160777513,
                                200167155234,
                                200158167677,
                                200166057331,
                                1000035113
                            )
                            AND member_id IN (
                                SELECT *
                                FROM t_prt_um_pool_master
                            )
                        GROUP BY member_id,
                            seller_id
                    )
            )
        WHERE 1 = 1
            AND r = 1
    ) AS t1
    LEFT JOIN (
        SELECT member_id,
            general_phone_console AS general_phone
        FROM lazada_analyst_dev.tmp_loutruong_1
        WHERE 1 = 1
            AND data_label = 'techno'
    ) AS t3 ON t1.member_id = t3.member_id
    LEFT JOIN (
        SELECT member_id,
            general_phone_techno AS general_phone
        FROM lazada_analyst_dev.tmp_loutruong_1
        WHERE 1 = 1
            AND data_label = 'techno'
    ) AS t4 ON t1.member_id = t4.member_id
    LEFT JOIN (
        SELECT member_id,
            name AS member_name
        FROM lazada_ads_data.ads_lzd_affiliate_account_audit_vn
        WHERE 1 = 1
            AND ds = MAX_PT(
                'lazada_ads_data.ads_lzd_affiliate_account_audit'
            )
            AND venture = 'VN'
    ) AS t5 ON t1.member_id = t5.member_id
    LEFT JOIN (
        SELECT seller_id,
            seller_name
        FROM lazada_cdm.dim_lzd_slr_seller_vn
        WHERE 1 = 1
            AND ds = MAX_PT('lazada_cdm.dim_lzd_slr_seller')
            AND venture = 'VN'
    ) AS t6 ON t1.seller_id = t6.seller_id;
WITH t_prt_um_pool_master AS (
    SELECT DISTINCT member_id
    FROM lazada_analyst.loutruong_aff_mem_id_offline_info
    WHERE 1 = 1
        AND month_apply IN (
            SELECT MAX(month_apply)
            FROM lazada_analyst.loutruong_aff_mem_id_offline_info
        )
        AND member_pic_local IN ('UM')
)
SELECT t1.member_id AS member_id,
    t5.member_name AS member_name,
    t1.seller_id AS seller_id,
    t6.seller_name AS seller_name,
    CASE
        WHEN t3.general_phone IS NULL
        OR t3.general_phone = '' THEN t4.general_phone
        ELSE t3.general_phone
    END AS phone
FROM (
        SELECT *
        FROM (
                SELECT member_id,
                    seller_id,
                    a2c_uv,
                    ROW_NUMBER() OVER (
                        PARTITION BY member_id
                        ORDER BY a2c_uv DESC
                    ) AS r
                FROM (
                        SELECT member_id,
                            seller_id,
                            COUNT(DISTINCT buyer_id) AS a2c_uv
                        FROM lazada_cdm.dwd_lzd_mkt_a2c_uam_core_di
                        WHERE 1 = 1
                            AND TO_CHAR(TO_DATE(ds, 'yyyymmdd'), 'yyyy') = 2024
                            AND venture = 'VN'
                            AND attr_model IN ('lt_1d_p')
                            AND TOLOWER(
                                COALESCE(
                                    lazada_cdm.mkt_get_updated_funding_bucket(
                                        channel,
                                        GET_JSON_OBJECT(campaign_info, '$.funding_bucket'),
                                        partner
                                    ),
                                    'Unknown'
                                )
                            ) IN ('lazada om')
                            AND TOLOWER(
                                COALESCE(
                                    lazada_cdm.mkt_get_updated_funding_type(
                                        channel,
                                        GET_JSON_OBJECT(campaign_info, '$.funding_bucket'),
                                        GET_JSON_OBJECT(campaign_info, '$.funding_type'),
                                        partner
                                    ),
                                    'Unknown'
                                )
                            ) IN ('om', 'ams')
                            AND TOLOWER(
                                lazada_cdm.mkt_get_sub_channel_from_json(
                                    sg_udf :bi_put_json_values(
                                        '{}',
                                        'channel',
                                        sg_udf :bi_to_json_string(channel),
                                        'funding_bucket',
                                        sg_udf :bi_to_json_string(
                                            lazada_cdm.mkt_get_updated_funding_bucket(
                                                channel,
                                                COALESCE(
                                                    GET_JSON_OBJECT(campaign_info, '$.funding_bucket'),
                                                    'Unknown'
                                                ),
                                                partner
                                            )
                                        ),
                                        'free_paid',
                                        sg_udf :bi_to_json_string('Paid'),
                                        'segmentation',
                                        sg_udf :bi_to_json_string(segmentation),
                                        'rt_bucket',
                                        sg_udf :bi_to_json_string(
                                            COALESCE(
                                                GET_JSON_OBJECT(campaign_info, '$.rt_bucket'),
                                                'Unknown'
                                            )
                                        ),
                                        'campaign_type',
                                        sg_udf :bi_to_json_string(
                                            COALESCE(
                                                GET_JSON_OBJECT(campaign_info, '$.campaign_type'),
                                                'Unknown'
                                            )
                                        ),
                                        'placement',
                                        sg_udf :bi_to_json_string(placement)
                                    )
                                )
                            ) IN ('cps affiliate')
                            AND seller_id IN (
                                1000494382,
                                1000126669,
                                1000242544,
                                200163633036,
                                200164011536,
                                1000088919,
                                1000288791,
                                100232407,
                                200168226009,
                                1000495523,
                                1000132285,
                                200230437197,
                                200166930857,
                                200168688064,
                                1000010262,
                                1000248565,
                                1000047470,
                                1000035115,
                                200158218693,
                                1000014072,
                                14449,
                                200174199350,
                                1000127552,
                                200238832509,
                                1000476516,
                                200177997108,
                                100169489,
                                200299536090,
                                16440,
                                1000126624,
                                200166720045,
                                200160777513,
                                200167155234,
                                200158167677,
                                200166057331,
                                1000035113
                            )
                        GROUP BY member_id,
                            seller_id
                    )
            )
        WHERE 1 = 1
            AND r = 1
    ) AS t1
    LEFT JOIN (
        SELECT member_id,
            general_phone_console AS general_phone
        FROM lazada_analyst_dev.tmp_loutruong_1
        WHERE 1 = 1
            AND data_label = 'techno'
    ) AS t3 ON t1.member_id = t3.member_id
    LEFT JOIN (
        SELECT member_id,
            general_phone_techno AS general_phone
        FROM lazada_analyst_dev.tmp_loutruong_1
        WHERE 1 = 1
            AND data_label = 'techno'
    ) AS t4 ON t1.member_id = t4.member_id
    LEFT JOIN (
        SELECT member_id,
            name AS member_name
        FROM lazada_ads_data.ads_lzd_affiliate_account_audit_vn
        WHERE 1 = 1
            AND ds = MAX_PT(
                'lazada_ads_data.ads_lzd_affiliate_account_audit'
            )
            AND venture = 'VN'
    ) AS t5 ON t1.member_id = t5.member_id
    LEFT JOIN (
        SELECT seller_id,
            seller_name
        FROM lazada_cdm.dim_lzd_slr_seller_vn
        WHERE 1 = 1
            AND ds = MAX_PT('lazada_cdm.dim_lzd_slr_seller')
            AND venture = 'VN'
    ) AS t6 ON t1.seller_id = t6.seller_id;
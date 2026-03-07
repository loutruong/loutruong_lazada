-- MaxCompute SQL 
-- ********************************************************************--
-- author:Truong, Van Thanh
-- create time:2024-05-13 19:21:25
-- ********************************************************************--
--@@ Input = lazada_ods.s_lzd_btp_campaign_template_df
--@@ Input = lazada_ods.s_lzd_btp_campaign_df
--@@ Input = lazada_ods.s_lazada_funding_bucket_dict_df
--@@ Input = lazada_ods.s_lazada_funding_type_dict_df
--@@ Input = lazada_ods.s_lazada_campaign_type_dict_df
SELECT DISTINCT offer_id,
    offer_name,
    campaign_id,
    funding_bucket,
    funding_type
FROM (
        SELECT CONCAT(
                'loutruong',
                'vn',
                'lzdaffilaite',
                '_',
                t2.campaign_id,
                '_',
                t1.offer_id
            ) AS loutruong_id,
CASE
                WHEN t1.campaign_id IN ('101060000151001') THEN 1
                ELSE 0
            END AS is_campaign_all_in,
            t2.campaign_id AS campaign_id,
            t2.campaign_name AS campaign_name,
            t1.offer_id AS offer_id,
            t1.offer_name AS offer_name,
            t1.offer_descrption AS offer_description,
            t1.visibility AS offer_on_off,
            t1.link AS offer_link,
            t1.offer_type AS offer_type,
            t1.ext_url_enable AS ext_url_enable,
            TO_CHAR(t1.offer_create, 'yyyymmdd') AS offer_create,
            TO_CHAR(t1.offer_modified, 'yyyymmdd') AS offer_modified,
            TO_CHAR(t1.offer_start, 'yyyymmdd') AS offer_start,
            TO_CHAR(t1.offer_end, 'yyyymmdd') AS offer_end,
            t3.campaign_type AS campaign_type,
            t4.funding_bucket AS funding_bucket,
            t5.funding_type AS funding_type
        FROM (
                SELECT campaign_id,
                    offer_id,
                    offer_pic,
                    offer_type,
                    offer_name,
                    offer_descrption,
                    visibility,
                    gmt_create AS offer_create,
                    gmt_modified AS offer_modified,
CASE
                        WHEN venture IN ('VN', 'TH', 'ID') THEN sg_udf :bi_changetimezone(FROM_UNIXTIME(starts / 1000), 'GMT+8', 'GMT+7')
                        ELSE FROM_UNIXTIME(starts / 1000)
                    END AS offer_start,
CASE
                        WHEN venture IN ('VN', 'TH', 'ID') THEN sg_udf :bi_changetimezone(FROM_UNIXTIME(expires / 1000), 'GMT+8', 'GMT+7')
                        ELSE FROM_UNIXTIME(expires / 1000)
                    END AS offer_end,
                    link,
                    payout_rule,
                    create_type,
                    creative_files,
                    os,
                    ext_url_enable,
                    bus_type,
                    is_test
                FROM lazada_ods.s_lzd_btp_campaign_template_df --<< Offer
                WHERE 1 = 1
                    AND ds = MAX_PT('lazada_ods.s_lzd_btp_campaign_template_df')
                    AND venture = 'VN'
                    AND is_test = 0
            ) AS t1
            INNER JOIN (
                SELECT campaign_id,
                    campaign_name,
                    gmt_create AS campaign_create,
                    gmt_modified AS campaign_modified,
CASE
                        WHEN venture IN ('VN', 'TH', 'ID') THEN sg_udf :bi_changetimezone(FROM_UNIXTIME(starts / 1000), 'GMT+8', 'GMT+7')
                        ELSE FROM_UNIXTIME(starts / 1000)
                    END AS campaign_start,
CASE
                        WHEN venture IN ('VN', 'TH', 'ID') THEN sg_udf :bi_changetimezone(FROM_UNIXTIME(expires / 1000), 'GMT+8', 'GMT+7')
                        ELSE FROM_UNIXTIME(expires / 1000)
                    END AS campaign_end,
                    campaign_type,
                    funding_bucket,
                    funding_type,
                    category,
                    advertizer,
                    cid_p,
                    budget,
                    audience,
                    audience_id,
                    social_ad_type,
                    display_video_format,
                    display_targeting,
                    sem_device,
                    sem_merchant,
                    sem_match_type,
                    sem_position,
                    advertiser_id,
                    advertiser_type
                FROM lazada_ods.s_lzd_btp_campaign_df --<< Campaign
                WHERE 1 = 1
                    AND ds = MAX_PT('lazada_ods.s_lzd_btp_campaign_df')
                    AND venture = 'VN'
            ) AS t2 ON t1.campaign_id = t2.campaign_id
            LEFT JOIN (
                SELECT type AS campaign_type,
                    value
                FROM lazada_ods.s_lazada_campaign_type_dict_df
                WHERE 1 = 1
                    AND ds = MAX_PT('lazada_ods.s_lazada_campaign_type_dict_df')
            ) AS t3 ON t2.campaign_type = t3.value
            LEFT JOIN (
                SELECT type AS funding_bucket,
                    value
                FROM lazada_ods.s_lazada_funding_bucket_dict_df
                WHERE 1 = 1
                    AND ds = MAX_PT('lazada_ods.s_lazada_funding_bucket_dict_df')
            ) AS t4 ON t2.funding_bucket = t4.value
            LEFT JOIN (
                SELECT type AS funding_type,
                    value
                FROM lazada_ods.s_lazada_funding_type_dict_df
                WHERE 1 = 1
                    AND ds = MAX_PT('lazada_ods.s_lazada_funding_type_dict_df')
            ) AS t5 ON t2.funding_type = t5.value
    )
WHERE 1 = 1
    AND offer_modified > 20240101;
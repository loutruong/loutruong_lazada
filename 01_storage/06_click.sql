-- MaxCompute SQL 
-- ********************************************************************--
-- author:Truong, Van Thanh
-- create time:2024-05-12 11:44:33
-- ********************************************************************--
--@@ Input = lazada_cdm.dwd_lzd_clickserver_log_di
--@@ Output = lazada_analyst_dev.loutruong_dwd_lzd_clickserver_log_di
-- DROP TABLE IF EXISTS lazada_analyst_dev.loutruong_dwd_lzd_clickserver_log_di
-- ;
-- CREATE TABLE IF NOT EXISTS lazada_analyst_dev.loutruong_dwd_lzd_clickserver_log_di
-- (
--     tracking_type         STRING
--     ,clickid              STRING
--     ,old_clickid          STRING
--     ,inner_clickid        STRING
--     ,is_app               BIGINT
--     ,site                 STRING
--     ,file_path            STRING
--     ,log_time             DATETIME
--     ,click_timestamp_unix STRING
--     ,click_timestamp      DATETIME
--     ,client_ip            STRING
--     ,user_agent           STRING
--     ,domain               STRING
--     ,click_url            STRING
--     ,referer_url          STRING
--     ,referer_host         STRING
--     ,source_origin        STRING
--     ,source_fixed         STRING
--     ,lp_url               STRING
--     ,bm                   BIGINT
--     ,pricing_model        STRING
--     ,busi_type            STRING
--     ,pid                  STRING
--     ,member_id            STRING
--     ,site_id              STRING
--     ,adzone_id            STRING
--     ,sub_aff_id           STRING
--     ,sub_id1              STRING
--     ,sub_id2              STRING
--     ,sub_id3              STRING
--     ,sub_id4              STRING
--     ,sub_id5              STRING
--     ,sub_id6              STRING
--     ,offer_id             STRING
--     ,redirect_offer_id    STRING
--     ,group_id             STRING
--     ,creative_id          STRING
--     ,crow_id              STRING
--     ,query                STRING
--     ,mkttid               STRING
--     ,miid                 STRING
--     ,cna                  STRING
--     ,anonymous_id         STRING
--     ,adid                 STRING
--     ,utdid                STRING
--     ,buyer_id             STRING
--     ,eagleeyeid           STRING
--     ,channel_pkg_token    STRING
--     ,param_upload_mode    STRING
--     ,extra                STRING
--     ,app_key              STRING
--     ,os                   STRING
--     ,os_version           STRING
--     ,device_type          STRING
--     ,ex_track_info        STRING
--     ,mm_offer_id          STRING
--     ,mm_campaign_id       STRING
--     ,product_id           STRING
--     ,sku_id               STRING
--     ,ext_info             STRING
--     ,exlaz                STRING
--     ,gateway_info         STRING
-- )
-- COMMENT 'Light version of table upstream click server lazada_cdm.dwd_lzd_clickserver_log_di'
-- PARTITIONED BY 
-- (
--     ds                    STRING
-- )
-- LIFECYCLE 3600
-- ;
INSERT OVERWRITE TABLE lazada_analyst_dev.loutruong_dwd_lzd_clickserver_log_di PARTITION (ds)
SELECT tracking_type,
    clickid,
    old_clickid,
    inner_clickid,
    IF(LENGTH(utdid) > 0, 1, 0) AS is_app,
    site,
    file_path,
    log_time,
    click_timestamp AS click_timestamp_unix,
    FROM_UNIXTIME(CAST(click_timestamp AS BIGINT) / 1000) AS click_timestamp,
    client_ip,
    user_agent,
    domain,
    click_url,
    referer AS referer_url,
    SUBSTRING_INDEX(SUBSTRING_INDEX(referer, '/', 3), '//', -1) AS referer_host,
    source AS source_origin,
CASE
        WHEN source IN (00) THEN 'Affiliate'
        WHEN source IN (10) THEN 'Google Search APP Re-engagement'
        WHEN source IN (11) THEN 'Google SEM'
        WHEN source IN (12) THEN 'Google PLA'
        WHEN source IN (13) THEN 'Google DSA'
        WHEN source IN (14) THEN 'Google YouTube'
        WHEN source IN (15) THEN 'Google GDN'
        WHEN source IN (16) THEN 'Google MPA'
        WHEN source IN (20) THEN 'Facebook DPA-APP RT'
        WHEN source IN (21) THEN 'Facebook Engagement'
        WHEN source IN (30) THEN 'Bing SEM'
        WHEN source IN (40) THEN 'Coccoc SEM'
        WHEN source IN (50) THEN 'Tiktok'
        WHEN source IN (60) THEN 'Twittter'
        ELSE source
    END AS source_fixed,
    lp_url,
    bm,
CASE
        WHEN bm = 1 THEN 'CPC'
        WHEN bm = 2 THEN 'CPS'
        WHEN bm = 3 THEN 'CPM'
        WHEN bm = 4 THEN 'CPI'
        WHEN bm = 5 THEN 'CPD'
    END AS pricing_model,
    busi_type,
    pid,
    COALESCE(SPLIT(pid, '_') [1], '') AS member_id,
    COALESCE(SPLIT(pid, '_') [2], '') AS site_id,
    COALESCE(SPLIT(pid, '_') [3], '') AS adzone_id,
    sub_aff_id,
    sub_id1,
    sub_id2,
    sub_id3,
    sub_id4,
    sub_id5,
    sub_id6,
    offer_id,
    redirect_offer_id,
    group_id,
    creative_id,
    crow_id,
    query,
    mkttid,
    miid,
    cna,
    anonymous_id,
    adid,
    utdid,
    buyer_id,
    eagleeyeid,
    channel_pkg_token,
    param_upload_mode,
    extra,
    app_key,
    os,
    os_version,
    device_type,
    ex_track_info,
    GET_JSON_OBJECT(ex_track_info, '$.mmOfferId') mm_offer_id,
    GET_JSON_OBJECT(ex_track_info, '$.mmCampaignId') mm_campaign_id,
    GET_JSON_OBJECT(ex_track_info, '$.itemId') AS product_id,
    GET_JSON_OBJECT(ex_track_info, '$.skuId') AS sku_id,
    ext_info,
    exlaz,
    gateway_info,
    ds
FROM lazada_cdm.dwd_lzd_clickserver_log_di
WHERE 1 = 1 --
    -- AND     ds BETWEEN '${start_date}' AND '${end_date}'
    AND ds >= TO_CHAR(
        DATEADD(TO_DATE('${bizdate}', 'yyyymmdd'), -2, 'dd'),
        'yyyymmdd'
    ) --<< Daily
    AND venture = 'VN';
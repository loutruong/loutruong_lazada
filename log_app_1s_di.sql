--@@ Input = alilog.dwd_lzd_user_track
--@@ Input = lazada_cdm.dwd_lzd_log_ut_pv_flow_di
--@@ Output = lazada_analyst_dev.loutruong_log_app_di
-- DROP TABLE IF EXISTS lazada_analyst_dev.loutruong_log_app_di
-- ;
-- CREATE TABLE IF NOT EXISTS lazada_analyst_dev.loutruong_log_app_di
-- (
--     app_id             STRING COMMENT 'lazada'
--     ,client_code       STRING COMMENT 'lazada'
--     ,event_type        STRING COMMENT 'page, others'
--     ,event_id          STRING COMMENT '2001, 1010'
--     ,url_type          STRING
--     ,pre_flow_lvl2_ids STRING
--     ,url_flow_lvl2_ids STRING
--     ,utdid             STRING
--     ,row_label         DOUBLE
--     ,user_id           STRING
--     ,seller_id         BIGINT
--     ,shop_id           BIGINT
--     ,product_id        BIGINT
-- )
-- COMMENT 'Table contains all user log og VN | event_id IN 2001, 1010'
-- PARTITIONED BY 
-- (
--     ds                 STRING
-- )
-- LIFECYCLE 3600
-- ;
INSERT OVERWRITE TABLE lazada_analyst_dev.loutruong_log_app_di PARTITION (ds)
SELECT app_id,
    client_code,
    event_type,
    event_id,
    url_type,
    pre_flow_lvl2_ids,
    url_flow_lvl2_ids,
    utdid,
    ROW_NUMBER() OVER (
        PARTITION BY ds
        ORDER BY ds ASC
    ) AS row_label,
    user_id,
    seller_id,
    shop_id,
    product_id,
    ds
FROM (
        SELECT ds,
            app_id,
            TOLOWER(sg_udf :bi_get_aplus_appinfo(app_id, 'code')) AS client_code,
            'page' AS event_type,
            '2001' AS event_id,
            url_type,
            pre_flow_lvl2_ids,
            url_flow_lvl2_ids,
            utdid,
            user_id,
            seller_id,
            shop_id,
            asc_item_id AS product_id
        FROM lazada_cdm.dwd_lzd_log_ut_pv_flow_di
        WHERE 1 = 1 --
            -- AND     ds BETWEEN 20230301 AND ${bizdate} --<< Controller partition - Starting point, special partition
            AND ds >= TO_CHAR(
                DATEADD(TO_DATE($ { bizdate }, 'yyyymmdd'), -2, 'dd'),
                'yyyymmdd'
            ) --<< Controller partition - Daily operation
            AND venture = 'VN' --
            AND TOLOWER(sg_udf :bi_get_aplus_appinfo(app_id, 'code')) IN ('lazada') --<< Data tech function, restricted use
            -- AND     app_id IN ('23867946@aliyunos','23867946@android','23868882@ipad','23868882@iphoneos')
        UNION ALL
        SELECT t1.ds AS ds,
            t1.app_id AS app_id,
            t1.client_code AS client_code,
            t1.event_type AS event_type,
            t1.event_id AS event_id,
            t1.url_type AS url_type,
            sg_udf :GetUtFlowLvl2Ids(
                CASE
                    WHEN sg_udf :bi_app_spm(
                        '${bizdate}',
                        CONCAT('w-', t1.pre_spm_id),
                        app_id
                    ) IS NOT NULL
                    AND sg_udf :bi_spm('${bizdate}', CONCAT('w-', t1.pre_spm_id)) IS NOT NULL THEN CONCAT(
                        sg_udf :bi_app_spm(
                            '${bizdate}',
                            CONCAT('w-', t1.pre_spm_id),
                            app_id
                        ),
                        ',',
                        sg_udf :bi_spm('${bizdate}', CONCAT('w-', t1.pre_spm_id))
                    )
                    WHEN sg_udf :bi_app_spm(
                        '${bizdate}',
                        CONCAT('w-', t1.pre_spm_id),
                        app_id
                    ) IS NOT NULL
                    AND sg_udf :bi_spm('${bizdate}', CONCAT('w-', t1.pre_spm_id)) IS NULL THEN sg_udf :bi_app_spm(
                        '${bizdate}',
                        CONCAT('w-', t1.pre_spm_id),
                        app_id
                    )
                    ELSE sg_udf :bi_spm('${bizdate}', CONCAT('w-', t1.pre_spm_id))
                END,
                SPLIT_PART(t1.pre_spm_id, '.', 1, 3)
            ) AS pre_flow_lvl2_ids,
            sg_udf :GetUtFlowLvl2Ids(
                CASE
                    WHEN sg_udf :bi_app_spm(
                        '${bizdate}',
                        CONCAT('w-', t1.url_spm_id),
                        app_id
                    ) IS NOT NULL
                    AND sg_udf :bi_spm('${bizdate}', CONCAT('w-', t1.url_spm_id)) IS NOT NULL THEN CONCAT(
                        sg_udf :bi_app_spm(
                            '${bizdate}',
                            CONCAT('w-', t1.url_spm_id),
                            app_id
                        ),
                        ',',
                        sg_udf :bi_spm('${bizdate}', CONCAT('w-', t1.url_spm_id))
                    )
                    WHEN sg_udf :bi_app_spm(
                        '${bizdate}',
                        CONCAT('w-', t1.url_spm_id),
                        app_id
                    ) IS NOT NULL
                    AND sg_udf :bi_spm('${bizdate}', CONCAT('w-', t1.url_spm_id)) IS NULL THEN sg_udf :bi_app_spm(
                        '${bizdate}',
                        CONCAT('w-', t1.url_spm_id),
                        app_id
                    )
                    ELSE sg_udf :bi_spm('${bizdate}', CONCAT('w-', t1.url_spm_id))
                END,
                SPLIT_PART(t1.url_spm_id, '.', 1, 3)
            ) AS url_flow_lvl2_ids,
            t1.utdid AS utdid,
            t1.user_id AS user_id,
            t1.seller_id AS seller_id,
            t1.shop_id AS shop_id,
            t1.product_id AS product_id
        FROM (
                SELECT ds,
                    app_id,
                    TOLOWER(sg_udf :bi_get_aplus_appinfo(app_id, 'code')) AS client_code,
                    event_type,
                    event_id,
                    url_type,
                    pre_spm_id,
                    url_spm_id,
                    utdid,
                    user_id,
                    seller_id,
                    shop_id,
                    asc_item_id AS product_id
                FROM alilog.dwd_lzd_user_track
                WHERE 1 = 1 --
                    -- AND     ds BETWEEN 20230301 AND ${bizdate} --<< Controller partition - Starting point, special partition
                    AND ds >= TO_CHAR(
                        DATEADD(TO_DATE($ { bizdate }, 'yyyymmdd'), -2, 'dd'),
                        'yyyymmdd'
                    ) --<< Controller partition - Daily operation
                    AND venture = 'VN'
                    AND event_type IN ('others')
                    AND event_id IN ('1010') --
                    AND TOLOWER(sg_udf :bi_get_aplus_appinfo(app_id, 'code')) IN ('lazada') --<< Data tech function, restricted use
                    -- AND     app_id IN ('23867946@aliyunos','23867946@android','23868882@ipad','23868882@iphoneos')
            ) AS t1 LEFT ANTI
            JOIN (
                SELECT ds,
                    utdid
                FROM alilog.dwd_lzd_user_track
                WHERE 1 = 1 --
                    -- AND     ds BETWEEN 20230301 AND ${bizdate} --<< Controller partition - Starting point, special partition
                    AND ds >= TO_CHAR(
                        DATEADD(TO_DATE($ { bizdate }, 'yyyymmdd'), -2, 'dd'),
                        'yyyymmdd'
                    ) --<< Controller partition - Daily operation
                    AND venture = 'VN'
                    AND event_type IN ('page')
                    AND event_id IN ('2001') --
                    AND TOLOWER(sg_udf :bi_get_aplus_appinfo(app_id, 'code')) IN ('lazada') --<< Data tech function, restricted use
                    -- AND     app_id IN ('23867946@aliyunos','23867946@android','23868882@ipad','23868882@iphoneos')
            ) AS t2 ON t1.ds = t2.ds
            AND t1.utdid = t2.utdid
    );
--@@ Input = lazada_analyst_dev.loutruong_log_app_di
--@@ Input = lazada_cdm.dim_lzd_log_app_flow_info_flag
--@@ Input = lazada_cdm.dwd_lzd_trd_core_fulfill_di
--@@ Input = lazada_cdm.dim_lzd_prd_product
--@@ Input = lazada_analyst_dev.loutruong_utdid_clm_di
--@@ Output = lazada_analyst_dev.loutruong_log_1s_app_di
-- DROP TABLE IF EXISTS lazada_analyst_dev.loutruong_log_1s_app_di
-- ;
-- CREATE TABLE IF NOT EXISTS lazada_analyst_dev.loutruong_log_1s_app_di
-- (
--     int_channel_lv1  STRING COMMENT 'Internal channel level 1'
--     ,int_channel_lv2 STRING COMMENT 'Internal channel level 2'
--     ,cluster         STRING
--     ,bu_level1       STRING
--     ,bu_level2       STRING
--     ,cat_level1      STRING
--     ,clm_level       STRING
--     ,app_id          STRING
--     ,client_code     STRING
--     ,event_type      STRING
--     ,event_id        STRING
--     ,url_type        STRING
--     ,utdid           STRING
--     ,row_label       DOUBLE
--     ,account_id      STRING
--     ,buyer_id        STRING
--     ,order_id        STRING
--     ,product_id      BIGINT
--     ,seller_id       BIGINT
--     ,shop_id         BIGINT
-- )
-- COMMENT 'Table contains all the log user of LZD VN with 1 step guided'
-- PARTITIONED BY 
-- (
--     ds               STRING
-- )
-- LIFECYCLE 3600
-- ;
INSERT OVERWRITE TABLE lazada_analyst_dev.loutruong_log_1s_app_di PARTITION (ds)
SELECT TOLOWER(COALESCE(t2.int_channel_lv1, 'others')) AS int_channel_lv1,
    TOLOWER(COALESCE(t2.int_channel_lv2, 'others')) AS int_channel_lv2,
    CASE
        WHEN t6.cluster IS NOT NULL THEN TOLOWER(t6.cluster)
        ELSE 'unknown'
    END AS cluster,
    CASE
        WHEN t6.bu_level1 IS NOT NULL THEN TOLOWER(t6.bu_level1)
        ELSE 'unknown'
    END AS bu_level1,
    CASE
        WHEN t6.bu_level2 IS NOT NULL THEN TOLOWER(t6.bu_level2)
        ELSE 'unknown'
    END AS bu_level2,
    CASE
        WHEN t6.cat_level1 IS NOT NULL THEN TOLOWER(t6.cat_level1)
        ELSE 'unknown'
    END AS cat_level1,
    CASE
        WHEN t5.clm_level IS NOT NULL THEN t5.clm_level
        ELSE 'Guest'
    END AS clm_level,
    t1.app_id AS app_id,
    t1.client_code AS client_code,
    t1.event_type AS event_type,
    t1.event_id AS event_id,
    t1.url_type AS url_type,
    t1.utdid AS utdid --<< Used pdp_uv
,
    t1.row_label AS row_label,
    CASE
        WHEN t1.user_id IS NOT NULL THEN t1.user_id
        ELSE t4.buyer_id
    END AS account_id,
    CASE
        WHEN t3.buyer_id IS NOT NULL THEN t3.buyer_id
        ELSE t4.buyer_id
    END AS buyer_id --<< Used buyer
,
    CASE
        WHEN t3.order_id IS NOT NULL THEN t3.order_id
        ELSE t4.order_id
    END AS order_id,
    t1.product_id AS product_id,
    t1.seller_id AS seller_id,
    t1.shop_id AS shop_id,
    t1.ds AS ds
FROM (
        SELECT ds,
            app_id,
            client_code,
            event_type,
            event_id,
            url_type,
            flow_id --<< 1 step guided
,
            utdid,
            row_label,
            user_id,
            seller_id,
            shop_id,
            product_id
        FROM lazada_analyst_dev.loutruong_log_app_di LATERAL VIEW EXPLODE(SPLIT(url_flow_lvl2_ids, ',')) t_flow AS flow_id
        WHERE 1 = 1 --
            -- AND     ds BETWEEN 20230301 AND ${bizdate} --<< Controller partition - Starting point, special partition
            AND ds >= TO_CHAR(
                DATEADD(TO_DATE($ { bizdate }, 'yyyymmdd'), -2, 'dd'),
                'yyyymmdd'
            ) --<< Controller partition - Daily operation
    ) AS t1
    INNER JOIN (
        SELECT flow_id,
            TOLOWER(flow_name) AS int_channel_lv2,
            parent_flow_id,
            CASE
                WHEN TOLOWER(parent_flow_name) IN ('root') THEN 'others'
                ELSE TOLOWER(parent_flow_name)
            END AS int_channel_lv1,
            flow_level
        FROM lazada_cdm.dim_lzd_log_app_flow_info_flag
        WHERE 1 = 1
            AND ds = MAX_PT('lazada_cdm.dim_lzd_log_app_flow_info_flag')
            AND flow_level IN (1, 2)
        ORDER BY parent_flow_name,
            flow_name
    ) AS t2 ON t1.flow_id = t2.flow_id
    LEFT JOIN (
        -- Mapping with transaction table for any order from the same user/buyer for the same product_id within a day
        SELECT TO_CHAR(order_create_date, 'yyyymmdd') AS ds --<< Use order_create_date >> correct ordering date
,
            buyer_id,
            product_id,
            order_id
        FROM lazada_cdm.dwd_lzd_trd_core_fulfill_di
        WHERE 1 = 1 --
            -- AND     TO_CHAR(order_create_date,'yyyymmdd') BETWEEN 20230301 AND ${bizdate} --<< Controller partition - Starting point, special partition
            AND TO_CHAR(order_create_date, 'yyyymmdd') >= TO_CHAR(
                DATEADD(TO_DATE($ { bizdate }, 'yyyymmdd'), -2, 'dd'),
                'yyyymmdd'
            ) --<< Controller partition - Daily operation
            AND venture = 'VN'
            AND is_revenue = 1
            AND usertrack_id IS NOT NULL --<< Order from app only
    ) AS t3 ON t1.ds = t3.ds
    AND t1.user_id = t3.buyer_id
    AND t1.product_id = t3.product_id
    LEFT JOIN (
        --<< Used for mapping order not using buyer_id data, some users browse but not log-in then log in to purchase later
        SELECT TO_CHAR(order_create_date, 'yyyymmdd') AS ds --<< Use order_create_date >> correct ordering date
,
            usertrack_id AS utdid,
            buyer_id,
            product_id,
            order_id
        FROM lazada_cdm.dwd_lzd_trd_core_fulfill_di
        WHERE 1 = 1 --
            -- AND     TO_CHAR(order_create_date,'yyyymmdd') BETWEEN 20230301 AND ${bizdate} --<< Controller partition - Starting point, special partition
            AND TO_CHAR(order_create_date, 'yyyymmdd') >= TO_CHAR(
                DATEADD(TO_DATE($ { bizdate }, 'yyyymmdd'), -2, 'dd'),
                'yyyymmdd'
            ) --<< Controller partition - Daily operation
            AND venture = 'VN'
            AND is_revenue = 1
            AND usertrack_id IS NOT NULL --<< Order from app only
    ) AS t4 ON t1.ds = t4.ds
    AND t1.utdid = t4.utdid
    AND t1.product_id = t4.product_id
    LEFT JOIN (
        SELECT ds,
            utdid,
            clm_level
        FROM lazada_analyst_dev.loutruong_utdid_clm_di
        WHERE 1 = 1 --
            -- AND     ds BETWEEN 20230301 AND ${bizdate} --<< Controller partition - Starting point, special partition
            AND ds >= TO_CHAR(
                DATEADD(TO_DATE($ { bizdate }, 'yyyymmdd'), -2, 'dd'),
                'yyyymmdd'
            ) --<< Controller partition - Daily operation
        GROUP BY ds,
            utdid,
            clm_level
    ) AS t5 ON t1.ds = t5.ds
    AND t1.utdid = t5.utdid
    LEFT JOIN (
        SELECT ds,
            product_id,
            COALESCE(TOLOWER(industry_name), 'unknown') AS cluster,
            COALESCE(TOLOWER(business_type), 'unknown') AS bu_level1,
            COALESCE(TOLOWER(business_type_level2), 'unknown') AS bu_level2,
            COALESCE(TOLOWER(regional_category1_name), 'unknown') AS cat_level1
        FROM lazada_cdm.dim_lzd_prd_product
        WHERE 1 = 1 --
            AND ds = MAX_PT('lazada_cdm.dim_lzd_prd_product')
            AND venture = 'VN'
    ) AS t6 ON t1.product_id = t6.product_id;
--@@ Input = lazada_analyst_dev.loutruong_log_1s_app_di
--@@ Output = lazada_analyst_dev.loutruong_log_1s_item_clm_app_di
-- DROP TABLE IF EXISTS lazada_analyst_dev.loutruong_log_1s_item_clm_app_di
-- ;
-- CREATE TABLE IF NOT EXISTS lazada_analyst_dev.loutruong_log_1s_item_clm_app_di
-- (
--     camp_type             STRING
--     ,master_campaign_name STRING
--     ,period               STRING
--     ,int_channel_lv1      STRING
--     ,int_channel_lv2      STRING
--     ,clm_level            STRING
--     ,cluster              STRING
--     ,cat_level1           STRING
--     ,bu_level1            STRING
--     ,bu_level2            STRING
--     ,order_app_cnt        DOUBLE
--     ,buyer_app_cnt        DOUBLE
--     ,pdp_pv_app_cnt       DOUBLE
--     ,pdp_uv_app_cnt       DOUBLE
-- )
-- PARTITIONED BY 
-- (
--     ds                    STRING
-- )
-- LIFECYCLE 3600
-- ;
INSERT OVERWRITE TABLE lazada_analyst_dev.loutruong_log_1s_item_clm_app_di PARTITION (ds)
SELECT DISTINCT CASE
        WHEN TOLOWER(t2.camp_type) IS NULL THEN 'bau'
        ELSE TOLOWER(t2.camp_type)
    END AS camp_type,
    CASE
        WHEN TOLOWER(t2.master_campaign_name) IS NULL THEN 'bau'
        ELSE TOLOWER(t2.master_campaign_name)
    END AS master_campaign_name,
    CASE
        WHEN TOLOWER(t2.period) IS NULL THEN 'bau'
        ELSE TOLOWER(t2.period)
    END AS period,
    t1.int_channel_lv1 AS int_channel_lv1,
    t1.int_channel_lv2 AS int_channel_lv2,
    t1.clm_level AS clm_level,
    t1.cluster AS cluster,
    t1.cat_level1 AS cat_level1,
    t1.bu_level1 AS bu_level1,
    t1.bu_level2 AS bu_level2,
    t1.order_app_cnt AS order_app_cnt,
    t1.buyer_app_cnt AS buyer_app_cnt,
    t1.pdp_pv_app_cnt AS pdp_pv_app_cnt,
    t1.pdp_uv_app_cnt AS pdp_uv_app_cnt,
    t1.ds AS ds
FROM (
        SELECT ds,
            COALESCE(TOLOWER(int_channel_lv1), 'all') AS int_channel_lv1,
            COALESCE(TOLOWER(int_channel_lv2), 'all') AS int_channel_lv2,
            COALESCE(TOLOWER(clm_level), 'all') AS clm_level,
            COALESCE(TOLOWER(cluster), 'all') AS cluster,
            COALESCE(TOLOWER(cat_level1), 'all') AS cat_level1,
            COALESCE(TOLOWER(bu_level1), 'all') AS bu_level1,
            COALESCE(TOLOWER(bu_level2), 'all') AS bu_level2,
            COUNT(DISTINCT order_id) AS order_app_cnt,
            COUNT(DISTINCT buyer_id) AS buyer_app_cnt,
            COUNT(DISTINCT row_label) AS pdp_pv_app_cnt,
            COUNT(DISTINCT utdid) AS pdp_uv_app_cnt
        FROM lazada_analyst_dev.loutruong_log_1s_app_di
        WHERE 1 = 1 --
            -- AND     ds BETWEEN 20230301 AND ${bizdate} --<< Controller partition - Starting point, special partition
            AND ds >= TO_CHAR(
                DATEADD(TO_DATE($ { bizdate }, 'yyyymmdd'), -2, 'dd'),
                'yyyymmdd'
            ) --<< Controller partition - Daily operation
            AND TOLOWER(url_type) IN ('ipv')
        GROUP BY GROUPING SETS (
                (
                    ds,
                    int_channel_lv1,
                    int_channel_lv2,
                    clm_level,
                    cluster,
                    cat_level1,
                    bu_level1,
                    bu_level2
                ),
                (
                    ds,
                    int_channel_lv1,
                    int_channel_lv2,
                    clm_level,
                    cluster,
                    cat_level1,
                    bu_level1
                ),
                (
                    ds,
                    int_channel_lv1,
                    int_channel_lv2,
                    clm_level,
                    cluster,
                    cat_level1
                ),
                (
                    ds,
                    int_channel_lv1,
                    int_channel_lv2,
                    clm_level,
                    cluster
                ),
                (
                    ds,
                    int_channel_lv1,
                    int_channel_lv2,
                    clm_level,
                    cat_level1,
                    bu_level1,
                    bu_level2
                ),
                (
                    ds,
                    int_channel_lv1,
                    int_channel_lv2,
                    clm_level,
                    cat_level1,
                    bu_level1
                ),
                (
                    ds,
                    int_channel_lv1,
                    int_channel_lv2,
                    clm_level,
                    cat_level1
                ),
                (ds, int_channel_lv1, int_channel_lv2, clm_level) ---
                ---
,
                (
                    ds,
                    int_channel_lv1,
                    int_channel_lv2,
                    cluster,
                    cat_level1,
                    bu_level1,
                    bu_level2
                ),
                (
                    ds,
                    int_channel_lv1,
                    int_channel_lv2,
                    cluster,
                    cat_level1,
                    bu_level1
                ),
                (
                    ds,
                    int_channel_lv1,
                    int_channel_lv2,
                    cluster,
                    cat_level1
                ),
                (ds, int_channel_lv1, int_channel_lv2, cluster),
                (
                    ds,
                    int_channel_lv1,
                    int_channel_lv2,
                    cat_level1,
                    bu_level1,
                    bu_level2
                ),
                (
                    ds,
                    int_channel_lv1,
                    int_channel_lv2,
                    cat_level1,
                    bu_level1
                ),
                (ds, int_channel_lv1, int_channel_lv2, cat_level1),
                (ds, int_channel_lv1, int_channel_lv2) ---
                ---
,
                (
                    ds,
                    int_channel_lv1,
                    clm_level,
                    cluster,
                    cat_level1,
                    bu_level1,
                    bu_level2
                ),
                (
                    ds,
                    int_channel_lv1,
                    clm_level,
                    cluster,
                    cat_level1,
                    bu_level1
                ),
                (
                    ds,
                    int_channel_lv1,
                    clm_level,
                    cluster,
                    cat_level1
                ),
                (ds, int_channel_lv1, clm_level, cluster),
                (
                    ds,
                    int_channel_lv1,
                    clm_level,
                    cat_level1,
                    bu_level1,
                    bu_level2
                ),
                (
                    ds,
                    int_channel_lv1,
                    clm_level,
                    cat_level1,
                    bu_level1
                ),
                (ds, int_channel_lv1, clm_level, cat_level1),
                (ds, int_channel_lv1, clm_level) ---
                ---
,
                (
                    ds,
                    int_channel_lv1,
                    cluster,
                    cat_level1,
                    bu_level1,
                    bu_level2
                ),
                (
                    ds,
                    int_channel_lv1,
                    cluster,
                    cat_level1,
                    bu_level1
                ),
                (ds, int_channel_lv1, cluster, cat_level1),
                (ds, int_channel_lv1, cluster),
                (
                    ds,
                    int_channel_lv1,
                    cat_level1,
                    bu_level1,
                    bu_level2
                ),
                (ds, int_channel_lv1, cat_level1, bu_level1),
                (ds, int_channel_lv1, cat_level1),
                (ds, int_channel_lv1) ---
                ---
,
                (
                    ds,
                    clm_level,
                    cluster,
                    cat_level1,
                    bu_level1,
                    bu_level2
                ),
                (ds, clm_level, cluster, cat_level1, bu_level1),
                (ds, clm_level, cluster, cat_level1),
                (ds, clm_level, cluster),
                (ds, clm_level, cat_level1, bu_level1, bu_level2),
                (ds, clm_level, cat_level1, bu_level1),
                (ds, clm_level, cat_level1),
                (ds, clm_level) ---
                ---
,
                (ds, cluster, cat_level1, bu_level1, bu_level2),
                (ds, cluster, cat_level1, bu_level1),
                (ds, cluster, cat_level1),
                (ds, cluster),
                (ds, cat_level1, bu_level1, bu_level2),
                (ds, cat_level1, bu_level1),
                (ds, cat_level1),
                (ds)
            )
    ) AS t1
    LEFT JOIN (
        SELECT ds,
            camp_type,
            master_campaign_name,
            period
        FROM lazada_analyst.datx_campaign_list
        WHERE 1 = 1 --
            -- AND     ds BETWEEN 20230701 AND ${bizdate} --<< Controller partition - Starting point, special partition
            AND ds >= TO_CHAR(
                DATEADD(TO_DATE($ { bizdate }, 'yyyymmdd'), -2, 'dd'),
                'yyyymmdd'
            ) --<< Controller partition - Daily operation
        GROUP BY ds,
            camp_type,
            master_campaign_name,
            period
    ) AS t2 ON t1.ds = t2.ds;
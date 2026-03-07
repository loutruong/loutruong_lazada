-- MaxCompute SQL 
-- ********************************************************************--
-- author:Truong, Van Thanh
-- create time:2024-05-10 12:00:43
-- ********************************************************************--
--@@ Input = lazada_cdm.dwd_lzd_trd_core_df
--@@ Input = lazada_cdm.dim_lzd_usr
--@@ Input = lazada_tech.s_membership_visible_id_user_id_record
--@@ Output = lazada_analyst_dev.loutruong_check_out_rank_df
DROP TABLE IF EXISTS lazada_analyst_dev.loutruong_check_out_rank_df;
-- CREATE TABLE IF NOT EXISTS lazada_analyst_dev.loutruong_check_out_rank_df
-- (
--     fulfillment_create_year      STRING COMMENT 'i.e 2023'
--     ,fulfillment_create_month    STRING COMMENT 'i.e 202311'
--     ,fulfillment_create_date     STRING COMMENT 'i.e 20231111'
--     ,fulfillment_create_hh       STRING COMMENT 'Min 00 Max 23'
--     ,client_type                 STRING COMMENT 'desktop / msite / app'
--     ,platform                    STRING COMMENT 'app / web'
--     ,anonymous_id                STRING COMMENT 'web device_id'
--     ,utdid                       STRING COMMENT 'app device_id'
--     ,master_id                   STRING COMMENT 'Mix app / web device_id COALESCE(MAX(usertrack_id),MAX(anonymous_id))'
--     ,buyer_id                    STRING COMMENT 'user_id / buyer_id'
--     ,visible_lazada_id           STRING COMMENT 'ui / ux id'
--     ,created_at                  DATETIME
--     ,updated_at                  DATETIME
--     ,suspended                   BIGINT COMMENT 'whether the user is normal or logged out：0-normal,2-logged out'
--     ,language                    STRING COMMENT 'current language of customer'
--     ,avatar                      STRING
--     ,first_name                  STRING
--     ,middle_name                 STRING
--     ,last_name                   STRING
--     ,nick_name                   STRING
--     ,gender                      STRING
--     ,phone_country_code          STRING
--     ,phone                       STRING COMMENT 'users phone'
--     ,email                       STRING COMMENT 'users email'
--     ,birthday                    STRING
--     ,age                         BIGINT
--     ,member_level                STRING COMMENT 'enum("trusted","VIP","neutral","Flagged")'
--     ,member_status               STRING COMMENT 'enum("normal","delete")'
--     ,memberfrozentag             BIGINT COMMENT 'memberFrozenTag,1 yes,0 no'
--     ,facebook_user_id            STRING COMMENT 'facebook id of customer if any'
--     ,google_user_id              STRING COMMENT 'google id of customer if any'
--     ,is_kol                      BIGINT COMMENT '1=Yes, 0=No'
--     ,is_sms_subscriber           BIGINT COMMENT '1=Yes, 0=No'
--     ,is_email_confirmed          BIGINT COMMENT '1=Yes, 0=No'
--     ,is_newsletter_subscriber    BIGINT COMMENT '1=Yes, 0=No'
--     ,business_application        STRING COMMENT 'LZD = Lazada, ZAL = Minishop'
--     ,enable_ewallet              BIGINT
--     ,default_payment_method      STRING COMMENT 'default payment method selected by customer'
--     ,default_billing_address     STRING COMMENT 'default billing address of the customer'
--     ,default_shipping_address    STRING COMMENT 'default shipping address of the customer'
--     ,region1                     STRING COMMENT 'highest level of region of default shipping address'
--     ,region2                     STRING COMMENT 'second highest level of region of default shipping address'
--     ,region3                     STRING COMMENT 'third highest level of region of default shipping address'
--     ,region4                     STRING COMMENT 'lowest level of region of default shipping address'
--     ,check_out_id                STRING
--     ,sales_order_item_id         STRING COMMENT 'Lowest dimension in this table'
--     ,lifetime_check_out_rank     BIGINT COMMENT 'Duplicate since ranking by check_out_id but sales_order_item_id is the lowest dimension'
--     ,yearly_check_out_rank       BIGINT COMMENT 'Duplicate since ranking by check_out_id but sales_order_item_id is the lowest dimension'
--     ,monthly_check_out_rank      BIGINT COMMENT 'Duplicate since ranking by check_out_id but sales_order_item_id is the lowest dimension'
--     ,daily_check_out_rank        BIGINT COMMENT 'Duplicate since ranking by check_out_id but sales_order_item_id is the lowest dimension'
--     ,app_lifetime_check_out_rank BIGINT COMMENT 'Duplicate since ranking by check_out_id but sales_order_item_id is the lowest dimension'
--     ,app_yearly_check_out_rank   BIGINT COMMENT 'Duplicate since ranking by check_out_id but sales_order_item_id is the lowest dimension'
--     ,app_monthly_check_out_rank  BIGINT COMMENT 'Duplicate since ranking by check_out_id but sales_order_item_id is the lowest dimension'
--     ,app_daily_check_out_rank    BIGINT COMMENT 'Duplicate since ranking by check_out_id but sales_order_item_id is the lowest dimension'
--     ,web_lifetime_check_out_rank BIGINT COMMENT 'Duplicate since ranking by check_out_id but sales_order_item_id is the lowest dimension'
--     ,web_yearly_check_out_rank   BIGINT COMMENT 'Duplicate since ranking by check_out_id but sales_order_item_id is the lowest dimension'
--     ,web_monthly_check_out_rank  BIGINT COMMENT 'Duplicate since ranking by check_out_id but sales_order_item_id is the lowest dimension'
--     ,web_daily_check_out_rank    BIGINT COMMENT 'Duplicate since ranking by check_out_id but sales_order_item_id is the lowest dimension'
-- )
-- COMMENT 'All check out rank of VN from 2012'
-- PARTITIONED BY 
-- (
--     ds                           STRING COMMENT 'data snap shot date'
-- )
-- LIFECYCLE 7
-- ;
INSERT OVERWRITE TABLE lazada_analyst_dev.loutruong_check_out_rank_df PARTITION (ds = '${bizdate}')
SELECT t1.yy AS fulfillment_create_year,
    t1.mm AS fulfillment_create_month,
    t1.ds AS fulfillment_create_date,
    t1.hh AS fulfillment_create_hh,
    t1.client_type AS client_type,
    t1.platform AS platform,
    t1.anonymous_id AS anonymous_id,
    t1.utdid AS utdid,
    t1.master_id AS master_id,
    t1.buyer_id AS buyer_id,
    t4.visible_lazada_id AS visible_lazada_id,
    t3.created_at AS created_at,
    t3.updated_at AS updated_at,
    t3.suspended AS suspended,
    t3.language AS language,
    t3.avatar AS avatar,
    t3.first_name AS first_name,
    t3.middle_name AS middle_name,
    t3.last_name AS last_name,
    t3.nick_name AS nick_name,
    t3.gender AS gender,
    t3.phone_country_code AS phone_country_code,
    t3.phone AS phone,
    t3.email AS email,
    t3.birthday AS birthday,
    t3.age AS age,
    t3.member_level AS member_level,
    t3.member_status AS member_status,
    t3.memberfrozentag AS memberfrozentag,
    t3.facebook_user_id AS facebook_user_id,
    t3.google_user_id AS google_user_id,
    t3.is_kol AS is_kol,
    t3.is_sms_subscriber AS is_sms_subscriber,
    t3.is_email_confirmed AS is_email_confirmed,
    t3.is_newsletter_subscriber AS is_newsletter_subscriber,
    t3.business_application AS business_application,
    t3.enable_ewallet AS enable_ewallet,
    t3.default_payment_method AS default_payment_method,
    t3.default_billing_address AS default_billing_address,
    t3.default_shipping_address AS default_shipping_address,
    t3.region1 AS region1,
    t3.region2 AS region2,
    t3.region3 AS region3,
    t3.region4 AS region4,
    t1.check_out_id AS check_out_id,
    t2.sales_order_item_id AS sales_order_item_id,
    t1.lifetime_check_out_rank AS lifetime_check_out_rank,
    t1.yearly_check_out_rank AS yearly_check_out_rank,
    t1.monthly_check_out_rank AS monthly_check_out_rank,
    t1.daily_check_out_rank AS daily_check_out_rank,
    t1.app_lifetime_check_out_rank AS app_lifetime_check_out_rank,
    t1.app_yearly_check_out_rank AS app_yearly_check_out_rank,
    t1.app_monthly_check_out_rank AS app_monthly_check_out_rank,
    t1.app_daily_check_out_rank AS app_daily_check_out_rank,
    t1.web_lifetime_check_out_rank AS web_lifetime_check_out_rank,
    t1.web_yearly_check_out_rank AS web_yearly_check_out_rank,
    t1.web_monthly_check_out_rank AS web_monthly_check_out_rank,
    t1.web_daily_check_out_rank AS web_daily_check_out_rank
FROM (
        SELECT yy,
            mm,
            ds,
            hh,
            client_type,
            platform,
            anonymous_id,
            utdid,
            master_id,
            buyer_id,
            check_out_id,
            ROW_NUMBER() OVER (
                PARTITION BY buyer_id
                ORDER BY fulfillment_create_date ASC,
                    check_out_id ASC
            ) AS lifetime_check_out_rank,
            ROW_NUMBER() OVER (
                PARTITION BY yy,
                buyer_id
                ORDER BY fulfillment_create_date ASC,
                    check_out_id ASC
            ) AS yearly_check_out_rank,
            ROW_NUMBER() OVER (
                PARTITION BY mm,
                buyer_id
                ORDER BY fulfillment_create_date ASC,
                    check_out_id ASC
            ) AS monthly_check_out_rank,
            ROW_NUMBER() OVER (
                PARTITION BY ds,
                buyer_id
                ORDER BY fulfillment_create_date ASC,
                    check_out_id ASC
            ) AS daily_check_out_rank,
CASE
                WHEN TOLOWER(platform) IN ('app') THEN ROW_NUMBER() OVER (
                    PARTITION BY TOLOWER(platform) IN ('app'),
                    buyer_id
                    ORDER BY fulfillment_create_date ASC,
                        check_out_id ASC
                )
                ELSE NULL
            END AS app_lifetime_check_out_rank,
CASE
                WHEN TOLOWER(platform) IN ('app') THEN ROW_NUMBER() OVER (
                    PARTITION BY TOLOWER(platform) IN ('app'),
                    yy,
                    buyer_id
                    ORDER BY fulfillment_create_date ASC,
                        check_out_id ASC
                )
                ELSE NULL
            END AS app_yearly_check_out_rank,
CASE
                WHEN TOLOWER(platform) IN ('app') THEN ROW_NUMBER() OVER (
                    PARTITION BY TOLOWER(platform) IN ('app'),
                    mm,
                    buyer_id
                    ORDER BY fulfillment_create_date ASC,
                        check_out_id ASC
                )
                ELSE NULL
            END AS app_monthly_check_out_rank,
CASE
                WHEN TOLOWER(platform) IN ('app') THEN ROW_NUMBER() OVER (
                    PARTITION BY TOLOWER(platform) IN ('app'),
                    ds,
                    buyer_id
                    ORDER BY fulfillment_create_date ASC,
                        check_out_id ASC
                )
                ELSE NULL
            END AS app_daily_check_out_rank,
CASE
                WHEN TOLOWER(platform) IN ('web') THEN ROW_NUMBER() OVER (
                    PARTITION BY TOLOWER(platform) IN ('web'),
                    buyer_id
                    ORDER BY fulfillment_create_date ASC,
                        check_out_id ASC
                )
                ELSE NULL
            END AS web_lifetime_check_out_rank,
CASE
                WHEN TOLOWER(platform) IN ('web') THEN ROW_NUMBER() OVER (
                    PARTITION BY TOLOWER(platform) IN ('web'),
                    yy,
                    buyer_id
                    ORDER BY fulfillment_create_date ASC,
                        check_out_id ASC
                )
                ELSE NULL
            END AS web_yearly_check_out_rank,
CASE
                WHEN TOLOWER(platform) IN ('web') THEN ROW_NUMBER() OVER (
                    PARTITION BY TOLOWER(platform) IN ('web'),
                    mm,
                    buyer_id
                    ORDER BY fulfillment_create_date ASC,
                        check_out_id ASC
                )
                ELSE NULL
            END AS web_monthly_check_out_rank,
CASE
                WHEN TOLOWER(platform) IN ('web') THEN ROW_NUMBER() OVER (
                    PARTITION BY TOLOWER(platform) IN ('web'),
                    ds,
                    buyer_id
                    ORDER BY fulfillment_create_date ASC,
                        check_out_id ASC
                )
                ELSE NULL
            END AS web_daily_check_out_rank
        FROM (
                SELECT TO_CHAR(fulfillment_create_date, 'yyyy') AS yy,
                    TO_CHAR(fulfillment_create_date, 'yyyymm') AS mm,
                    TO_CHAR(fulfillment_create_date, 'yyyymmdd') AS ds,
                    TO_CHAR(fulfillment_create_date, 'hh') AS hh,
                    fulfillment_create_date,
CASE
                        WHEN device_type = 'desktop'
                        AND client_type = 'desktop' THEN 'desktop'
                        WHEN device_type = 'mobile'
                        AND client_type = 'mobile' THEN 'msite'
                        ELSE 'app'
                    END AS client_type,
CASE
                        WHEN device_type IN ('desktop', 'mobile')
                        AND client_type IN ('desktop', 'mobile') THEN 'web'
                        ELSE 'app'
                    END AS platform,
                    anonymous_id,
                    usertrack_id AS utdid,
                    COALESCE(MAX(usertrack_id), MAX(anonymous_id)) AS master_id,
                    buyer_id,
                    check_out_id
                FROM lazada_cdm.dwd_lzd_trd_core_df
                WHERE 1 = 1
                    AND ds = MAX_PT('lazada_cdm.dwd_lzd_trd_core_df')
                    AND venture = 'VN'
                    AND is_revenue = 1
                    AND is_fulfilled = 1
                    AND business_application IN ('LZD,ZAL', 'LZD')
                GROUP BY TO_CHAR(fulfillment_create_date, 'yyyy'),
                    TO_CHAR(fulfillment_create_date, 'yyyymm'),
                    TO_CHAR(fulfillment_create_date, 'yyyymmdd'),
                    TO_CHAR(fulfillment_create_date, 'hh'),
                    fulfillment_create_date,
CASE
                        WHEN device_type = 'desktop'
                        AND client_type = 'desktop' THEN 'desktop'
                        WHEN device_type = 'mobile'
                        AND client_type = 'mobile' THEN 'msite'
                        ELSE 'app'
                    END,
CASE
                        WHEN device_type IN ('desktop', 'mobile')
                        AND client_type IN ('desktop', 'mobile') THEN 'web'
                        ELSE 'app'
                    END,
                    anonymous_id,
                    usertrack_id,
                    buyer_id,
                    check_out_id
                ORDER BY TO_CHAR(fulfillment_create_date, 'yyyy') ASC,
                    TO_CHAR(fulfillment_create_date, 'yyyymm') ASC,
                    TO_CHAR(fulfillment_create_date, 'yyyymmdd') ASC,
                    TO_CHAR(fulfillment_create_date, 'hh') ASC
            )
    ) AS t1
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
    ) AS t2 ON t1.check_out_id = t2.check_out_id
    LEFT JOIN (
        SELECT utdid,
            user_id,
            created_at,
            updated_at,
            suspended,
            language,
            avatar,
            first_name,
            middle_name,
            last_name,
            nick_name,
            gender,
            phone_country_code,
            phone,
            email,
            birthday,
            age,
            member_level,
            member_status,
            memberfrozentag,
            facebook_user_id,
            google_user_id,
            is_kol,
            is_sms_subscriber,
            is_email_confirmed,
            is_newsletter_subscriber,
            business_application,
            enable_ewallet,
            default_payment_method,
            default_billing_address,
            default_shipping_address,
            region1,
            region2,
            region3,
            region4
        FROM lazada_cdm.dim_lzd_usr
        WHERE 1 = 1
            AND ds = MAX_PT('lazada_cdm.dim_lzd_usr')
            AND venture = 'VN'
    ) AS t3 ON t1.buyer_id = t3.user_id
    LEFT JOIN (
        SELECT user_id,
            visible_lazada_id
        FROM lazada_tech.s_membership_visible_id_user_id_record
        WHERE 1 = 1
            AND ds = MAX_PT(
                'lazada_tech.s_membership_visible_id_user_id_record'
            )
            AND site_id = 'VN'
    ) AS t4 ON t1.buyer_id = t4.user_id;
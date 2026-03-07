-- MaxCompute SQL 
-- ********************************************************************--
-- author:Truong, Van Thanh
-- create time:2024-04-02 14:49:27
-- ********************************************************************--
--@@ Input = lazada_ads_data.ads_lzd_cps_partner_info_df
--@@ Input = lazada_analyst.loutruong_aff_lead_info
--@@ Input = lazada_cdm.dim_lzd_date
WITH t_timeline AS (
    SELECT iso_date AS ds_1,
        ds_day AS ds_2,
        day_of_week_name AS day_of_week_name_1,
        day_of_week_name_short AS day_of_week_name_2,
        iso_week AS week_1,
        text_week AS week_2,
        text_month AS month_1,
        MAX(ds_day) OVER (PARTITION BY text_week) AS week_end,
        MIN(ds_day) OVER (PARTITION BY text_week) AS week_start
    FROM lazada_cdm.dim_lzd_date
    WHERE 1 = 1
        AND ds_day BETWEEN '20240320' AND TO_CHAR(GETDATE(), 'yyyymmdd')
    GROUP BY iso_date,
        ds_day,
        day_of_week_name,
        day_of_week_name_short,
        iso_week,
        text_week,
        text_month
),
t_prt_info_base AS (
    SELECT phone_number,
        CASE
            WHEN SUBSTR(
                REPLACE(
                    REPLACE(REPLACE(phone_number, ' ', ''), '+840', ''),
                    '+84',
                    ''
                ),
                1,
                1
            ) = 0 THEN SUBSTR(
                REPLACE(
                    REPLACE(REPLACE(phone_number, ' ', ''), '+840', ''),
                    '+84',
                    ''
                ),
                2
            )
            ELSE REPLACE(
                REPLACE(REPLACE(phone_number, ' ', ''), '+840', ''),
                '+84',
                ''
            )
        END AS phone,
        email,
        member_id,
        member_name,
        register_time
    FROM lazada_ads_data.ads_lzd_cps_partner_info_df
    WHERE 1 = 1
        AND ds = MAX_PT('lazada_ads_data.ads_lzd_cps_partner_info_df')
        AND venture = 'VN'
),
t_aff_growth_external_traffic_sources AS (
    SELECT t1.date_create AS lead_date_create,
        TO_CHAR(
            TO_DATE(t1.date_create, 'yyyy-mm-dd hh:mi:ss'),
            'yyyymmdd'
        ) AS lead_ds_create,
        t4.week_2 AS week,
        t4.week_start AS week_start,
        t4.week_end AS week_end,
        CONCAT(
            'Data range:',
            ' ',
            t4.week_start,
            ' ',
            '-->>',
            ' ',
            t4.week_end
        ) AS data_rage,
        t4.day_of_week_name_1 AS day_of_week_name,
        t1.source AS lead_source,
        t1.phone AS lead_phone,
        t1.mail AS lead_mail,
        COALESCE(t2.member_id, t3.member_id) AS member_id,
        COALESCE(t2.member_name, t3.member_name) AS member_name,
        COALESCE(t2.phone, t3.phone) AS member_phone,
        COALESCE(t2.email, t3.email) AS member_email,
        COALESCE(t2.register_time, t3.register_time) AS member_register_time --
,
        CASE
            WHEN COALESCE(t2.register_time, t3.register_time) >= t1.date_create THEN 1
            ELSE 0
        END AS is_after_lead --<< Cross check
,
        DATEDIFF(
            COALESCE(t2.register_time, t3.register_time),
            t1.date_create,
            'dd'
        ) AS lead_time_original --<< Cross check
,
        CASE
            WHEN DATEDIFF(
                COALESCE(t2.register_time, t3.register_time),
                t1.date_create,
                'dd'
            ) IS NULL THEN -1 --<< No reg
            WHEN DATEDIFF(
                COALESCE(t2.register_time, t3.register_time),
                t1.date_create,
                'dd'
            ) < 0 THEN -2 --<< No qualify
            ELSE DATEDIFF(
                COALESCE(t2.register_time, t3.register_time),
                t1.date_create,
                'dd'
            ) --<< qualify
        END AS lead_time --<< Do we need to operation on Thursday or not?
,
        CASE
            WHEN DATEDIFF(
                COALESCE(t2.register_time, t3.register_time),
                t1.date_create,
                'dd'
            ) IS NULL THEN '00_lead_not_member_id'
            WHEN DATEDIFF(
                COALESCE(t2.register_time, t3.register_time),
                t1.date_create,
                'dd'
            ) >= 0
            AND COALESCE(t2.register_time, t3.register_time) >= t1.date_create THEN '01_lead_member_id_qualified'
            ELSE '02_lead_member_id_not_qualified'
        END AS lead_classify --<<
,
        CASE
            WHEN DATEDIFF(
                COALESCE(t2.register_time, t3.register_time),
                t1.date_create,
                'dd'
            ) IS NULL THEN '00_lead_not_member_id'
            WHEN DATEDIFF(
                COALESCE(t2.register_time, t3.register_time),
                t1.date_create,
                'dd'
            ) >= 0
            AND COALESCE(t2.register_time, t3.register_time) >= t1.date_create
            AND TO_CHAR(
                COALESCE(t2.register_time, t3.register_time),
                'yyyymmdd'
            ) >= t4.week_start
            AND TO_CHAR(
                COALESCE(t2.register_time, t3.register_time),
                'yyyymmdd'
            ) <= t4.week_end THEN '01_lead_member_id_qualified_week'
            WHEN DATEDIFF(
                COALESCE(t2.register_time, t3.register_time),
                t1.date_create,
                'dd'
            ) >= 0
            AND COALESCE(t2.register_time, t3.register_time) >= t1.date_create
            AND TO_CHAR(
                COALESCE(t2.register_time, t3.register_time),
                'yyyymmdd'
            ) > t4.week_end THEN '02_lead_member_id_qualified_not_week'
            ELSE '03_lead_member_id_not_qualified'
        END AS lead_classify_type --<<
    FROM (
            SELECT date_input,
                TO_DATE(date_create, 'yyyy-mm-dd hh:mi:ss') AS date_create,
                phone,
                mail,
                source
            FROM lazada_analyst.loutruong_aff_lead_info
            WHERE 1 = 1
                AND date_input IN (
                    SELECT MAX(date_input)
                    FROM lazada_analyst.loutruong_aff_lead_info
                )
        ) AS t1
        LEFT JOIN (
            SELECT phone_number,
                phone,
                email,
                member_id,
                member_name,
                register_time
            FROM t_prt_info_base
        ) AS t2 ON t1.phone = t2.phone
        LEFT JOIN (
            SELECT phone_number,
                phone,
                email,
                member_id,
                member_name,
                register_time
            FROM t_prt_info_base
        ) AS t3 ON t1.mail = t3.email
        INNER JOIN (
            SELECT *
            FROM t_timeline
        ) AS t4 ON TO_CHAR(t1.date_create, 'yyyymmdd') = t4.ds_2
    ORDER BY TO_CHAR(
            TO_DATE(t1.date_create, 'yyyy-mm-dd hh:mi:ss'),
            'yyyymmdd'
        ) DESC
),
t_aff_growth_ads AS (
    SELECT t2.week_2 AS week,
        CONCAT(
            'Data range:',
            ' ',
            t2.week_start,
            ' ',
            '-->>',
            ' ',
            t2.week_end
        ) AS data_rage,
        COUNT(DISTINCT t2.ds_2) AS data_duration,
        COUNT(DISTINCT t1.member_id) AS member_cnt
    FROM (
            SELECT TO_CHAR(register_time, 'yyyymmdd') AS ds,
                member_id
            FROM t_prt_info_base
        ) AS t1
        INNER JOIN (
            SELECT *
            FROM t_timeline
        ) AS t2 ON t1.ds = t2.ds_2
    GROUP BY t2.week_2,
        CONCAT(
            'Data range:',
            ' ',
            t2.week_start,
            ' ',
            '-->>',
            ' ',
            t2.week_end
        )
    ORDER BY t2.week_2 DESC
) --
SELECT lead_date_create,
    lead_ds_create,
    week,
    week_start,
    week_end,
    data_rage,
    lead_source,
    lead_phone,
    lead_mail,
    member_id,
    member_name,
    member_phone,
    member_email,
    member_register_time,
    is_after_lead,
    lead_time_original,
    lead_time,
    lead_classify,
    lead_classify_type
FROM t_aff_growth_external_traffic_sources -- 
    --
    -- SELECT  week
    --         ,data_rage
    --         ,data_duration
    --         ,member_cnt
    -- FROM    t_aff_growth_ads
ORDER BY week DESC;
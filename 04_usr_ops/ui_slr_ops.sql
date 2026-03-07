-- MaxCompute SQL 
-- ********************************************************************--
-- author:Truong, Van Thanh
-- create time:2024-07-08 12:31:44
-- ********************************************************************--
--@@ Input = lazada_cdm.dwd_lzd_pro_fsmax_program_seller_detail_df --<< Useable
--@@ Input = lazada_cdm.dwd_lzd_pro_fsmax_program_seller_di --<< Ref
--@@ Input = lazada_ds.ds_lzd_program_commission_settlement_config
--@@ Input = lazada_ds.ds_lzd_program
--@@ Input = lazada_analyst_dev.vn_map_memid_slrid
--@@ Input = lazada_cdm.dim_lzd_slr_seller_vn
--@@ Input = lazada_cdm.dim_lzd_slr_seller_extra_vn
--@@ Input = lazada_cdm.dwd_lzd_trd_core_fulfill_di
--@@ Input = lazada_cdm.dwd_lzd_pro_promotion_item_di
--@@ Input = lazada_cdm.dim_lzd_pro_collectibles
--@@ Input = lazada_cdm.dwd_lzd_trd_fulfill_spending_di
-- ********************************************************************--
-- Seller performance FSM adopting
-- ********************************************************************--
WITH t_dim_cms AS (
    SELECT t1.ds AS ds,
        t1.program_id AS program_id,
        t1.program_name AS program_name,
        t1.seller_tag AS seller_tag,
        t1.product_tag AS product_tag,
        t1.program_sign_begin_time AS program_sign_begin_time,
        t1.program_sign_end_time AS program_sign_end_time,
        t1.program_begin_time AS program_begin_time,
        t1.program_end_time AS program_end_time,
        ROUND(
            COALESCE(
                GET_JSON_OBJECT(t2.rule, '$.commissionRate'),
                t1.default_rate
            ) / 10000 * 1.1 * 100,
            0
        ) AS program_rate,
        CASE
            WHEN ROUND(
                COALESCE(
                    GET_JSON_OBJECT(t2.rule, '$.commissionCap'),
                    t1.default_cap
                ) / 100 * 1.1,
                -1
            ) >= 100000000 THEN 'No cap'
            ELSE ROUND(
                COALESCE(
                    GET_JSON_OBJECT(t2.rule, '$.commissionCap'),
                    t1.default_cap
                ) / 100 * 1.1,
                -1
            )
        END AS program_cap
    FROM (
            SELECT ds,
                id AS program_id,
                name AS program_name,
                description AS program_description,
                seller_tag,
                product_tag,
                TO_CHAR(
                    sg_udf :epoch_to_timezone(sign_start_time, venture),
                    'yyyymmdd'
                ) AS program_sign_begin_time,
                TO_CHAR(
                    sg_udf :epoch_to_timezone(sign_end_time, venture),
                    'yyyymmdd'
                ) AS program_sign_end_time,
                TO_CHAR(
                    sg_udf :epoch_to_timezone(begin_time, venture),
                    'yyyymmdd'
                ) AS program_begin_time,
                TO_CHAR(
                    sg_udf :epoch_to_timezone(end_time, venture),
                    'yyyymmdd'
                ) AS program_end_time,
                commission_rate AS default_rate,
                commission_cap AS default_cap
            FROM lazada_ds.ds_lzd_program
            WHERE 1 = 1
                AND ds = MAX_PT('lazada_ds.ds_lzd_program') --<<
                AND ds BETWEEN TO_CHAR(
                    sg_udf :epoch_to_timezone(begin_time, venture),
                    'yyyymmdd'
                ) AND TO_CHAR(
                    sg_udf :epoch_to_timezone(end_time, venture),
                    'yyyymmdd'
                )
                AND venture = 'VN'
                AND program_type = 1
                AND status = 1
                AND is_test = 0
                AND REGEXP_INSTR(name, '^.*(TEST|Test|test)+.*') = 0
        ) AS t1
        LEFT JOIN (
            SELECT *
            FROM lazada_ds.ds_lzd_program_commission_settlement_config
            WHERE 1 = 1
                AND ds = MAX_PT(
                    'lazada_ds.ds_lzd_program_commission_settlement_config'
                ) --<< Test
                AND ds BETWEEN TO_CHAR(
                    sg_udf :epoch_to_timezone(effect_start_time, venture),
                    'yyyymmdd'
                ) AND TO_CHAR(
                    sg_udf :epoch_to_timezone(effect_end_time, venture),
                    'yyyymmdd'
                )
                AND venture = 'VN'
                AND status = 1
        ) AS t2 ON t1.ds = t2.ds
        AND t1.program_id = t2.program_id
)
SELECT t1.ds AS ds,
    t1.seller_id AS seller_id,
    t3.seller_short_code AS seller_short_code,
    t3.seller_name AS seller_name,
    COALESCE(t1.program_status, 'Unknown status') AS program_status,
    t3.business_type AS bu_lv1,
    t3.business_type_level2 AS bu_lv2,
    t3.industry_name AS main_industry_name,
    t3.main_category_name AS main_cat_lv1,
    t4.new_seller_segment AS segment,
    t4.pic_lead_name AS pic_lead_name,
    COALESCE(t5.is_mp3_seller, 0) AS is_mp3_slr,
    t2.program_type AS program_type,
    t2.program_id AS program_id,
    t2.program_name AS program_name,
    t2.program_begin_time AS program_begin_time,
    t2.program_end_time AS program_end_time,
    t2.program_rate AS program_rate,
    t2.program_cap AS program_cap
FROM (
        SELECT ds,
            program_id,
            seller_id,
            CASE
                WHEN status = 0 THEN 'Invite'
                WHEN status = 1 THEN 'Join'
                WHEN status = 2 THEN 'Exit'
                WHEN status = 3 THEN 'Replace join to quit'
                WHEN status = 4 THEN 'Replace invite to quit'
                ELSE 'Unknown status'
            END AS program_status,
            sg_udf :epoch_to_timezone(gmt_create, venture) AS invite_time,
            CASE
                WHEN status IN (1) THEN sg_udf :epoch_to_timezone(unfreeze_time, venture)
                ELSE ''
            END AS join_time,
            CASE
                WHEN status IN (2, 3, 4) THEN sg_udf :epoch_to_timezone(unfreeze_time, venture)
                ELSE ''
            END AS quit_time
        FROM lazada_ds.ds_lzd_program_seller
        WHERE 1 = 1
            AND ds = MAX_PT('lazada_ds.ds_lzd_program_seller')
            AND venture = 'VN'
            AND program_id IN (
                SELECT program_id
                FROM t_dim_cms
            )
    ) AS t1
    LEFT JOIN (
        SELECT *,
            CASE
                WHEN program_rate = 8
                AND program_cap = 'No cap' THEN 'Standard'
                ELSE 'Special'
            END AS program_type
        FROM t_dim_cms
    ) AS t2 ON t1.ds = t2.ds
    AND t1.program_id = t2.program_id
    LEFT JOIN (
        SELECT DISTINCT seller_id,
            seller_short_code,
            seller_name,
            business_type,
            business_type_level2,
            industry_name,
            main_category_name
        FROM lazada_cdm.dim_lzd_slr_seller_extra_vn
        WHERE 1 = 1
            AND ds = MAX_PT('lazada_cdm.dim_lzd_slr_seller_extra')
            AND venture = 'VN'
    ) AS t3 ON t1.seller_id = t3.seller_id
    LEFT JOIN (
        SELECT DISTINCT ext_num_id AS seller_id,
            new_seller_segment,
            commercial_team,
            pic_lead_name
        FROM lazada_analyst_dev.vn_map_memid_slrid
        WHERE 1 = 1
            AND date_ = MAX_PT('lazada_analyst_dev.vn_map_memid_slrid')
    ) AS t4 ON t1.seller_id = t4.seller_id
    LEFT JOIN (
        SELECT seller_id,
            is_mp3_seller
        FROM lazada_cdm.dim_lzd_slr_seller_vn
        WHERE 1 = 1
            AND ds = MAX_PT('lazada_cdm.dim_lzd_slr_seller')
            AND venture = 'VN'
    ) AS t5 ON t1.seller_id = t5.seller_id;
-- ********************************************************************--
-- Seller performance FSM
-- ********************************************************************--
SELECT TO_CHAR(TO_DATE(t1.ds, 'yyyymmdd'), 'yyyymm') AS mm,
    t1.ds AS ds,
    t1.seller_id AS seller_id,
    MAX(
        MAX(
            CASE
                WHEN t3.delivery_company IN ('SELLER_OWN_FLEET') THEN 1
                ELSE 0
            END
        )
    ) OVER (
        PARTITION BY TO_CHAR(TO_DATE(t1.ds, 'yyyymmdd'), 'yyyymm'),
        t1.seller_id
    ) AS is_dbs,
    t7.seller_short_code AS seller_short_code,
    t7.seller_name AS seller_name,
    t7.shop_url AS seller_url,
    t8.business_type AS bu_lv1,
    t8.business_type_level2 AS bu_lv2,
    t8.industry_name AS industry_name,
    t8.main_category_name AS main_cat_lv1,
    t9.new_seller_segment AS segment,
    t9.commercial_team AS team_manage,
    t9.pic_lead_name AS pic_lead,
    CASE
        WHEN TOLOWER(t1.program_name) IN ('freeship max 8% no cap') THEN 'Standard'
        ELSE 'Special'
    END AS program_tier,
    t1.program_id AS program_id,
    t1.program_name AS program_name,
    t2.description AS program_description,
    t1.start_time AS program_start_time,
    t1.end_time AS program_end_time,
    t2.sign_start_time AS program_sign_start_time,
    t2.sign_end_time AS program_sign_end_time,
    t1.join_time AS slr_join_time,
    TO_CHAR(
        TO_DATE(t1.join_time, 'yyyy-mm-dd hh:mi:ss'),
        'yyyymmdd'
    ) AS slr_join_date,
    t2.join_url AS program_join_url,
    MAX(COALESCE(t6.fsm_slr_rate, 0)) AS program_rate,
    MAX(COALESCE(t6.fsm_slr_cap, 0)) AS program_cap,
    COUNT(DISTINCT t3.order_id) AS order_id,
    SUM(
        COALESCE(t3.actual_gmv, 0) * COALESCE(t3.exchange_rate, 0)
    ) AS gmv,
    COUNT(
        DISTINCT CASE
            WHEN t5.prom_sub_type IN ('FS Max') THEN t3.order_id
            ELSE NULL
        END
    ) AS order_id_fsm_guided,
    SUM(
        CASE
            WHEN t5.prom_sub_type IN ('FS Max') THEN COALESCE(t3.actual_gmv, 0) * COALESCE(t3.exchange_rate, 0)
            ELSE 0
        END
    ) AS gmv_fsm_guided,
    SUM(
        CASE
            WHEN t5.prom_sub_type IN ('FS Max') THEN COALESCE(t4.fsm_promo_amt, 0) * COALESCE(t3.exchange_rate, 0)
            ELSE 0
        END
    ) AS promo_amt_gross_fulfill,
    SUM(
        COALESCE(t6.fsm_slr_amt, 0) * COALESCE(t3.exchange_rate, 0)
    ) AS slr_amt_fulfill,
    SUM(
        CASE
            WHEN t5.prom_sub_type IN ('FS Max') THEN COALESCE(t4.fsm_promo_amt, 0) * COALESCE(t3.exchange_rate, 0)
            ELSE 0
        END
    ) - SUM(
        COALESCE(t6.fsm_slr_amt, 0) * COALESCE(t3.exchange_rate, 0)
    ) AS promo_amt_net_fulfill,
    SUM(
        CASE
            WHEN t5.prom_sub_type IN ('FS Max')
            AND TOLOWER(t3.item_status_esm) IN ('shipped', 'exportable', 'delivered') THEN COALESCE(t4.fsm_promo_amt, 0) * COALESCE(t3.exchange_rate, 0)
            ELSE 0
        END
    ) AS promo_amt_gross_deliver,
    SUM(
        CASE
            WHEN TOLOWER(t3.item_status_esm) IN ('shipped', 'exportable', 'delivered') THEN COALESCE(t6.fsm_slr_amt, 0) * COALESCE(t3.exchange_rate, 0)
            ELSE 0
        END
    ) AS slr_amt_deliver,
    SUM(
        CASE
            WHEN t5.prom_sub_type IN ('FS Max')
            AND TOLOWER(t3.item_status_esm) IN ('shipped', 'exportable', 'delivered') THEN COALESCE(t4.fsm_promo_amt, 0) * COALESCE(t3.exchange_rate, 0)
            ELSE 0
        END
    ) - SUM(
        CASE
            WHEN TOLOWER(t3.item_status_esm) IN ('shipped', 'exportable', 'delivered') THEN COALESCE(t6.fsm_slr_amt, 0) * COALESCE(t3.exchange_rate, 0)
            ELSE 0
        END
    ) AS promo_amt_net_deliver
FROM (
        SELECT ds,
            seller_id,
            program_id,
            program_name,
            join_time,
            start_time,
            end_time
        FROM lazada_cdm.dwd_lzd_pro_fsmax_program_seller_detail_df
        WHERE 1 = 1 --
            -- AND     ds = MAX_PT("lazada_cdm.dwd_lzd_pro_fsmax_program_seller_detail_df") --<< Get full list within 6 months rolling
            AND ds >= TO_CHAR(
                TO_DATE(join_time, 'yyyy-mm-dd hh:mi:ss'),
                'yyyymmdd'
            ) --<< Joined
            AND ds BETWEEN TO_CHAR(
                TO_DATE(start_time, 'yyyy-mm-dd hh:mi:ss'),
                'yyyymmdd'
            ) AND TO_CHAR(
                TO_DATE(end_time, 'yyyy-mm-dd hh:mi:ss'),
                'yyyymmdd'
            ) --<< Running
            AND suspend_time IS NULL --<< Not shut down
            AND venture = 'VN'
            AND joined = 1
            AND program_type = 1 --<< FSM
            AND seller_id NOT IN (202204217328, 200180502014) --<< Exclude seller account test, seller account bug
    ) AS t1
    LEFT JOIN (
        SELECT id,
            sg_udf :epoch_to_timezone(begin_time, venture) AS begin_time,
            sg_udf :epoch_to_timezone(end_time, venture) AS end_time,
            sg_udf :epoch_to_timezone(sign_start_time, venture) AS sign_start_time,
            sg_udf :epoch_to_timezone(sign_end_time, venture) AS sign_end_time,
            description,
            join_url
        FROM lazada_ds.ds_lzd_program
        WHERE 1 = 1
            AND ds = MAX_PT("lazada_ds.ds_lzd_program")
            AND venture = 'VN' --
            AND program_type IN (1) --<< 1 = Fsm, 3 = Voucher plus, 6 = Edc
            -- AND     program_type IN (1,3,6) --<< 1 = Fsm, 3 = Voucher plus, 6 = Edc
    ) AS t2 ON t1.program_id = t2.id
    LEFT JOIN (
        SELECT *
        FROM lazada_cdm.dwd_lzd_trd_core_fulfill_di
        WHERE 1 = 1 --
            AND ds >= (
                SELECT MIN(ds)
                FROM lazada_cdm.dwd_lzd_pro_fsmax_program_seller_detail_df
                WHERE 1 = 1
                    AND venture = 'VN'
            ) --<< Operation 
            -- AND     ds = '${bizdate}' --<< Test
            AND venture = 'VN'
            AND is_revenue = 1
            AND COALESCE(business_application, 'LZD') IN ('LZD,ZAL', 'LZD')
    ) AS t3 ON t1.ds = t3.ds
    AND t1.seller_id = t3.seller_id
    LEFT JOIN (
        SELECT sales_order_item_id,
            promotion_id,
            promotion_name,
            promotion_type,
            promotion_amount AS fsm_promo_amt
        FROM lazada_cdm.dwd_lzd_pro_promotion_item_di
        WHERE 1 = 1 --
            AND ds >= (
                SELECT TO_CHAR(
                        DATEADD(TO_DATE(MIN(ds), 'yyyymmdd'), -1, 'mm'),
                        'yyyymmdd'
                    )
                FROM lazada_cdm.dwd_lzd_pro_fsmax_program_seller_detail_df
                WHERE 1 = 1
                    AND venture = 'VN'
            ) --<< Operation 
            -- AND     ds = '${bizdate}' --<< Test
            AND venture = 'VN'
            AND is_fulfilled = 1
            AND promotion_type IN ('shippingFeeCoupon')
            AND TOLOWER(promotion_role) IN ('platform')
            AND (
                TOLOWER(retail_sponsor) IN ('platform')
                OR retail_sponsor IS NULL
            )
    ) AS t4 ON t3.sales_order_item_id = t4.sales_order_item_id
    LEFT JOIN (
        SELECT promotion_id,
            prom_sub_type,
            MAX(description) AS description
        FROM lazada_cdm.dim_lzd_pro_collectibles
        WHERE 1 = 1
            AND ds = MAX_PT('lazada_cdm.dim_lzd_pro_collectibles')
            AND venture = 'VN'
        GROUP BY promotion_id,
            prom_sub_type
    ) AS t5 ON t4.promotion_id = t5.promotion_id
    LEFT JOIN (
        SELECT sales_order_item_id,
            COALESCE(KEYVALUE(exp_comm_amt_detail, 'FS_MAX'), 0) AS fsm_slr_amt,
            COALESCE(KEYVALUE(exp_comm_amt_detail, 'FS_MAX_rate'), 0) AS fsm_slr_rate,
            COALESCE(KEYVALUE(exp_comm_amt_detail, 'FS_MAX_cap'), 0) AS fsm_slr_cap
        FROM lazada_cdm.dwd_lzd_trd_fulfill_spending_di
        WHERE 1 = 1 --
            AND ds >= (
                SELECT MIN(ds)
                FROM lazada_cdm.dwd_lzd_pro_fsmax_program_seller_detail_df
                WHERE 1 = 1
                    AND venture = 'VN'
            ) --<< Operation 
            -- AND     ds = '${bizdate}' --<< Test
            AND venture = 'VN'
    ) AS t6 ON t3.sales_order_item_id = t6.sales_order_item_id
    LEFT JOIN (
        SELECT seller_id,
            seller_name,
            short_code AS seller_short_code,
            shop_url
        FROM lazada_cdm.dim_lzd_slr_seller_vn
        WHERE 1 = 1
            AND ds = MAX_PT('lazada_cdm.dim_lzd_slr_seller')
            AND venture = 'VN'
    ) AS t7 ON t1.seller_id = t7.seller_id
    LEFT JOIN (
        SELECT seller_id,
            business_type,
            business_type_level2,
            industry_name,
            main_category_name
        FROM lazada_cdm.dim_lzd_slr_seller_extra_vn
        WHERE 1 = 1
            AND ds = MAX_PT('lazada_cdm.dim_lzd_slr_seller_extra')
            AND venture = 'VN'
    ) AS t8 ON t1.seller_id = t8.seller_id
    LEFT JOIN (
        SELECT ext_num_id AS seller_id,
            new_seller_segment,
            commercial_team,
            pic_lead_name
        FROM lazada_analyst_dev.vn_map_memid_slrid
        WHERE 1 = 1
            AND date_ = MAX_PT('lazada_analyst_dev.vn_map_memid_slrid')
    ) AS t9 ON t1.seller_id = t9.seller_id
GROUP BY TO_CHAR(TO_DATE(t1.ds, 'yyyymmdd'), 'yyyymm'),
    t1.ds,
    t1.seller_id,
    t7.seller_short_code,
    t7.seller_name,
    t7.shop_url,
    t8.business_type,
    t8.business_type_level2,
    t8.industry_name,
    t8.main_category_name,
    t9.new_seller_segment,
    t9.commercial_team,
    t9.pic_lead_name,
    CASE
        WHEN TOLOWER(t1.program_name) IN ('freeship max 8% no cap') THEN 'Standard'
        ELSE 'Special'
    END,
    t1.program_id,
    t1.program_name,
    t2.description,
    t1.start_time,
    t1.end_time,
    t2.sign_start_time,
    t2.sign_end_time,
    t2.join_url,
    t1.join_time;
-- ********************************************************************--
-- Seller information VCM
-- ********************************************************************--
--@@ Input = lazada_cdm.dim_lzd_pro_lpi_sku
--@@ Input = lazada_cdm.dwd_lzd_prd_sku_tags
--@@ Input = lazada_ods.s_bm_campaign_region
--@@ Input = lazada_cdm.dim_lzd_pro_lpi_campaign
--@@ Input = lazada_cdm.dim_aidc_lzd_prom_lpi_rule_inst
--@@ Input = lazada_cdm.dwd_lzd_trd_core_fulfill_di
--@@ Input = lazada_cdm.dwd_lzd_trd_fulfill_spending_di
--@@ Input = lazada_cdm.dwd_lzd_pro_promotion_item_di
--@@ Rule 1) SELLER TAG (Seller ID Tag) + SKU TAG -> 3006/3007
--@@ Rule 2) SELLER BU + CATEGORY + SKU TAG -> 3004/3005
--@@ Rule 3) CATEGORY + SKU TAG -> 3008/3009
--@@ Rule 4) SELLER BU + SKU TAG -> 3002/3003
--@@ Rule 5) SKU TAG Only -> 3000/3001
SELECT t1.program_id AS program_id,
    CASE
        WHEN t1.program_id IN (1534) THEN 'Voucher Max tháng 10/2024'
        WHEN t1.program_id IN (1564) THEN 'Voucher Max tháng 11/2024'
        ELSE t1.program_id
    END AS program_name,
    t1.seller_id AS seller_id,
    t6.seller_short_code AS seller_short_code,
    t6.seller_name AS seller_name,
    t6.business_type AS bu_lv1,
    t6.business_type_level2 AS bu_lv2,
    t6.industry_name AS main_industry,
    t6.main_category_name AS main_cat_lv1,
    t5.new_seller_segment AS segment,
    t5.pic_lead_name AS pic_lead_name,
    t1.vcm_status AS vcm_status,
    t1.invite_time AS invite_time,
    t1.join_time AS join_time,
    t1.quit_time AS quit_time,
    COALESCE(t4.program, t2.program) AS program,
    CASE
        WHEN COALESCE(t4.program, t2.program) IN ('4% cap 50k') THEN '1. Standard offer'
        ELSE '2. Special offer'
    END AS program_type,
    COALESCE(t9.is_mp3_seller, 0) AS is_mp3,
    CASE
        WHEN t1.seller_id IN (
            200160465601,
            1000026556,
            1000468586,
            100146296,
            200174427817,
            200165055144,
            100216770,
            1000131335,
            1000220898,
            1000112031,
            1000420142,
            200159052802,
            200165709979,
            200166981352,
            200165808095,
            100155856,
            200166126753,
            1000334351,
            100186118,
            200169063256,
            200247825532,
            1000318177,
            100130902,
            200400032700,
            200339728113,
            200169147040,
            1000069791,
            200230377149,
            8158,
            200166093050,
            200163921334,
            200166021916,
            200173590196,
            200164341175,
            200164929254,
            200576768626,
            200316752928,
            200213616842,
            100025968,
            1000445810,
            200161896236,
            200233482267,
            200159544035,
            200416176014,
            200171100396,
            200164683028,
            1000086800,
            200343328915,
            1000139353,
            200379840317,
            100175148,
            100017117,
            200246277179,
            200213319665,
            2407,
            200164557340,
            200400320897,
            200166801086,
            200165862403,
            200177358123,
            1000066499,
            1000165359,
            200398288367,
            1000078695,
            200160450909,
            200165859561,
            1000400768,
            200200131911,
            200166762119,
            100014237,
            200168868069,
            1000047719,
            200425440799,
            200159415684,
            100181688,
            200693536124,
            200161263243,
            1000084839,
            200558192453,
            200372400864,
            200180589701,
            200343808323,
            200170146048,
            200173434147,
            200210676167,
            200209938440,
            100248529,
            200161524583,
            200169453768,
            100007994,
            200160768455,
            100125908,
            1000050244,
            200161656949,
            100136732,
            200484784343,
            200615392947,
            200167425041,
            200173263136,
            200707360233,
            200681616021,
            200165310169,
            200168814687,
            1000420135,
            200590576996,
            200165502445,
            1000088844,
            100196091,
            200222094629,
            200251728197,
            200522560065,
            200440016005,
            200324608486,
            200222481085,
            100238630,
            200245074089,
            200648272337,
            200166129457,
            200165172099,
            100235988,
            200434880956,
            1000058151,
            200169525299,
            200453376777,
            200466176058,
            200215131270,
            1000003553,
            200163222040,
            200171211586,
            200169933998,
            200495440553,
            200475696940,
            200158131573,
            200166720045,
            200166615475,
            200168187129,
            200177298252,
            200173947544,
            200158677842,
            200164311447,
            200168844194,
            1000256765,
            200169069732,
            1000105582,
            200163615186,
            100006772,
            200175870423,
            200161290307,
            200163852603,
            200166030814,
            1000174967,
            200173659083,
            200161347018,
            200167809453,
            19824,
            200163822294,
            200668672602,
            200169957063,
            200183202077,
            200192814152,
            200160438457,
            100219144,
            100009775,
            200432816346,
            200619040020,
            200505120298,
            200172792116,
            100242660,
            200168391136,
            200167503290,
            200166372759,
            200185980520,
            200521680553,
            200207847401,
            1000351739,
            200214843459,
            1000102293,
            200462944033,
            100135693,
            200324416475,
            200169021518,
            200224500284,
            200346576437,
            200161035083,
            200250300159,
            200163648227,
            200435440332,
            200613904833,
            200166708079,
            200369024263,
            200701888897,
            200166207733,
            200172054200,
            200169366367,
            200159259346,
            200161404024,
            200311920672,
            200425632994,
            200443808345,
            200158068297,
            200192598595,
            100046909,
            1000378366,
            200188185116,
            200166447726,
            200188200186,
            200482656553,
            200377152050,
            200186946924,
            200168838294,
            200167842186,
            200163843380,
            200167191455,
            200213130114,
            200190084507,
            200453616932,
            200377600196,
            200173782101,
            200164314880,
            200207370025,
            200167827192,
            200173278253,
            200164335698,
            200173755117,
            200174346270,
            200434896713,
            23481,
            100004774,
            200658384041,
            200158284953,
            1000379388,
            200159010838,
            200169459197,
            200519728369,
            200168097848,
            200201034681,
            200288944031,
            200162415012,
            200522096058,
            200172603107,
            200158983626,
            200160774161,
            100179721,
            200174742869,
            200454512725,
            200161347063,
            1000399273,
            200166609594,
            200355520426,
            200169351237,
            1000105945,
            200413280549,
            1000001116,
            200174391205,
            200436704572,
            200459376644,
            200173788085,
            200158503314,
            200159010931,
            200330848186,
            200163864007,
            1000161875,
            200436304702,
            200162184038,
            200397232554,
            200442864176,
            100130533,
            100202421,
            200167950402,
            200199672484,
            200370768536,
            200498672165,
            200589280801,
            200166957557,
            200377632002,
            200167701680,
            200472416449,
            1000442453,
            200167722799,
            200174355249,
            200174904202,
            200167809937,
            200167731769,
            200174907196,
            200174370271,
            200174370279,
            1000058426,
            200165895015,
            200200914880,
            200207562509,
            200164203469,
            200251395599,
            200158275615,
            200158218281,
            100004712,
            1000181489,
            200195409293,
            1000049605,
            200162487625,
            1000026486,
            200167851542,
            200503120214,
            1000333991,
            200167272155,
            200448848277,
            200174211252,
            25445,
            200162304020,
            200161350557,
            1000008792,
            1000273930,
            200162760123,
            1000173457,
            1000280990,
            200226561348,
            200192946361,
            1000125912,
            100235021,
            200639392129,
            200166873237,
            100159546,
            200166555315,
            200489840016,
            200184165593,
            200298600086,
            200164038516,
            22437,
            200199630968,
            1000134783,
            200439664479,
            200366176556,
            200162217068,
            100228003,
            1000035402,
            200162508541,
            100167641,
            100111828,
            200516144164,
            200167935237,
            100136500,
            100219242,
            1000280567,
            1000292477,
            200580192076,
            1000059689,
            200244246725,
            200172591087,
            200192583979,
            1000170822,
            200498048095,
            200160546048,
            200430720848,
            200229018270,
            200298276015,
            200162241006,
            200287044024,
            100011150,
            100136272,
            200158497871,
            508,
            200198715228,
            100185243,
            200164800180,
            200687472963,
            200244435258,
            1000218639,
            200158698263,
            200161305366,
            200312992087,
            200164395048,
            200161362536,
            100144619,
            1000247896,
            200211231188,
            1000219402,
            5784,
            1000013860,
            100017862,
            200176653370,
            200161293124,
            200697104257,
            200160456965,
            100198915,
            200169129250,
            200442496035,
            100215303,
            1000088220,
            200260448615,
            200443920191,
            200170716045,
            200165454202,
            1000109455,
            1000359796,
            200165049757,
            17792,
            200442096610,
            200168214469,
            100132496,
            1000163666,
            200174427073,
            100034668,
            200167575500,
            200169222518,
            200159892875,
            200173251213,
            200162610536,
            1000002367,
            200190726707,
            100188667,
            200532144330,
            200166942087,
            100078616,
            200217756282,
            200163840515,
            200164590475,
            1000036807,
            1000338429,
            1000223582,
            1000095246,
            200165052502,
            200397200629,
            200162997001,
            200263749288,
            200668128040,
            200170053299,
            200173575492,
            200524032429,
            200171169289,
            200188119975,
            200166738130,
            100124778,
            200166624184,
            200485936488,
            200211738515,
            100110175,
            200166042579,
            200163615349,
            200507584162,
            1000022222,
            1000119670,
            200158473026,
            200162742162,
            1000006850,
            200442256840,
            200166798098,
            200158443032,
            200378464258,
            200201985007,
            200175906103,
            1000102337,
            200171811096,
            200696864189,
            100211135,
            200696656311,
            200605760004,
            1000380592,
            200166135701,
            200213505444,
            200167908108,
            100015290,
            100214308,
            200171811112,
            100194141,
            200474176376,
            200162079208,
            200478160367,
            100230246,
            100154069,
            200481632244,
            1000299498,
            200166168704,
            200209395583,
            100165145,
            200166954224,
            1000006784,
            100234885,
            200158173530,
            200168697204,
            200163579005,
            200355440969,
            200166468104,
            25390,
            100137989,
            200166039509,
            200163027123,
            200165664267,
            200158998834,
            100250345,
            200164377034,
            200492784567,
            100106592,
            200215758466,
            200432864047,
            1000016958,
            200171277723,
            25154,
            200163327424,
            200168232448,
            200159406738,
            200163297009,
            200161338077,
            200716112055,
            200560816033,
            28679,
            200169528279,
            200174427023,
            200161551218,
            200386432084,
            100045066,
            200502896371,
            200166948592,
            200161572954,
            200169129156,
            1000049511,
            200400064997,
            200165886106,
            1000069230,
            100125783,
            200161188358,
            200168085228,
            1000020167,
            200342736257,
            200166339820,
            200161161346,
            200330624005,
            200195433972,
            200450480549,
            200160804148,
            1000118573,
            6231,
            200184519343,
            200207823322,
            1000307289,
            100190764,
            200161668520,
            200161854486,
            200457904809,
            200158092545,
            100235704,
            100166299,
            100152751,
            200170773456,
            200339616301,
            100167432,
            200232915812,
            1000058797,
            200488816993,
            200163645203,
            200168058040,
            200499776370,
            200162970414,
            200212821438,
            200328736140,
            100121297,
            200167995225,
            1000343687,
            100082539,
            200170413500,
            100137242,
            200548176361,
            200174196370,
            200178399195,
            1000015471,
            100005670,
            200175459491,
            200165619111,
            200173575354,
            200441440556,
            200186724373,
            200196582072,
            1000136648,
            200377408136,
            200510960980,
            200160735041,
            200160804171,
            200164389170,
            1000097428,
            200233281282,
            200214579322,
            200483376590,
            200195379314,
            200162511069,
            200167116195,
            19226,
            1000141978,
            1000105081,
            200386416942,
            200498368410,
            200162448187,
            100208798,
            200171067021,
            200552032058,
            200453968803,
            100140399,
            100203796,
            200699424393,
            200164026086,
            200502880090,
            200159124240,
            1000206168,
            200651120395,
            100128323,
            200169006801,
            200167170264,
            1000157177,
            200164035526,
            200160708049,
            100161558,
            1000393884,
            100247948,
            200162961142,
            100230188,
            200170005269,
            200195160442,
            1000004392,
            200551584292,
            200160174420,
            200166063535,
            100025193,
            200678160818,
            200516592824,
            26248,
            1000264352,
            200182680203,
            200527840954,
            200512976574,
            200158128839,
            1000516802,
            200168214950,
            200189016428,
            200230443271,
            200165574103,
            225,
            200168727186,
            200167938124,
            200430448659,
            200160963123,
            200428272758,
            200172705100,
            200158980537,
            200167611015,
            200170581012,
            200183589360,
            200162232286,
            100156086,
            200158071879,
            200172339129,
            200162169146,
            200475584731,
            1000477953,
            200411264176,
            100009848,
            1000194191,
            1000108062,
            200673024234,
            1000164212,
            200207055251,
            200167044362,
            200197683568,
            100000105,
            1000027262,
            200184198176,
            200629568049,
            200159250862,
            100182787,
            200418032593,
            1000244414,
            1000088522,
            200167164154,
            100106435,
            200172795175,
            200167617372,
            200175894110,
            200414480324,
            100001361,
            200185758305,
            200656480764,
            200166465066,
            200166957208,
            200161290254,
            200707440526,
            200400976038,
            200176332003,
            200160363279,
            200336432532,
            200618304094,
            200425616299,
            200498624183,
            1000094299,
            200344960261,
            200159262865,
            200178132471,
            200169954493,
            200664144247,
            200165049607,
            200233269579,
            200349840583,
            200166003505,
            200495856052,
            100127280,
            200244456989,
            200161335107,
            1000033621,
            200201907949,
            200188722980,
            1000145165,
            200329616240,
            200164737039,
            200165040320,
            200502560215,
            200158332404,
            200167062230,
            200515056136,
            200161146326,
            1000190412,
            100004246,
            100168974,
            200627840138,
            200458736189,
            200659232834,
            1000275268,
            200635568235,
            1000344928,
            200166612480,
            200331616200,
            200502544051,
            200179290053,
            1000144709,
            200190207612,
            200165571139,
            200442400719,
            200640960733,
            100126344,
            200165847174,
            200197593324,
            100012168,
            200620288019,
            200471360276,
            200165103009,
            200389232495,
            200648784181,
            200161803256,
            200217462857,
            1000104103,
            200233491217,
            200337808077,
            200580752717,
            200171967204,
            200184042805,
            200234019411,
            200167887070,
            200318320202,
            1000371499,
            200481184208,
            200165784166,
            100004336,
            200160060777,
            200168064141,
            200236917910,
            200195319850,
            200166978204,
            200169015376,
            200163738048,
            200169390203,
            27928,
            12223,
            200165382229,
            200686224452,
            200495568824,
            200169252540,
            200402880040,
            200216559939,
            200185947113,
            200169957468,
            200509616548,
            200195280553,
            1000462840,
            200161746391,
            200392288583,
            200614592462,
            1000362684,
            200211936964,
            200177298604,
            200228943412,
            200168970144,
            28631,
            1000251773,
            200160582212,
            1000448700,
            200471184568,
            200161194019,
            200174280099,
            200165142024,
            200246103625,
            1000191947,
            200639456769,
            200495232604,
            200169045620,
            200160816110,
            200166183274,
            200485472427,
            200158233336,
            200386464150,
            200162490134,
            200310208457,
            100116912,
            200166564524,
            100022269,
            200172015226,
            200423824014,
            200270944348,
            200160126014,
            1000258496,
            200173851076,
            200474672025,
            200205861380,
            200509584408,
            200248389023,
            200336656101,
            200168592187,
            200169459262,
            200527696121,
            200173344362,
            200173311152,
            200564320053,
            200199972344,
            200220234723,
            200161071392,
            200158242430,
            29393,
            200158521958,
            1000049759,
            200519472293,
            200169273648,
            200167932567,
            200453040133,
            200588976561,
            200166081253,
            200575344908,
            200169978431,
            1000075867,
            200163219131,
            1000307278,
            200164548523,
            200160849080,
            200427600503,
            200430736831,
            200163879569,
            200350720016,
            200529056021,
            200495968177,
            200317504352,
            1000022223,
            200171877035,
            1000153604,
            200418560269,
            1000249555,
            200526944441,
            200514992623,
            200169267625,
            200485088895,
            17875,
            200170953027,
            200171235433,
            200186982820,
            1000055793,
            200331136215,
            200160060420,
            200221674947,
            200506704220,
            200380352164,
            200194452050,
            200174439819,
            200158083530,
            200456176423,
            200162037101,
            200163060096,
            200169489032,
            200314576223,
            200165862088,
            200165058692,
            200495696283,
            200172291049,
            200167581533,
            200178519743,
            200168292518,
            200166582381,
            200167608279,
            200546640003,
            200174379318,
            200166615025,
            200165889459,
            200510608141,
            200576848235,
            200174217135,
            200166468110,
            200163276190,
            200185836193,
            1000118637,
            200577280632,
            200580352015,
            200227020221,
            200174070230,
            200173581475,
            200506224061,
            200231151287,
            200490080142,
            200494992434,
            200507408466,
            200350928035,
            200490544506,
            200687328845,
            200235336890,
            200229462123,
            200225904917,
            200159127003,
            200162658649,
            200165709351,
            200498592148,
            200166465108,
            200164341662,
            200491040620,
            200479680087,
            200160459585,
            200478064576,
            200164017113,
            200192904654,
            200166864487,
            200171724171,
            200232696415,
            200497968341,
            200595408163,
            200507264926,
            200158170209,
            1000334430,
            1000264357,
            200486208586,
            200519968357,
            200165610395,
            200187087715,
            200516672097,
            200168214157,
            200225580531,
            200167170070,
            200321776755,
            200161590962,
            200168496103,
            200675872783,
            200393152294,
            200164545191,
            200171166267,
            200168229023,
            100010080,
            200567648703,
            200178579057,
            25044,
            200177997108,
            200159019471,
            200166456046,
            200293944313,
            200478192456,
            200597696820,
            200168820473,
            200192295089,
            200201922687,
            200169465205,
            200166348830,
            100003818,
            100057054,
            200158458078,
            200160507577,
            200207310235,
            200244225692,
            200250036573,
            100171601,
            200159244054,
            200161035078,
            200161587407,
            200162994439,
            200164014881,
            200165709073,
            200166690040,
            200171124284,
            200205942817,
            200218941421,
            200314464336,
            200352048888
        ) THEN 1
        ELSE 0
    END AS is_sbp,
    CASE
        WHEN t7.is_delivered_by_seller = 1
        AND t8.seller_id IS NULL THEN 1
        ELSE 0
    END AS is_dbs_trusted,
    CASE
        WHEN t1.seller_id IN (
            200555632012,
            200578448549,
            200534240895,
            200578432181,
            200563456300,
            200578096704,
            200578416091,
            200617840143,
            200551344352,
            200533088623,
            200578480198,
            200531600636,
            200174049228,
            200578416918,
            200578416820,
            200545808615,
            200578448985,
            200534224026,
            200531632631,
            200577248732,
            200578384824,
            200432000149,
            200560864137,
            200594656026,
            200551328737,
            200576848617,
            1000363401,
            200578176481,
            200539152758,
            200594560326,
            200576976249,
            200594384784,
            200603728097,
            200594496167,
            200617824068,
            200587456435,
            200578416439,
            200598736776,
            200604464184,
            200578400373,
            200530336090,
            200642112007,
            200596016280,
            200578224397,
            200558688067,
            200578416325,
            200594560822,
            200616016202,
            200594480962,
            200578464897,
            200594544444,
            200594496707,
            200394464832,
            200578432804,
            200477856940,
            200531312444,
            1000346159,
            200594432541,
            200594480782,
            200578448466,
            200578416777,
            200594496489,
            200594560944,
            200617792123,
            200551200013,
            200575680911,
            200578400841,
            200594640019,
            200620800138,
            200594544203,
            200171616090,
            200533088171
        ) THEN 1
        ELSE 0
    END AS is_biz_risk,
    COALESCE(t10.slr_gmv_l60d) AS slr_gmv_l60d,
    COALESCE(t10.slr_ord_cnt_l60d) AS slr_ord_cnt_l60d,
    COALESCE(t10.slr_gmv_l30d) AS slr_gmv_l30d,
    COALESCE(t10.slr_ord_cnt_l30d) AS slr_ord_cnt_l30d,
    COALESCE(t10.plt_gmv_l60d) AS plt_gmv_l60d,
    COALESCE(t10.plt_ord_cnt_l60d) AS plt_ord_cnt_l60d,
    COALESCE(t10.plt_gmv_l30d) AS plt_gmv_l30d,
    COALESCE(t10.plt_ord_cnt_l30d) AS plt_ord_cnt_l30d
FROM (
        SELECT program_id,
            seller_id,
            MAX(
                CASE
                    WHEN status = 0 THEN '01. Invite'
                    WHEN status = 1 THEN '02. Join'
                    WHEN status = 2 THEN '03. Exit'
                    WHEN status = 3 THEN '04. Replace join to quit'
                    WHEN status = 4 THEN '05. Replace invite to quit'
                    ELSE '07. Unknown status'
                END
            ) AS vcm_status,
            MAX(sg_udf :epoch_to_timezone(gmt_create, venture)) AS invite_time,
            MAX(
                CASE
                    WHEN status IN (1) THEN sg_udf :epoch_to_timezone(operator_time, venture)
                    ELSE ''
                END
            ) AS join_time,
            MAX(
                CASE
                    WHEN status IN (2, 3, 4) THEN sg_udf :epoch_to_timezone(unfreeze_time, venture)
                    ELSE ''
                END
            ) AS quit_time
        FROM lazada_ds.ds_lzd_program_seller
        WHERE 1 = 1
            AND ds >= 20241014
            AND venture = 'VN'
        GROUP BY program_id,
            seller_id
    ) AS t1
    INNER JOIN (
        SELECT id,
            CONCAT(
                REPLACE(ROUND(commission_rate / 100 * 1.1, 0), '.0', ''),
                '%',
                ' ',
                'cap',
                ' ',
                REPLACE(
                    ROUND(commission_cap / 100 * 1.1, -1) / 1000,
                    '.0',
                    ''
                ),
                'k'
            ) AS program
        FROM lazada_ds.ds_lzd_program
        WHERE 1 = 1
            AND ds = MAX_PT('lazada_ds.ds_lzd_program')
            AND venture = 'VN'
            AND program_type = 24
            AND is_test = 0
            AND status = 1
            AND REGEXP_INSTR(name, '^.*(TEST|Test|test)+.*') = 0
    ) AS t2 ON t1.program_id = t2.id
    LEFT JOIN (
        SELECT ds,
            tag_code AS slr_tag_wl,
            seller_id,
            updated_by
        FROM lazada_ods.s_seller_tag_data_vn
        WHERE 1 = 1
            AND ds = MAX_PT('lazada_ods.s_seller_tag_data_vn') --
            -- AND     venture = 'VN'
            AND ds BETWEEN TO_CHAR(
                TO_DATE(
                    sg_udf :epoch_to_timezone(gmt_modified, 'VN'),
                    'yyyy-mm-dd hh:mi:ss'
                ),
                'yyyymmdd'
            ) AND TO_CHAR(GETDATE(), 'yyyymmdd')
            AND tag_type = 'TAG'
            AND tag_code IN (
                295822,
                295823,
                295824,
                295825,
                295826,
                295827,
                295828,
                295829,
                295830,
                295831,
                300896
            )
            AND status = 1
    ) AS t3 ON t1.seller_id = t3.seller_id
    LEFT JOIN (
        SELECT ds,
            program_id,
            id AS settlement_id_set_up,
            status AS settlement_status,
            TO_CHAR(
                sg_udf :epoch_to_timezone(effect_start_time, venture),
                'yyyymmdd'
            ) AS settlement_start,
            TO_CHAR(
                sg_udf :epoch_to_timezone(effect_end_time, venture),
                'yyyymmdd'
            ) AS settlement_end,
            COALESCE(rate_type, 'DEFAULT') AS settlment_type,
            GET_JSON_OBJECT(rule, '$.whitelistTagCode') AS slr_tag_wl,
            CONCAT(
                REPLACE(
                    ROUND(
                        GET_JSON_OBJECT(rule, '$.commissionRate') / 100 * 1.1,
                        0
                    ),
                    '.0',
                    ''
                ),
                '%',
                ' ',
                'cap',
                ' ',
                REPLACE(
                    ROUND(
                        GET_JSON_OBJECT(rule, '$.commissionCap') / 100 * 1.1,
                        -1
                    ) / 1000,
                    '.0',
                    ''
                ),
                'k'
            ) AS program
        FROM lazada_ds.ds_lzd_program_commission_settlement_config
        WHERE 1 = 1
            AND ds = MAX_PT(
                'lazada_ds.ds_lzd_program_commission_settlement_config'
            )
            AND venture = 'VN'
            AND COALESCE(rate_type, 'DEFAULT') NOT IN ('CATEGORY', 'DEFAULT') --
            -- AND     ds BETWEEN TO_CHAR(sg_udf:epoch_to_timezone(effect_start_time,venture),'yyyymmdd') AND TO_CHAR(sg_udf:epoch_to_timezone(effect_end_time,venture),'yyyymmdd')
            -- AND     program_id IN (1534,1564)
    ) AS t4 ON t2.id = t4.program_id
    AND t3.slr_tag_wl = t4.slr_tag_wl
    LEFT JOIN (
        SELECT ext_num_id AS seller_id,
            MAX(new_seller_segment) AS new_seller_segment,
            MAX(pic_lead_name) AS pic_lead_name
        FROM lazada_analyst_dev.vn_map_memid_slrid
        WHERE 1 = 1
            AND date_ = MAX_PT('lazada_analyst_dev.vn_map_memid_slrid')
        GROUP BY ext_num_id
    ) AS t5 ON t1.seller_id = t5.seller_id
    LEFT JOIN (
        SELECT seller_id,
            seller_short_code,
            seller_name,
            business_type,
            business_type_level2,
            industry_name,
            main_category_name
        FROM lazada_cdm.dim_lzd_slr_seller_extra_vn
        WHERE 1 = 1
            AND ds = MAX_PT('lazada_cdm.dim_lzd_slr_seller_extra')
            AND venture = 'VN'
    ) AS t6 ON t1.seller_id = t6.seller_id
    LEFT JOIN (
        SELECT seller_id,
            MAX(is_delivered_by_seller) AS is_delivered_by_seller
        FROM lazada_cdm.dim_lzd_prd_product
        WHERE 1 = 1
            AND ds = MAX_PT('lazada_cdm.dim_lzd_prd_product') --<< Take the lastest snapshot
            AND venture = 'VN'
        GROUP BY seller_id
    ) AS t7 ON t1.seller_id = t7.seller_id
    LEFT JOIN (
        SELECT *
        FROM lazada_analyst_dev.loutruong_slr_non_trusted_dbs
    ) AS t8 ON t1.seller_id = t8.seller_id
    LEFT JOIN (
        SELECT seller_id,
            is_mp3_seller
        FROM lazada_cdm.dim_lzd_slr_seller_vn
        WHERE 1 = 1
            AND ds = MAX_PT('lazada_cdm.dim_lzd_slr_seller')
            AND venture = 'VN'
    ) AS t9 ON t1.seller_id = t9.seller_id
    LEFT JOIN (
        SELECT *
        FROM lazada_analyst_dev.loutruong_vcm_slr_list
        WHERE 1 = 1
            AND ds = MAX_PT('lazada_analyst_dev.loutruong_vcm_slr_list')
    ) AS t10 ON t1.seller_id = t10.seller_id
WHERE 1 = 1
    AND t1.seller_id NOT IN (300425408076);
-- ********************************************************************--
-- Seller Performance VCM
-- ********************************************************************--
WITH t_lpi_eligible AS (
    SELECT t1.sales_order_item_id AS sales_order_item_id,
        COUNT(DISTINCT t3.campaign_id) AS campaign_cnt,
        COALESCE(WM_CONCAT(DISTINCT ' ||;|| ', t3.campaign_id), 0) AS campaign_id,
        COALESCE(
            WM_CONCAT(DISTINCT ' ||;|| ', t4.master_campaign_name),
            0
        ) AS campaign_name,
        COALESCE(WM_CONCAT(DISTINCT ' ||;|| ', t4.level_name), 0) AS campaign_level,
        COALESCE(
            WM_CONCAT(
                DISTINCT ' ||;|| ',
                t4.master_campaign_live_start_time
            ),
            0
        ) AS master_campaign_live_start_time,
        COALESCE(
            WM_CONCAT(
                DISTINCT ' ||;|| ',
                t4.master_campaign_live_end_time
            ),
            0
        ) AS master_campaign_live_end_time,
        MAX(
            CASE
                WHEN t2.sku_id IS NOT NULL THEN 1
                ELSE 0
            END
        ) AS is_lpi_eligble
    FROM (
            SELECT *
            FROM lazada_cdm.dwd_lzd_trd_core_fulfill_di
            WHERE 1 = 1 --
                AND ds >= 20240401 --<< LPI 3.0 start to apply
                -- AND     ds >= 20240701 --<< Test
                -- AND     ds = 20240707 --<< Test
                AND venture = 'VN'
        ) AS t1
        LEFT JOIN (
            SELECT sku_id,
                tag_code,
                master_campaign_live_start_time,
                master_campaign_live_end_time
            FROM lazada_cdm.dim_lzd_pro_lpi_sku
            WHERE 1 = 1
                AND ds = MAX_PT("lazada_cdm.dim_lzd_pro_lpi_sku")
                AND venture = 'VN'
        ) AS t2 ON t1.sku_id = t2.sku_id
        AND t1.fulfillment_create_date BETWEEN t2.master_campaign_live_start_time AND t2.master_campaign_live_end_time
        LEFT JOIN (
            SELECT campaign_id,
                GET_JSON_OBJECT(lpi_rule, '$.ruleList[0].tagCode') AS tag_code
            FROM lazada_ods.s_bm_campaign_region
            WHERE 1 = 1
                AND ds = MAX_PT("lazada_ods.s_bm_campaign_region")
                AND DECODE(
                    region_id,
                    1,
                    "SG",
                    2,
                    "MY",
                    3,
                    "TH",
                    4,
                    "PH",
                    5,
                    "VN",
                    6,
                    "ID"
                ) = 'VN'
        ) AS t3 ON t2.tag_code = t3.tag_code
        LEFT JOIN (
            SELECT *
            FROM lazada_cdm.dim_lzd_pro_lpi_campaign
            WHERE 1 = 1
                AND ds = MAX_PT("lazada_cdm.dim_lzd_pro_lpi_campaign")
                AND venture = 'VN'
        ) AS t4 ON t3.campaign_id = t4.master_campaign_id
    GROUP BY t1.sales_order_item_id
),
t_lpi_info AS (
    SELECT sales_order_item_id,
        MAX(
            CASE
                WHEN ABS(
                    COALESCE(KEYVALUE(exp_comm_amt_detail, 'FS_MAX'), 0)
                ) > 0 THEN 1
                ELSE 0
            END
        ) AS is_fsm_slr,
        MAX(
            CASE
                WHEN ds BETWEEN 20240401 AND 20240930
                AND ABS(
                    COALESCE(KEYVALUE(exp_comm_amt_detail, 'LPI'), 0)
                ) > 0 THEN 1
                ELSE 0
            END
        ) AS is_lpi_slr --<< LPI 3.0 live duration
,
        MAX(
            CASE
                WHEN ds >= 20241001
                AND ABS(
                    COALESCE(KEYVALUE(exp_comm_amt_detail, 'LPI'), 0)
                ) > 0 THEN 1
                ELSE 0
            END
        ) AS is_vcm_slr --<< VCM 2.0 live duration
,
        SUM(
            ABS(
                COALESCE(KEYVALUE(exp_comm_amt_detail, 'FS_MAX'), 0)
            )
        ) AS fsm_slr_amt,
        SUM(
            CASE
                WHEN ds BETWEEN 20240401 AND 20240930 THEN ABS(
                    COALESCE(KEYVALUE(exp_comm_amt_detail, 'LPI'), 0)
                )
                ELSE 0
            END
        ) AS lpi_slr_amt --<< LPI 3.0 live duration
,
        SUM(
            CASE
                WHEN ds >= 20241001 THEN ABS(
                    COALESCE(KEYVALUE(exp_comm_amt_detail, 'LPI'), 0)
                )
                ELSE 0
            END
        ) AS vcm_slr_amt --<< VCM 2.0 live duration
    FROM lazada_ent_cdm.dwd_lzd_fin_trd_commission_di
    WHERE 1 = 1 --
        -- AND     ds = '${bizdate}' --<< Test
        AND ds >= 20240401 --<< Production
        -- AND     ds >= 20241001 --<< Adhoc
        AND venture = 'VN'
    GROUP BY sales_order_item_id
)
SELECT TO_CHAR(TO_DATE(t1.ds, 'yyyymmdd'), 'yyyymm') AS mm,
    t1.ds AS ds,
    t1.seller_id AS seller_id,
    t5.seller_name AS seller_name,
    t5.seller_short_code AS seller_short_code,
    t5.shop_url AS seller_url,
    t6.business_type AS bu_lv1,
    t6.business_type_level2 AS bu_lv2,
    t6.industry_name AS industry_name,
    t6.main_category_name AS main_cat_lv1,
    t7.new_seller_segment AS segment,
    t7.commercial_team AS team_manage,
    t7.pic_lead_name AS pic_lead,
    MAX(t2.is_lpi_eligble) AS is_lpi_eligible --<< Filter tag
,
    MAX(t2.campaign_cnt) AS campaign_cnt,
    MAX(t2.campaign_id) AS campaign_id,
    MAX(t2.campaign_name) AS campaign_name,
    MAX(t2.campaign_level) AS campaign_level,
    MAX(t2.master_campaign_live_start_time) AS campaign_live_start,
    MAX(t2.master_campaign_live_end_time) AS campaign_live_end,
    WM_CONCAT(DISTINCT ' ||;|| ', t3.lpi_slr_rate) AS campaign_rate,
    WM_CONCAT(DISTINCT ' ||;|| ', t3.lpi_slr_cap) AS campaign_cap,
    IF(
        SUM(
            COALESCE(t4.promotion_amount, 0) * t1.exchange_rate
        ) = 0,
        0,
        (
            SUM(
                COALESCE(t4.promotion_amount, 0) * t1.exchange_rate
            ) - SUM(COALESCE(t3.lpi_slr_amt, 0) * t1.exchange_rate)
        ) / SUM(
            COALESCE(t4.promotion_amount, 0) * t1.exchange_rate
        )
    ) AS campaign_ratio_cost_gross,
    IF(
        SUM(
            CASE
                WHEN TOLOWER(t1.item_status_esm) IN ('shipped', 'exportable', 'delivered') THEN COALESCE(t3.lpi_slr_amt, 0) * t1.exchange_rate
                ELSE 0
            END
        ) = 0,
        0,
        SUM(
            CASE
                WHEN TOLOWER(t1.item_status_esm) IN ('shipped', 'exportable', 'delivered') THEN COALESCE(t4.promotion_amount, 0) * t1.exchange_rate
                ELSE 0
            END
        ) / SUM(
            CASE
                WHEN TOLOWER(t1.item_status_esm) IN ('shipped', 'exportable', 'delivered') THEN COALESCE(t3.lpi_slr_amt, 0) * t1.exchange_rate
                ELSE 0
            END
        )
    ) AS campaign_ratio_cost_net,
    SUM(
        COALESCE(t4.promotion_amount, 0) * t1.exchange_rate
    ) AS lpi_promo_amt_gross,
    SUM(COALESCE(t3.lpi_slr_amt, 0) * t1.exchange_rate) AS lpi_slr_amt_gross,
    SUM(
        CASE
            WHEN TOLOWER(t1.item_status_esm) IN ('shipped', 'exportable', 'delivered') THEN COALESCE(t4.promotion_amount, 0) * t1.exchange_rate
            ELSE 0
        END
    ) AS lpi_promo_amt_net,
    SUM(
        CASE
            WHEN TOLOWER(t1.item_status_esm) IN ('shipped', 'exportable', 'delivered') THEN COALESCE(t3.lpi_slr_amt, 0) * t1.exchange_rate
            ELSE 0
        END
    ) AS lpi_slr_amt_net,
    COUNT(DISTINCT t1.order_id) AS order_id,
    SUM(
        COALESCE(t1.actual_gmv, 0) * COALESCE(t1.exchange_rate, 0)
    ) AS gmv,
    COUNT(
        DISTINCT CASE
            WHEN t4.sales_order_item_id IS NOT NULL THEN t1.order_id
            ELSE NULL
        END
    ) AS order_id_lpi_guided_ops,
    SUM(
        CASE
            WHEN t4.sales_order_item_id IS NOT NULL THEN COALESCE(t1.actual_gmv, 0) * COALESCE(t1.exchange_rate, 0)
            ELSE 0
        END
    ) AS gmv_lpi_guided_ops
FROM (
        SELECT *
        FROM lazada_cdm.dwd_lzd_trd_core_fulfill_di
        WHERE 1 = 1 --
            AND ds >= 20240401 --<< LPI 3.0 start to apply
            -- AND     ds >= 20240701 --<< Test
            -- AND     ds = 20240707 --<< Test
            AND venture = 'VN'
            AND is_revenue = 1
            AND COALESCE(business_application, 'LZD') IN ('LZD,ZAL', 'LZD')
    ) AS t1
    LEFT JOIN (
        SELECT *
        FROM t_lpi_eligible
    ) AS t2 ON t1.sales_order_item_id = t2.sales_order_item_id
    LEFT JOIN (
        SELECT *
        FROM t_lpi_info
    ) AS t3 ON t1.sales_order_item_id = t3.sales_order_item_id
    LEFT JOIN (
        SELECT sales_order_item_id,
            promotion_amount
        FROM lazada_cdm.dwd_lzd_pro_promotion_item_di
        WHERE 1 = 1 --
            AND ds >= 20240101 --<< Order created date
            -- AND     ds >= 20240701 --<< Test
            -- AND     ds = 20240707 --<< Test
            AND venture = 'VN'
            AND is_fulfilled = 1
            AND promotion_id IN (
                SELECT promotion_id
                FROM lazada_cdm.dim_lzd_pro_collectibles
                WHERE 1 = 1
                    AND ds = MAX_PT('lazada_cdm.dim_lzd_pro_collectibles')
                    AND venture = 'VN'
                    AND REGEXP_INSTR(voucher_name, '^.*(TEST|Test|test)+.*') = 0 --<< Clear data
                    AND sponsor = 'platform'
                    AND (
                        TOLOWER(retail_sponsor) IN ('platform')
                        OR retail_sponsor IS NULL
                    )
                    AND (
                        (
                            product_code IN ('categoryCoupon', 'collectibleCoupon')
                            AND (
                                TOLOWER(voucher_name) LIKE '%voucher%bonus%'
                                OR TOLOWER(description) LIKE '%voucher%bonus%'
                            )
                        )
                        OR (
                            (
                                CAST(
                                    REGEXP_EXTRACT(
                                        KEYVALUE(features, ',', ':', '"voucherSubType"'),
                                        '\"([0-9]*)\"'
                                    ) AS BIGINT
                                ) IN (7) --<< Effected after 20241018 00:00:00
                                OR (
                                    product_code IN ('categoryCoupon') --<< ,'collectibleCoupon' to get vcm 1.0
                                    AND (
                                        TOLOWER(voucher_name) LIKE '%platform_voucher max%'
                                        OR TOLOWER(voucher_name) LIKE '%voucher%max%'
                                        OR TOLOWER(description) LIKE '%platform_voucher max%'
                                        OR TOLOWER(description) LIKE '%voucher%max%'
                                    )
                                ) --<< Effected 20241001 00:00:00 ~~ 20241017 23:59:59
                            )
                            AND TOLOWER(voucher_name) NOT LIKE '%day%' --<< Only open to get all vcm all ops since campaign voucher use the same setting template
                        )
                    )
            )
    ) AS t4 ON t1.sales_order_item_id = t4.sales_order_item_id
    LEFT JOIN (
        SELECT seller_id,
            seller_name,
            short_code AS seller_short_code,
            shop_url
        FROM lazada_cdm.dim_lzd_slr_seller_vn
        WHERE 1 = 1
            AND ds = MAX_PT('lazada_cdm.dim_lzd_slr_seller')
            AND venture = 'VN'
    ) AS t5 ON t1.seller_id = t5.seller_id
    LEFT JOIN (
        SELECT seller_id,
            business_type,
            business_type_level2,
            industry_name,
            main_category_name
        FROM lazada_cdm.dim_lzd_slr_seller_extra_vn
        WHERE 1 = 1
            AND ds = MAX_PT('lazada_cdm.dim_lzd_slr_seller_extra')
            AND venture = 'VN'
    ) AS t6 ON t1.seller_id = t6.seller_id
    LEFT JOIN (
        SELECT ext_num_id AS seller_id,
            new_seller_segment,
            commercial_team,
            pic_lead_name
        FROM lazada_analyst_dev.vn_map_memid_slrid
        WHERE 1 = 1
            AND date_ = MAX_PT('lazada_analyst_dev.vn_map_memid_slrid')
    ) AS t7 ON t1.seller_id = t7.seller_id
GROUP BY TO_CHAR(TO_DATE(t1.ds, 'yyyymmdd'), 'yyyymm'),
    t1.ds,
    t1.seller_id,
    t5.seller_name,
    t5.seller_short_code,
    t5.shop_url,
    t6.business_type,
    t6.business_type_level2,
    t6.industry_name,
    t6.main_category_name,
    t7.new_seller_segment,
    t7.commercial_team,
    t7.pic_lead_name;
-----------------------------------------------------------------------------------------------
SELECT t1.ds AS ds,
    COALESCE(
        COALESCE(t1.vcm_status, 'Unknown'),
        '00. Eligible'
    ) AS vcm_status,
    COALESCE(COALESCE(t6.business_type, 'Unknown'), '00. All') AS bu_lv1,
    COALESCE(
        COALESCE(t6.business_type_level2, 'Unknown'),
        '00. All'
    ) AS bu_lv2,
    COALESCE(COALESCE(t6.industry_name, 'Unknown'), '00. All') AS industry,
    COALESCE(
        COALESCE(t6.main_category_name, 'Unknown'),
        '00. All'
    ) AS main_cate_lv1,
    COALESCE(
        COALESCE(t5.new_seller_segment, 'Unknown'),
        '00. All'
    ) AS segment,
    COALESCE(COALESCE(t5.pic_lead_name, 'Unknown'), '00. All') AS pic_lead,
    COALESCE(COALESCE(t5.pic_status, 'Unknown'), '00. All') AS pic_status,
    COUNT(DISTINCT t1.seller_id) AS slr_cnt,
    SUM(COALESCE(t10.slr_gmv_1d, 0)) AS slr_gmv_1d,
    SUM(COALESCE(t10.slr_gmv_30d, 0)) AS slr_gmv_30d,
    SUM(COALESCE(t10.slr_gmv_60d, 0)) AS slr_gmv_60d,
    MAX(COALESCE(t10.plt_gmv_1d, 0)) AS plt_gmv_1d,
    MAX(COALESCE(t10.plt_gmv_30d, 0)) AS plt_gmv_30d,
    MAX(COALESCE(t10.plt_gmv_60d, 0)) AS plt_gmv_60d,
    SUM(COALESCE(t10.slr_ord_1d, 0)) AS slr_ord_1d,
    SUM(COALESCE(t10.slr_ord_30d, 0)) AS slr_ord_30d,
    SUM(COALESCE(t10.slr_ord_60d, 0)) AS slr_ord_60d,
    MAX(COALESCE(t10.plt_ord_1d, 0)) AS plt_ord_1d,
    MAX(COALESCE(t10.plt_ord_30d, 0)) AS plt_ord_30d,
    MAX(COALESCE(t10.plt_ord_60d, 0)) AS plt_ord_60d
FROM (
        SELECT ds,
            program_id,
            seller_id,
            CASE
                WHEN status = 0 THEN '01. Invite'
                WHEN status = 1 THEN '02. Join'
                WHEN status = 2 THEN '03. Exit'
                WHEN status = 3 THEN '04. Replace join to quit'
                WHEN status = 4 THEN '05. Replace invite to quit'
                ELSE '07. Unknown status'
            END AS vcm_status,
            sg_udf :epoch_to_timezone(gmt_create, venture) AS invite_time,
            CASE
                WHEN status IN (1) THEN sg_udf :epoch_to_timezone(operator_time, venture)
                ELSE ''
            END AS join_time,
            CASE
                WHEN status IN (2, 3, 4) THEN sg_udf :epoch_to_timezone(unfreeze_time, venture)
                ELSE ''
            END AS quit_time
        FROM lazada_ds.ds_lzd_program_seller
        WHERE 1 = 1
            AND (
                (
                    ds BETWEEN 20240925 AND 20241030
                    AND program_id IN (1534)
                )
                OR (
                    ds BETWEEN 20241031 AND TO_CHAR(DATEADD(GETDATE(), -1, 'dd'), 'yyyymmdd')
                    AND program_id IN (1564)
                )
            )
            AND venture = 'VN'
    ) AS t1
    INNER JOIN (
        SELECT id,
            CONCAT(
                REPLACE(ROUND(commission_rate / 100 * 1.1, 0), '.0', ''),
                '%',
                ' ',
                'cap',
                ' ',
                REPLACE(
                    ROUND(commission_cap / 100 * 1.1, -1) / 1000,
                    '.0',
                    ''
                ),
                'k'
            ) AS program
        FROM lazada_ds.ds_lzd_program
        WHERE 1 = 1
            AND ds = MAX_PT('lazada_ds.ds_lzd_program')
            AND venture = 'VN'
            AND program_type = 24
            AND is_test = 0
            AND status = 1
            AND REGEXP_INSTR(name, '^.*(TEST|Test|test)+.*') = 0
    ) AS t2 ON t1.program_id = t2.id
    LEFT JOIN (
        SELECT ds,
            tag_code AS slr_tag_wl,
            seller_id,
            updated_by
        FROM lazada_ods.s_seller_tag_data_vn
        WHERE 1 = 1
            AND ds = MAX_PT('lazada_ods.s_seller_tag_data_vn') --
            -- AND     venture = 'VN'
            AND ds BETWEEN TO_CHAR(
                TO_DATE(
                    sg_udf :epoch_to_timezone(gmt_modified, 'VN'),
                    'yyyy-mm-dd hh:mi:ss'
                ),
                'yyyymmdd'
            ) AND TO_CHAR(GETDATE(), 'yyyymmdd')
            AND tag_type = 'TAG'
            AND tag_code IN (
                295822,
                295823,
                295824,
                295825,
                295826,
                295827,
                295828,
                295829,
                295830,
                295831,
                300896
            )
            AND status = 1
    ) AS t3 ON t1.seller_id = t3.seller_id
    LEFT JOIN (
        SELECT ds,
            program_id,
            id AS settlement_id_set_up,
            status AS settlement_status,
            TO_CHAR(
                sg_udf :epoch_to_timezone(effect_start_time, venture),
                'yyyymmdd'
            ) AS settlement_start,
            TO_CHAR(
                sg_udf :epoch_to_timezone(effect_end_time, venture),
                'yyyymmdd'
            ) AS settlement_end,
            COALESCE(rate_type, 'DEFAULT') AS settlment_type,
            GET_JSON_OBJECT(rule, '$.whitelistTagCode') AS slr_tag_wl,
            CONCAT(
                REPLACE(
                    ROUND(
                        GET_JSON_OBJECT(rule, '$.commissionRate') / 100 * 1.1,
                        0
                    ),
                    '.0',
                    ''
                ),
                '%',
                ' ',
                'cap',
                ' ',
                REPLACE(
                    ROUND(
                        GET_JSON_OBJECT(rule, '$.commissionCap') / 100 * 1.1,
                        -1
                    ) / 1000,
                    '.0',
                    ''
                ),
                'k'
            ) AS program
        FROM lazada_ds.ds_lzd_program_commission_settlement_config
        WHERE 1 = 1
            AND ds = MAX_PT(
                'lazada_ds.ds_lzd_program_commission_settlement_config'
            )
            AND venture = 'VN'
            AND COALESCE(rate_type, 'DEFAULT') NOT IN ('CATEGORY', 'DEFAULT') --
            -- AND     ds BETWEEN TO_CHAR(sg_udf:epoch_to_timezone(effect_start_time,venture),'yyyymmdd') AND TO_CHAR(sg_udf:epoch_to_timezone(effect_end_time,venture),'yyyymmdd')
            -- AND     program_id IN (1534,1564)
    ) AS t4 ON t2.id = t4.program_id
    AND t3.slr_tag_wl = t4.slr_tag_wl
    LEFT JOIN (
        SELECT seller_id,
            new_seller_segment,
            pic_lead_name,
            CASE
                WHEN pic_lead_name IN ('no_pic') THEN 'No PIC'
                ELSE 'Has PIC'
            END AS pic_status
        FROM (
                SELECT ext_num_id AS seller_id,
                    MAX(new_seller_segment) AS new_seller_segment,
                    MAX(pic_lead_name) AS pic_lead_name
                FROM lazada_analyst_dev.vn_map_memid_slrid
                WHERE 1 = 1
                    AND date_ = MAX_PT('lazada_analyst_dev.vn_map_memid_slrid')
                GROUP BY ext_num_id
            )
    ) AS t5 ON t1.seller_id = t5.seller_id
    LEFT JOIN (
        SELECT seller_id,
            seller_short_code,
            seller_name,
            business_type,
            business_type_level2,
            industry_name,
            main_category_name
        FROM lazada_cdm.dim_lzd_slr_seller_extra_vn
        WHERE 1 = 1
            AND ds = MAX_PT('lazada_cdm.dim_lzd_slr_seller_extra')
            AND venture = 'VN'
    ) AS t6 ON t1.seller_id = t6.seller_id --
    -- LEFT JOIN   (
    --                 SELECT  seller_id
    --                         ,MAX(is_delivered_by_seller) AS is_delivered_by_seller
    --                 FROM    lazada_cdm.dim_lzd_prd_product
    --                 WHERE   1 = 1
    --                 AND     ds = MAX_PT('lazada_cdm.dim_lzd_prd_product') --<< Take the lastest snapshot
    --                 AND     venture = 'VN'
    --                 GROUP BY seller_id
    --             ) AS t7
    -- ON      t1.seller_id = t7.seller_id
    -- LEFT JOIN   (
    --                 SELECT  *
    --                 FROM    lazada_analyst_dev.loutruong_slr_non_trusted_dbs
    --             ) AS t8
    -- ON      t1.seller_id = t8.seller_id
    -- LEFT JOIN   (
    --                 SELECT  seller_id
    --                         ,is_mp3_seller
    --                 FROM    lazada_cdm.dim_lzd_slr_seller_vn
    --                 WHERE   1 = 1
    --                 AND     ds = MAX_PT('lazada_cdm.dim_lzd_slr_seller')
    --                 AND     venture = 'VN'
    --             ) AS t9
    -- ON      t1.seller_id = t9.seller_id
    LEFT JOIN (
        SELECT ds,
            seller_id,
            SUM(gmv_usd_1d) AS slr_gmv_1d,
            SUM(gmv_usd_30d) AS slr_gmv_30d,
            SUM(gmv_usd_60d) AS slr_gmv_60d,
            SUM(SUM(gmv_usd_1d)) OVER (PARTITION BY ds) AS plt_gmv_1d,
            SUM(SUM(gmv_usd_30d)) OVER (PARTITION BY ds) AS plt_gmv_30d,
            SUM(SUM(gmv_usd_60d)) OVER (PARTITION BY ds) AS plt_gmv_60d,
            SUM(sku_ord_cnt_1d) AS slr_ord_1d,
            SUM(sku_ord_cnt_30d) AS slr_ord_30d,
            SUM(sku_ord_cnt_60d) AS slr_ord_60d,
            SUM(SUM(sku_ord_cnt_1d)) OVER (PARTITION BY ds) AS plt_ord_1d,
            SUM(SUM(sku_ord_cnt_30d)) OVER (PARTITION BY ds) AS plt_ord_30d,
            SUM(SUM(sku_ord_cnt_60d)) OVER (PARTITION BY ds) AS plt_ord_60d
        FROM lazada_cdm.dws_lzd_slr_all_nd_v_vn
        WHERE 1 = 1
            AND ds >= 20240925
            AND venture = 'VN'
        GROUP BY ds,
            seller_id
    ) AS t10 ON t1.seller_id = t10.seller_id
    AND t1.ds = t10.ds
WHERE 1 = 1
    AND t1.seller_id NOT IN (300425408076)
GROUP BY t1.ds,
    CUBE(
        COALESCE(t1.vcm_status, 'Unknown'),
        COALESCE(t6.business_type, 'Unknown'),
        COALESCE(t6.business_type_level2, 'Unknown'),
        COALESCE(t6.industry_name, 'Unknown'),
        COALESCE(t6.main_category_name, 'Unknown'),
        COALESCE(t5.new_seller_segment, 'Unknown'),
        COALESCE(t5.pic_lead_name, 'Unknown'),
        COALESCE(t5.pic_status, 'Unknown')
    )
ORDER BY t1.ds,
    COALESCE(t1.vcm_status, 'Unknown'),
    COALESCE(t6.business_type, 'Unknown'),
    COALESCE(t6.business_type_level2, 'Unknown'),
    COALESCE(t6.industry_name, 'Unknown'),
    COALESCE(t6.main_category_name, 'Unknown'),
    COALESCE(t5.new_seller_segment, 'Unknown'),
    COALESCE(t5.pic_lead_name, 'Unknown'),
    COALESCE(t5.pic_status, 'Unknown');
-- MaxCompute SQL 
-- ********************************************************************--
-- author:Truong, Van Thanh
-- create time:2024-12-04 16:22:46
-- ********************************************************************--
SELECT t1.*,
    t2.vcm_fee AS vcm_fee,
    CASE
        WHEN t1.venture_category1_id IN (
            10100058,
            15267,
            15268,
            10100047,
            10003005,
            15273,
            2959
        )
        OR t1.venture_category3_id IN (9683, 5993)
        OR t1.venture_category4_id IN (10554, 10555, 12397) THEN 1
        ELSE 0
    END AS is_rebate
FROM (
        SELECT *
        FROM lazada_cdm.dwd_lzd_trd_core_fulfill_di
        WHERE 1 = 1
            AND TO_CHAR(TO_DATE(ds, 'yyyymmdd'), 'yyyymm') >= 202411
            AND venture = 'VN'
            AND is_revenue = 1
            AND COALESCE(business_application, 'LZD') IN ('LZD,ZAL', 'LZD')
    ) AS t1
    LEFT JOIN (
        SELECT sales_order_item_id,
            ABS(
                COALESCE(KEYVALUE(exp_comm_amt_detail, 'LPI'), 0)
            ) AS vcm_fee
        FROM lazada_ent_cdm.dwd_lzd_fin_trd_commission_di
        WHERE 1 = 1
            AND TO_CHAR(TO_DATE(ds, 'yyyymmdd'), 'yyyymm') >= 202411
            AND venture = 'VN'
    ) AS t2 ON t1.sales_order_item_id = t2.sales_order_item_id
    INNER JOIN (
        SELECT DISTINCT t1.ds AS ds,
            t1.seller_id AS seller_id
        FROM (
                SELECT ds,
                    program_id,
                    seller_id
                FROM lazada_ds.ds_lzd_program_seller
                WHERE 1 = 1
                    AND TO_CHAR(TO_DATE(ds, 'yyyymmdd'), 'yyyymm') >= 202411
                    AND venture = 'VN'
                    AND status = 1
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
            INNER JOIN (
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
    ) AS t3 ON t1.ds = t3.ds
    AND t1.seller_id = t3.seller_id;
SELECT t1.ds AS ds,
    t1.seller_id AS seller_id
FROM (
        SELECT ds,
            seller_id
        FROM lazada_ds.ds_lzd_program_seller
        WHERE 1 = 1
            AND TO_CHAR(TO_DATE(ds, 'yyyymmdd'), 'yyyymm') >= 202411
            AND venture = 'VN'
            AND status = 1
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
    INNER JOIN (
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
    ) AS t3 ON t1.seller_id = t3.seller_id;
SELECT ds,
    COUNT(DISTINCT seller_id) AS slr_cnt,
    COUNT(*)
FROM (
        SELECT t1.ds AS ds,
            t1.seller_id AS seller_id,
            COALESCE(t4.program, t2.program) AS program,
            CASE
                WHEN COALESCE(t4.program, t2.program) IN ('4% cap 50k') THEN '1. Standard offer'
                ELSE '2. Special offer'
            END AS program_type
        FROM (
                SELECT ds,
                    program_id,
                    seller_id
                FROM lazada_ds.ds_lzd_program_seller
                WHERE 1 = 1
                    AND ds BETWEEN 20241201 AND 20241206 --<< Change
                    AND venture = 'VN'
                    AND status = 1
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
                    AND ds BETWEEN 20241201 AND 20241206 --<< Change
                    -- AND     venture = 'VN'
                    AND ds BETWEEN TO_CHAR(
                        TO_DATE(
                            sg_udf :epoch_to_timezone(gmt_modified, 'VN'),
                            'yyyy-mm-dd hh:mi:ss'
                        ),
                        'yyyymmdd'
                    ) AND 20241206 --
                    -- TO_CHAR(GETDATE(),'yyyymmdd')
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
            AND t1.ds = t3.ds
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
                    AND ds BETWEEN 20241201 AND 20241206 --<< Change
                    AND venture = 'VN'
                    AND COALESCE(rate_type, 'DEFAULT') NOT IN ('CATEGORY', 'DEFAULT') --
                    -- AND     ds BETWEEN TO_CHAR(sg_udf:epoch_to_timezone(effect_start_time,venture),'yyyymmdd') AND TO_CHAR(sg_udf:epoch_to_timezone(effect_end_time,venture),'yyyymmdd')
                    -- AND     program_id IN (1534,1564)
            ) AS t4 ON t1.ds = t4.ds
            AND t2.id = t4.program_id
            AND t3.slr_tag_wl = t4.slr_tag_wl
    )
GROUP BY ds;
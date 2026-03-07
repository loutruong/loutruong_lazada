-- MaxCompute SQL 
-- ********************************************************************--
-- author:Truong, Van Thanh
-- create time:2024-08-07 16:51:15
-- ********************************************************************--
SELECT TO_CHAR(TO_DATE(t1.mm, 'yyyy-mm'), 'yyyymm') AS mm,
    t1.ds AS ds,
    CONCAT(
        'Week:',
        ' ',
        t1.week_start,
        ' ',
        '-->>',
        ' ',
        t1.week_end
    ) AS week,
    COALESCE(t2.master_campaign_id, 'Bau') AS master_campaign_id,
    COALESCE(t2.master_campaign_name, 'Bau') AS master_campaign_name,
    COALESCE(t2.camp_type, 'Bau') AS camp_type,
    COALESCE(t2.period, 'Bau') AS period
FROM (
        SELECT text_month AS mm,
            ds_day AS ds,
            MAX(ds_day) OVER (PARTITION BY text_week) AS week_end,
            MIN(ds_day) OVER (PARTITION BY text_week) AS week_start
        FROM lazada_cdm.dim_lzd_date
        WHERE 1 = 1
            AND ds_day BETWEEN 20240401 AND '${end_date}'
    ) AS t1
    LEFT JOIN (
        SELECT *
        FROM lazada_analyst.datx_campaign_list
        WHERE 1 = 1
            AND ds >= 20240401
    ) AS t2 ON t1.ds = t2.ds
ORDER BY t1.ds;
SELECT *
FROM lazada_cdm.dim_lzd_prd_category_tree_regional
WHERE 1 = 1
    AND ds = MAX_PT('lazada_cdm.dim_lzd_prd_category_tree_regional');
SELECT *
FROM lazada_cdm.dim_lzd_prd_category_tree_local
WHERE 1 = 1
    AND ds = MAX_PT('lazada_cdm.dim_lzd_prd_category_tree_local')
    AND venture = 'VN'
    AND (
        venture_category1_id IN (
            10100058,
            15267,
            15268,
            10100047,
            10003005,
            15273,
            2959
        )
        OR venture_category3_id IN (9683, 5993)
        OR venture_category4_id IN (10554, 10555, 12397)
    );
-- SELECT  *
-- FROM    lazada_ds.ds_lzd_program_commission_settlement_config
-- WHERE   1 = 1
-- AND     ds = MAX_PT('lazada_ds.ds_lzd_program_commission_settlement_config')
-- AND     venture = "VN"
-- AND     id = 2368
-- ;
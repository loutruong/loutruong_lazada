-- MaxCompute SQL 
-- ********************************************************************--
-- author:Truong, Van Thanh
-- create time:2023-12-18 14:59:40
-- ********************************************************************--
--@@ Input = lazada_analyst.loutruong_aff_mem_id_offline_info ==>> lazada_analyst_dev.loutruong_aff_mem_id_offline_info
--@@ Input = lazada_cdm.dim_lzd_date
--@@ Input = lazada_analyst_dev.loutruong_aff_perf_plm_ops_di
--@@ Input = lazada_analyst_dev.loutruong_trd_core_fulfill_ext_trf_di
--@@ Input = lazada_analyst_dev.hg_aff_positive_pc25_monthly
WITH t_partner_info AS (
    SELECT t2.mm AS month_perf,
        t2.ds AS ds_perf,
        t1.month_apply AS month_data_use_run,
        t1.ds AS date_data_snapshot,
        t1.ms AS month_data_snapshot,
        t1.cycle AS cycle,
        t1.exclude_reason AS exclude_reason,
        t1.exclude_number AS exclude_number,
        t1.member_id AS member_id,
        t1.member_name AS member_name,
        t1.segment_inventory AS segment_inventory,
        t1.team AS team,
        t1.plm_tier AS plm_tier,
        t1.base_cms_tier AS base_cms_tier,
        t1.pc25_tier AS pc25_tier,
        t1.pic AS pic
    FROM (
            SELECT month_apply,
                ds,
                ms,
                cycle,
                exclude_reason,
                exclude_number,
                member_id,
                member_name,
                segment_inventory,
                team_manage AS team,
                member_plm_local_lv2 AS plm_tier,
                base_cms_tier,
                pc25_tier,
                member_pic_local AS pic,
                1 AS map_key
            FROM lazada_analyst_dev.loutruong_aff_mem_id_offline_info --<< Change
            WHERE 1 = 1
                AND month_apply IN (
                    SELECT MAX(month_apply)
                    FROM lazada_analyst_dev.loutruong_aff_mem_id_offline_info --<< Change
                )
        ) AS t1
        INNER JOIN (
            SELECT TO_CHAR(TO_DATE(text_month, 'yyyy-mm'), 'yyyymm') AS mm,
                ds_day AS ds,
                1 AS map_key
            FROM lazada_cdm.dim_lzd_date
            WHERE 1 = 1 --
                -- AND     ds_day = '${bizdate}' --<< CHANGE PARAMATER TEST
                AND ds_day BETWEEN '20230301' AND TO_CHAR(DATEADD(GETDATE(), -1, 'dd'), 'yyyymmdd') --<< CHANGE PARAMATER PRODUCTION
        ) AS t2 ON t1.map_key = t2.map_key
),
t_partner_di AS (
    SELECT t1.month_perf AS month_perf,
        t1.ds_perf AS ds_perf,
        t1.month_data_use_run AS month_data_use_run,
        t1.date_data_snapshot AS date_data_snapshot,
        t1.month_data_snapshot AS month_data_snapshot,
        t1.cycle AS cycle,
        t1.exclude_reason AS exclude_reason,
        t1.exclude_number AS exclude_number,
        t1.member_id AS member_id,
        t1.member_name AS member_name,
        t1.segment_inventory AS segment_inventory,
        t1.team AS team,
        t1.plm_tier AS plm_tier,
        t1.base_cms_tier AS base_cms_tier,
        t1.pc25_tier AS pc25_tier,
        t1.pic AS pic,
CASE
            WHEN COALESCE(t2.app_visit_cnt_pc_session, 0) > 0 THEN t1.member_id
            ELSE NULL
        END AS engaged_member_id,
CASE
            WHEN COALESCE(t2.order_cnt_lt_1d_p, 0) > 0 THEN t1.member_id
            ELSE NULL
        END AS active_member_id,
        COALESCE(t2.app_click_cnt, 0) AS click_cnt,
        COALESCE(t2.app_install_cnt_lt_1d, 0) AS app_install_cnt_lt_1d,
        COALESCE(t2.app_reinstall_cnt_lt_1d, 0) AS app_reinstall_cnt_lt_1d,
        COALESCE(t2.register_cnt_ft_1d_np, 0) AS register_cnt_ft_1d_np,
        COALESCE(t2.total_payout_usd, 0) AS total_payout_usd,
        COALESCE(t2.platform_payout_usd, 0) AS platform_payout_usd,
        COALESCE(t2.brand_payout_usd, 0) AS brand_payout_usd,
        COALESCE(t2.app_visit_cnt_ft_1d_np, 0) AS app_visit_cnt_ft_1d_np,
        COALESCE(t2.app_dau_cnt_ft_1d_np, 0) AS app_dau_cnt_ft_1d_np,
        COALESCE(t2.app_pdp_pv_ft_1d_np, 0) AS app_pdp_pv_ft_1d_np,
        COALESCE(t2.app_pdp_uv_cnt_ft_1d_np, 0) AS app_pdp_uv_cnt_ft_1d_np,
        COALESCE(t2.a2c_cnt_lt_1d_p, 0) AS a2c_cnt_lt_1d_p,
        COALESCE(t2.a2c_uv_cnt_lt_1d_p, 0) AS a2c_uv_cnt_lt_1d_p,
        COALESCE(t2.buyer_cnt_lt_1d_p, 0) AS buyer_cnt_lt_1d_p,
        COALESCE(t2.existing_buyer_cnt_lt_1d_p, 0) AS existing_buyer_cnt_lt_1d_p,
        COALESCE(t2.new_buyer_cnt_lt_1d_p, 0) AS new_buyer_cnt_lt_1d_p,
        COALESCE(t2.reacquired_buyer_cnt_lt_1d_p, 0) AS reacquired_buyer_cnt_lt_1d_p,
        COALESCE(t2.order_cnt_lt_1d_p, 0) AS order_cnt_lt_1d_p,
        COALESCE(t3.pc25_positive_order_cnt_lt_1d_p, 0) AS pc25_positive_order_cnt_lt_1d_p,
        COALESCE(t3.pc25_negative_order_cnt_lt_1d_p, 0) AS pc25_negative_order_cnt_lt_1d_p,
        COALESCE(t2.sa_order_cnt_lt_1d_p, 0) AS sa_order_cnt_lt_1d_p,
        COALESCE(t2.non_sa_order_cnt_lt_1d_p, 0) AS non_sa_order_cnt_lt_1d_p,
        COALESCE(t2.gmv_usd_lt_1d_p, 0) AS gmv_usd_lt_1d_p,
        COALESCE(t2.sa_gmv_usd_lt_1d_p, 0) AS sa_gmv_usd_lt_1d_p,
        COALESCE(t2.non_sa_gmv_usd_lt_1d_p, 0) AS non_sa_gmv_usd_lt_1d_p,
        COALESCE(t3.pc25_positive_gmv_usd_lt_1d_p, 0) AS pc25_positive_gmv_usd_lt_1d_p,
        COALESCE(t3.pc25_negative_gmv_usd_lt_1d_p, 0) AS pc25_negative_gmv_usd_lt_1d_p
    FROM (
            SELECT month_perf,
                ds_perf,
                month_data_use_run,
                date_data_snapshot,
                month_data_snapshot,
                cycle,
                exclude_reason,
                exclude_number,
                member_id,
                member_name,
                segment_inventory,
                team,
                plm_tier,
                base_cms_tier,
                pc25_tier,
                pic
            FROM t_partner_info
        ) AS t1
        LEFT JOIN (
            SELECT ds,
                member_id,
                app_click_cnt,
                app_install_cnt_lt_1d,
                app_reinstall_cnt_lt_1d,
                register_cnt_ft_1d_np,
                total_payout_usd,
                platform_payout_usd,
                brand_payout_usd,
                app_visit_cnt_pc_session,
                app_visit_cnt_ft_1d_np,
                app_dau_cnt_ft_1d_np,
                app_pdp_pv_ft_1d_np,
                app_pdp_uv_cnt_ft_1d_np,
                a2c_cnt_lt_1d_p,
                a2c_uv_cnt_lt_1d_p,
                buyer_cnt_lt_1d_p,
                existing_buyer_cnt_lt_1d_p,
                new_buyer_cnt_lt_1d_p,
                reacquired_buyer_cnt_lt_1d_p,
                order_cnt_lt_1d_p,
                sa_order_cnt_lt_1d_p,
                non_sa_order_cnt_lt_1d_p,
                gmv_usd_lt_1d_p,
                sa_gmv_usd_lt_1d_p,
                non_sa_gmv_usd_lt_1d_p
            FROM lazada_analyst_dev.loutruong_aff_perf_plm_ops_di
            WHERE 1 = 1
                AND ds IN (
                    SELECT DISTINCT ds_perf
                    FROM t_partner_info
                )
        ) AS t2 ON t1.ds_perf = t2.ds
        AND t1.member_id = t2.member_id
        LEFT JOIN (
            SELECT t1.ds AS ds,
                t1.member_id AS member_id,
                COUNT(
                    DISTINCT CASE
                        WHEN t2.seller_id IS NOT NULL THEN t1.order_id
                        ELSE NULL
                    END
                ) AS pc25_positive_order_cnt_lt_1d_p,
                COUNT(
                    DISTINCT CASE
                        WHEN t2.seller_id IS NULL THEN t1.order_id
                        ELSE NULL
                    END
                ) AS pc25_negative_order_cnt_lt_1d_p,
                SUM(
                    CASE
                        WHEN t2.seller_id IS NOT NULL THEN t1.gmv_usd
                        ELSE 0
                    END
                ) AS pc25_positive_gmv_usd_lt_1d_p,
                SUM(
                    CASE
                        WHEN t2.seller_id IS NULL THEN t1.gmv_usd
                        ELSE 0
                    END
                ) AS pc25_negative_gmv_usd_lt_1d_p
            FROM (
                    SELECT *
                    FROM lazada_analyst_dev.loutruong_trd_core_fulfill_ext_trf_di
                    WHERE 1 = 1
                        AND ds IN (
                            SELECT DISTINCT ds_perf
                            FROM t_partner_info
                        )
                        AND TOLOWER(funding_bucket) IN ('lazada om')
                        AND TOLOWER(funding_type) IN ('om', 'ams')
                        AND TOLOWER(sub_channel) IN ('cps affiliate')
                ) AS t1
                LEFT JOIN (
                    SELECT seller_id
                    FROM lazada_analyst_dev.hg_aff_positive_pc25_monthly
                    WHERE 1 = 1
                        AND ds = MAX_PT(
                            'lazada_analyst_dev.hg_aff_positive_pc25_monthly'
                        )
                        AND month IN (
                            TO_CHAR(
                                DATEADD(DATEADD(GETDATE(), -1, 'dd'), -1, 'mm'),
                                'yyyymm'
                            )
                        )
                ) AS t2 ON t1.seller_id = t2.seller_id
            GROUP BY t1.ds,
                t1.member_id
        ) AS t3 ON t1.ds_perf = t3.ds
        AND t1.member_id = t3.member_id
),
t_partner_monthly AS (
    SELECT t1.month_perf AS month_perf,
        t1.member_id AS member_id,
CASE
            WHEN t1.acc_order_cnt_lt_1d_p > 0
            AND t2.acc_order_cnt_lt_1d_p > 0 THEN '1.Existing'
            WHEN t1.acc_order_cnt_lt_1d_p > 0
            AND t2.acc_order_cnt_lt_1d_p = 0 THEN '2.Reacquired'
            WHEN t1.acc_order_cnt_lt_1d_p = 0
            AND t2.acc_order_cnt_lt_1d_p > 0 THEN '3.Churn'
            WHEN t1.acc_order_cnt_lt_1d_p = 0
            AND t2.acc_order_cnt_lt_1d_p = 0 THEN '4.Lost'
            ELSE 'Others'
        END AS simple_tier,
CASE
            WHEN t1.acc_order_cnt_lt_1d_p >= 3001 THEN '1.KA'
            WHEN t1.acc_order_cnt_lt_1d_p BETWEEN 301 AND 3000 THEN '2.SM'
            WHEN t1.acc_order_cnt_lt_1d_p BETWEEN 31 AND 300 THEN '3.LT'
            WHEN t1.acc_order_cnt_lt_1d_p BETWEEN 21 AND 30 THEN '5.Hipo'
            WHEN t1.acc_order_cnt_lt_1d_p BETWEEN 1 AND 20 THEN '6.Mass'
            WHEN t1.acc_order_cnt_lt_1d_p = 0 THEN '8.Inactive'
            ELSE 'Others'
        END AS plm_tier_monthly
    FROM (
            SELECT month_perf,
                member_id,
                AVG(order_cnt_lt_1d_p) AS avg_order_cnt_lt_1d_p,
                SUM(order_cnt_lt_1d_p) AS acc_order_cnt_lt_1d_p
            FROM t_partner_di
            WHERE 1 = 1
                AND month_perf BETWEEN '202304' AND TO_CHAR(DATEADD(GETDATE(), -1, 'dd'), 'yyyymm')
            GROUP BY month_perf,
                member_id
        ) AS t1
        LEFT JOIN (
            SELECT TO_CHAR(
                    DATEADD(TO_DATE(month_perf, 'yyyymm'), + 1, 'mm'),
                    'yyyymm'
                ) AS month_perf,
                member_id,
                AVG(order_cnt_lt_1d_p) AS avg_order_cnt_lt_1d_p,
                SUM(order_cnt_lt_1d_p) AS acc_order_cnt_lt_1d_p
            FROM t_partner_di
            WHERE 1 = 1
                AND month_perf BETWEEN '202303' AND TO_CHAR(
                    DATEADD(DATEADD(GETDATE(), -1, 'dd'), -1, 'mm'),
                    'yyyymm'
                )
            GROUP BY TO_CHAR(
                    DATEADD(TO_DATE(month_perf, 'yyyymm'), + 1, 'mm'),
                    'yyyymm'
                ),
                member_id
        ) AS t2 ON t1.month_perf = t2.month_perf
        AND t1.member_id = t2.member_id
),
t_partner_master AS (
    SELECT t1.month_perf AS month_perf,
        t1.ds_perf AS ds_perf,
        t1.month_data_use_run AS month_data_use_run,
        t1.date_data_snapshot AS date_data_snapshot,
        t1.month_data_snapshot AS month_data_snapshot,
        t1.cycle AS cycle,
        t1.exclude_reason AS exclude_reason,
        t1.exclude_number AS exclude_number,
        t1.member_id AS member_id,
        t1.member_name AS member_name,
        t1.segment_inventory AS segment_inventory,
        t1.team AS team,
        t1.plm_tier AS plm_tier,
        t1.base_cms_tier AS base_cms_tier,
        t1.pc25_tier AS pc25_tier,
        t1.pic AS pic,
        t2.simple_tier AS simple_tier,
        t2.plm_tier_monthly AS plm_tier_monthly,
        t1.engaged_member_id AS engaged_member_id,
        t1.active_member_id AS active_member_id,
        t1.click_cnt AS click_cnt,
        t1.app_install_cnt_lt_1d AS app_install_cnt_lt_1d,
        t1.app_reinstall_cnt_lt_1d AS app_reinstall_cnt_lt_1d,
        t1.register_cnt_ft_1d_np AS register_cnt_ft_1d_np,
        t1.total_payout_usd AS total_payout_usd,
        t1.platform_payout_usd AS platform_payout_usd,
        t1.brand_payout_usd AS brand_payout_usd,
        t1.app_visit_cnt_ft_1d_np AS app_visit_cnt_ft_1d_np,
        t1.app_dau_cnt_ft_1d_np AS app_dau_cnt_ft_1d_np,
        t1.app_pdp_pv_ft_1d_np AS app_pdp_pv_ft_1d_np,
        t1.app_pdp_uv_cnt_ft_1d_np AS app_pdp_uv_cnt_ft_1d_np,
        t1.a2c_cnt_lt_1d_p AS a2c_cnt_lt_1d_p,
        t1.a2c_uv_cnt_lt_1d_p AS a2c_uv_cnt_lt_1d_p,
        t1.buyer_cnt_lt_1d_p AS buyer_cnt_lt_1d_p,
        t1.existing_buyer_cnt_lt_1d_p AS existing_buyer_cnt_lt_1d_p,
        t1.new_buyer_cnt_lt_1d_p AS new_buyer_cnt_lt_1d_p,
        t1.reacquired_buyer_cnt_lt_1d_p AS reacquired_buyer_cnt_lt_1d_p,
        t1.order_cnt_lt_1d_p AS order_cnt_lt_1d_p,
        t1.pc25_positive_order_cnt_lt_1d_p AS pc25_positive_order_cnt_lt_1d_p,
        t1.pc25_negative_order_cnt_lt_1d_p AS pc25_negative_order_cnt_lt_1d_p,
        t1.sa_order_cnt_lt_1d_p AS sa_order_cnt_lt_1d_p,
        t1.non_sa_order_cnt_lt_1d_p AS non_sa_order_cnt_lt_1d_p,
        t1.gmv_usd_lt_1d_p AS gmv_usd_lt_1d_p,
        t1.sa_gmv_usd_lt_1d_p AS sa_gmv_usd_lt_1d_p,
        t1.non_sa_gmv_usd_lt_1d_p AS non_sa_gmv_usd_lt_1d_p,
        t1.pc25_positive_gmv_usd_lt_1d_p AS pc25_positive_gmv_usd_lt_1d_p,
        t1.pc25_negative_gmv_usd_lt_1d_p AS pc25_negative_gmv_usd_lt_1d_p
    FROM (
            SELECT month_perf,
                ds_perf,
                month_data_use_run,
                date_data_snapshot,
                month_data_snapshot,
                cycle,
                exclude_reason,
                exclude_number,
                member_id,
                member_name,
                segment_inventory,
                team,
                plm_tier,
                base_cms_tier,
                pc25_tier,
                pic,
                engaged_member_id,
                active_member_id,
                click_cnt,
                app_install_cnt_lt_1d,
                app_reinstall_cnt_lt_1d,
                register_cnt_ft_1d_np,
                total_payout_usd,
                platform_payout_usd,
                brand_payout_usd,
                app_visit_cnt_ft_1d_np,
                app_dau_cnt_ft_1d_np,
                app_pdp_pv_ft_1d_np,
                app_pdp_uv_cnt_ft_1d_np,
                a2c_cnt_lt_1d_p,
                a2c_uv_cnt_lt_1d_p,
                buyer_cnt_lt_1d_p,
                existing_buyer_cnt_lt_1d_p,
                new_buyer_cnt_lt_1d_p,
                reacquired_buyer_cnt_lt_1d_p,
                order_cnt_lt_1d_p,
                pc25_positive_order_cnt_lt_1d_p,
                pc25_negative_order_cnt_lt_1d_p,
                sa_order_cnt_lt_1d_p,
                non_sa_order_cnt_lt_1d_p,
                gmv_usd_lt_1d_p,
                sa_gmv_usd_lt_1d_p,
                non_sa_gmv_usd_lt_1d_p,
                pc25_positive_gmv_usd_lt_1d_p,
                pc25_negative_gmv_usd_lt_1d_p
            FROM t_partner_di
            WHERE 1 = 1
                AND month_perf BETWEEN '202304' AND TO_CHAR(DATEADD(GETDATE(), -1, 'dd'), 'yyyymm')
        ) AS t1
        LEFT JOIN (
            SELECT month_perf,
                member_id,
                simple_tier,
                plm_tier_monthly
            FROM t_partner_monthly
            WHERE 1 = 1
                AND month_perf BETWEEN '202304' AND TO_CHAR(DATEADD(GETDATE(), -1, 'dd'), 'yyyymm')
        ) AS t2 ON t1.month_perf = t2.month_perf
        AND t1.member_id = t2.member_id
),
t_partner_pc25_plm AS (
    SELECT month_perf,
        ds_perf,
        plm_tier,
        pc25_tier,
CASE
            WHEN pc25_positive_order_cnt_lt_1d_p IS NOT NULL THEN 'order'
        END AS metric_filter,
CASE
            WHEN pc25_positive_order_cnt_lt_1d_p IS NOT NULL THEN 'pc25_positive_order'
        END AS metric,
        SUM(
            CASE
                WHEN pc25_positive_order_cnt_lt_1d_p IS NOT NULL THEN pc25_positive_order_cnt_lt_1d_p
            END
        ) AS value
    FROM t_partner_master
    GROUP BY month_perf,
        ds_perf,
        plm_tier,
        pc25_tier,
CASE
            WHEN pc25_positive_order_cnt_lt_1d_p IS NOT NULL THEN 'order'
        END,
CASE
            WHEN pc25_positive_order_cnt_lt_1d_p IS NOT NULL THEN 'pc25_positive_order'
        END
    UNION ALL
    --<< BREAK POINT 1
    SELECT month_perf,
        ds_perf,
        plm_tier,
        pc25_tier,
CASE
            WHEN pc25_negative_order_cnt_lt_1d_p IS NOT NULL THEN 'order'
        END AS metric_filter,
CASE
            WHEN pc25_negative_order_cnt_lt_1d_p IS NOT NULL THEN 'pc25_negative_order'
        END AS metric,
        SUM(
            CASE
                WHEN pc25_negative_order_cnt_lt_1d_p IS NOT NULL THEN pc25_negative_order_cnt_lt_1d_p
            END
        ) AS value
    FROM t_partner_master
    GROUP BY month_perf,
        ds_perf,
        plm_tier,
        pc25_tier,
CASE
            WHEN pc25_negative_order_cnt_lt_1d_p IS NOT NULL THEN 'order'
        END,
CASE
            WHEN pc25_negative_order_cnt_lt_1d_p IS NOT NULL THEN 'pc25_negative_order'
        END
    UNION ALL
    --<< BREAK POINT 2
    SELECT month_perf,
        ds_perf,
        plm_tier,
        pc25_tier,
CASE
            WHEN pc25_positive_gmv_usd_lt_1d_p IS NOT NULL THEN 'gmv'
        END AS metric_filter,
CASE
            WHEN pc25_positive_gmv_usd_lt_1d_p IS NOT NULL THEN 'pc25_positive_gmv'
        END AS metric,
        SUM(
            CASE
                WHEN pc25_positive_gmv_usd_lt_1d_p IS NOT NULL THEN pc25_positive_gmv_usd_lt_1d_p
            END
        ) AS value
    FROM t_partner_master
    GROUP BY month_perf,
        ds_perf,
        plm_tier,
        pc25_tier,
CASE
            WHEN pc25_positive_gmv_usd_lt_1d_p IS NOT NULL THEN 'gmv'
        END,
CASE
            WHEN pc25_positive_gmv_usd_lt_1d_p IS NOT NULL THEN 'pc25_positive_gmv'
        END
    UNION ALL
    --<< BREAK POINT 3
    SELECT month_perf,
        ds_perf,
        plm_tier,
        pc25_tier,
CASE
            WHEN pc25_negative_gmv_usd_lt_1d_p IS NOT NULL THEN 'gmv'
        END AS metric_filter,
CASE
            WHEN pc25_negative_gmv_usd_lt_1d_p IS NOT NULL THEN 'pc25_negative_gmv'
        END AS metric,
        SUM(
            CASE
                WHEN pc25_negative_gmv_usd_lt_1d_p IS NOT NULL THEN pc25_negative_gmv_usd_lt_1d_p
            END
        ) AS value
    FROM t_partner_master
    GROUP BY month_perf,
        ds_perf,
        plm_tier,
        pc25_tier,
CASE
            WHEN pc25_negative_gmv_usd_lt_1d_p IS NOT NULL THEN 'gmv'
        END,
CASE
            WHEN pc25_negative_gmv_usd_lt_1d_p IS NOT NULL THEN 'pc25_negative_gmv'
        END
),
t_partner_ads AS (
    SELECT month_perf,
        ds_perf,
        COALESCE(COALESCE(segment_inventory, 'Unknown'), '00_all') AS segment_inventory,
        COALESCE(COALESCE(team, 'Unknown'), '00_all') AS team,
        COALESCE(COALESCE(plm_tier, 'Unknown'), '00_all') AS plm_tier,
        COALESCE(COALESCE(pic, 'Unknown'), '00_all') AS pic,
        COUNT(DISTINCT member_id) AS pool,
        COUNT(DISTINCT engaged_member_id) AS engaged,
        COUNT(DISTINCT active_member_id) AS active,
        SUM(click_cnt) AS click,
        SUM(app_install_cnt_lt_1d) AS app_install,
        SUM(app_reinstall_cnt_lt_1d) AS app_re_install,
        SUM(register_cnt_ft_1d_np) AS register,
        SUM(total_payout_usd) AS po,
        SUM(platform_payout_usd) AS plt_po,
        SUM(brand_payout_usd) AS brd_po,
        SUM(app_visit_cnt_ft_1d_np) AS app_visit,
        SUM(app_dau_cnt_ft_1d_np) AS app_dau,
        SUM(app_pdp_pv_ft_1d_np) AS app_pdp_pv,
        SUM(app_pdp_uv_cnt_ft_1d_np) AS app_pdp_uv,
        SUM(a2c_cnt_lt_1d_p) AS a2c,
        SUM(a2c_uv_cnt_lt_1d_p) AS a2c_uv,
        SUM(buyer_cnt_lt_1d_p) AS buyer,
        SUM(existing_buyer_cnt_lt_1d_p) AS existing_buyer,
        SUM(new_buyer_cnt_lt_1d_p) AS new_buyer,
        SUM(reacquired_buyer_cnt_lt_1d_p) AS reacquired_buyer,
        SUM(order_cnt_lt_1d_p) AS order,
        SUM(pc25_positive_order_cnt_lt_1d_p) AS pc25_positive_order,
        SUM(pc25_negative_order_cnt_lt_1d_p) AS pc25_negative_order,
        SUM(sa_order_cnt_lt_1d_p) AS sa_order,
        SUM(non_sa_order_cnt_lt_1d_p) AS non_sa_order,
        SUM(gmv_usd_lt_1d_p) AS gmv,
        SUM(sa_gmv_usd_lt_1d_p) AS sa_gmv,
        SUM(non_sa_gmv_usd_lt_1d_p) AS non_sa_gmv,
        SUM(pc25_positive_gmv_usd_lt_1d_p) AS pc25_positive_gmv,
        SUM(pc25_negative_gmv_usd_lt_1d_p) AS pc25_negative_gmv
    FROM t_partner_master
    GROUP BY month_perf,
        ds_perf,
        CUBE(
            COALESCE(segment_inventory, 'Unknown'),
            COALESCE(team, 'Unknown'),
            COALESCE(plm_tier, 'Unknown'),
            COALESCE(pic, 'Unknown')
        )
)
SELECT data_label,
    month_perf,
    ds_perf,
    month_data_use_run,
    date_data_snapshot,
    month_data_snapshot,
    cycle,
    exclude_reason,
    exclude_number,
    member_id,
    member_name,
    segment_inventory,
    team,
    plm_tier,
    base_cms_tier,
    pc25_tier,
    pic,
    simple_tier,
    plm_tier_monthly,
    metric_filter,
    metric,
    value,
    pool,
    engaged,
    active,
    IF(pool = 0, 0, engaged / pool) AS engaged_rate,
    IF(active = 0, 0, active / engaged) AS active_rate,
    IF(active = 0, 0, gmv / active) AS gmv_per_active,
    IF(active = 0, 0, sa_gmv / active) AS sa_gmv_per_active,
    IF(active = 0, 0, order / active) AS order_per_active,
    IF(active = 0, 0, sa_order / active) AS sa_order_per_active,
    IF(active = 0, 0, buyer / active) AS buyer_per_active,
    IF(active = 0, 0, new_buyer / active) AS new_buyer_per_active,
    IF(active = 0, 0, po / active) AS po_per_active,
    IF(active = 0, 0, plt_po / active) AS plt_po_per_active,
    IF(active = 0, 0, brd_po / active) AS brd_po_per_active,
    click,
    app_install,
    app_re_install,
    register,
    po,
    plt_po,
    brd_po,
    app_visit,
    app_dau,
    app_pdp_pv,
    app_pdp_uv,
    a2c,
    a2c_uv,
    buyer,
    existing_buyer,
    new_buyer,
    reacquired_buyer,
    order,
    pc25_positive_order,
    pc25_negative_order,
    sa_order,
    non_sa_order,
    gmv,
    sa_gmv,
    non_sa_gmv,
    pc25_positive_gmv,
    pc25_negative_gmv,
    IF(po = 0, 0, brd_po / po) AS sa_rate,
    IF(app_dau = 0, 0, plt_po / app_dau) AS cdau,
    IF(buyer = 0, 0, plt_po / buyer) AS cpb,
    IF(order = 0, 0, plt_po / order) AS cpo,
    IF(gmv = 0, 0, plt_po / gmv) AS cir,
    IF(app_dau = 0, 0, buyer / app_dau) AS purchase_rate,
    IF(app_dau = 0, 0, a2c_uv / app_dau) AS a2c_uv_rate,
    IF(app_dau = 0, 0, app_pdp_uv / app_dau) AS pdp_uv_rate,
    IF(buyer = 0, 0, order / buyer) AS order_per_buyer,
    IF(order = 0, 0, gmv / order) AS gmv_per_order,
    IF(buyer = 0, 0, gmv / buyer) AS gmv_per_buyer
FROM (
        SELECT 'pc25' AS data_label,
            month_perf,
            ds_perf,
            NULL AS month_data_use_run,
            NULL AS date_data_snapshot,
            NULL AS month_data_snapshot,
            NULL AS cycle,
            NULL AS exclude_reason,
            NULL AS exclude_number,
            NULL AS member_id,
            NULL AS member_name,
            NULL AS segment_inventory,
            NULL AS team,
            plm_tier,
            NULL AS base_cms_tier,
            pc25_tier,
            NULL AS pic,
            NULL AS simple_tier,
            NULL AS plm_tier_monthly,
            metric_filter,
            metric,
            value,
            NULL AS pool,
            NULL AS engaged,
            NULL AS active,
            NULL AS click,
            NULL AS app_install,
            NULL AS app_re_install,
            NULL AS register,
            NULL AS po,
            NULL AS plt_po,
            NULL AS brd_po,
            NULL AS app_visit,
            NULL AS app_dau,
            NULL AS app_pdp_pv,
            NULL AS app_pdp_uv,
            NULL AS a2c,
            NULL AS a2c_uv,
            NULL AS buyer,
            NULL AS existing_buyer,
            NULL AS new_buyer,
            NULL AS reacquired_buyer,
            NULL AS order,
            NULL AS pc25_positive_order,
            NULL AS pc25_negative_order,
            NULL AS sa_order,
            NULL AS non_sa_order,
            NULL AS gmv,
            NULL AS sa_gmv,
            NULL AS non_sa_gmv,
            NULL AS pc25_positive_gmv,
            NULL AS pc25_negative_gmv
        FROM t_partner_pc25_plm
        UNION ALL
        SELECT 'ads' AS data_label,
            month_perf,
            ds_perf,
            NULL AS month_data_use_run,
            NULL AS date_data_snapshot,
            NULL AS month_data_snapshot,
            NULL AS cycle,
            NULL AS exclude_reason,
            NULL AS exclude_number,
            NULL AS member_id,
            NULL AS member_name,
            segment_inventory,
            team,
            plm_tier,
            NULL AS base_cms_tier,
            NULL AS pc25_tier,
            pic,
            NULL AS simple_tier,
            NULL AS plm_tier_monthly,
            NULL AS metric_filter,
            NULL AS metric,
            NULL AS value,
            pool,
            engaged,
            active,
            click,
            app_install,
            app_re_install,
            register,
            po,
            plt_po,
            brd_po,
            app_visit,
            app_dau,
            app_pdp_pv,
            app_pdp_uv,
            a2c,
            a2c_uv,
            buyer,
            existing_buyer,
            new_buyer,
            reacquired_buyer,
            order,
            pc25_positive_order,
            pc25_negative_order,
            sa_order,
            non_sa_order,
            gmv,
            sa_gmv,
            non_sa_gmv,
            pc25_positive_gmv,
            pc25_negative_gmv
        FROM t_partner_ads
        UNION ALL
        --<< Break point
        SELECT 'raw' AS data_label,
            month_perf,
            ds_perf,
            month_data_use_run,
            date_data_snapshot,
            month_data_snapshot,
            cycle,
            exclude_reason,
            exclude_number,
            member_id,
            member_name,
            segment_inventory,
            team,
            plm_tier,
            base_cms_tier,
            pc25_tier,
            pic,
            simple_tier,
            plm_tier_monthly,
            NULL AS metric_filter,
            NULL AS metric,
            NULL AS value,
            NULL AS pool,
            CAST(engaged_member_id AS BIGINT) AS engaged,
            CAST(active_member_id AS BIGINT) AS active,
            click_cnt AS click,
            app_install_cnt_lt_1d AS app_install,
            app_reinstall_cnt_lt_1d AS app_re_install,
            register_cnt_ft_1d_np AS register,
            total_payout_usd AS po,
            platform_payout_usd AS plt_po,
            brand_payout_usd AS brd_po,
            app_visit_cnt_ft_1d_np AS app_visit,
            app_dau_cnt_ft_1d_np AS app_dau,
            app_pdp_pv_ft_1d_np AS app_pdp_pv,
            app_pdp_uv_cnt_ft_1d_np AS app_pdp_uv,
            a2c_cnt_lt_1d_p AS a2c,
            a2c_uv_cnt_lt_1d_p AS a2c_uv,
            buyer_cnt_lt_1d_p AS buyer,
            existing_buyer_cnt_lt_1d_p AS existing_buyer,
            new_buyer_cnt_lt_1d_p AS new_buyer,
            reacquired_buyer_cnt_lt_1d_p AS reacquired_buyer,
            order_cnt_lt_1d_p AS order,
            pc25_positive_order_cnt_lt_1d_p AS pc25_positive_order,
            pc25_negative_order_cnt_lt_1d_p AS pc25_negative_order,
            sa_order_cnt_lt_1d_p AS sa_order,
            non_sa_order_cnt_lt_1d_p AS non_sa_order,
            gmv_usd_lt_1d_p AS gmv,
            sa_gmv_usd_lt_1d_p AS sa_gmv,
            non_sa_gmv_usd_lt_1d_p AS non_sa_gmv,
            pc25_positive_gmv_usd_lt_1d_p AS pc25_positive_gmv,
            pc25_negative_gmv_usd_lt_1d_p AS pc25_negative_gmv
        FROM t_partner_master
        WHERE 1 = 1
            AND TO_CHAR(TO_DATE(ds_perf, 'yyyymmdd'), 'yyyymm') >= TO_CHAR(
                DATEADD(DATEADD(GETDATE(), -1, 'dd'), -3, 'mm'),
                'yyyymm'
            )
    )
ORDER BY month_perf DESC,
    ds_perf DESC,
    metric_filter DESC,
    metric DESC,
    data_label ASC,
    segment_inventory ASC,
    team ASC,
    plm_tier ASC,
    base_cms_tier ASC,
    pc25_tier ASC,
    pic ASC,
    simple_tier ASC,
    plm_tier_monthly ASC;
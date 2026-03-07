-- MaxCompute SQL 
-- ********************************************************************--
-- author:Truong, Van Thanh
-- create time:2024-01-04 15:57:02
-- ********************************************************************--
--@@ Input = lazada_analyst_dev.tmp_loutruong_plt_perf_hh_1
--@@ Input = lazada_analyst_dev.tmp_loutruong_plt_perf_hh_2
--@@ Input = lazada_analyst_dev.tmp_loutruong_aff_perf_hh_1
--@@ Input = lazada_analyst_dev.tmp_loutruong_aff_perf_hh_2
--@@ Input = lazada_ads.ads_lzd_mkt_rt_uam_campaign_hh
--@@ Input = lazada_analyst_dev.hg_aff_plm_mixed
--@@ Input = lazada_ads_data.ads_lzd_cps_partner_info_df
--@@ Input = lazada_analyst_dev.hg_affiliate_target_plm_segment
WITH t_intra_plt AS (
    SELECT ds,
        hh,
        SUM(
            CASE
                WHEN TOLOWER(attr_model) IN ('ft_1d_np')
                AND TOLOWER(platform) IN ('app') THEN COALESCE(visits, 0)
                ELSE 0
            END
        ) AS app_visit_cnt_ft_1d_np,
        SUM(
            CASE
                WHEN TOLOWER(attr_model) IN ('ft_1d_np')
                AND TOLOWER(platform) IN ('app') THEN COALESCE(dau, 0)
                ELSE 0
            END
        ) AS app_dau_cnt_ft_1d_np,
        SUM(
            CASE
                WHEN TOLOWER(attr_model) IN ('lt_1d_p') THEN COALESCE(a2c, 0)
                ELSE 0
            END
        ) AS a2c_lt_1d_p,
        SUM(
            CASE
                WHEN TOLOWER(attr_model) IN ('lt_1d_p') THEN COALESCE(a2c_uv, 0)
                ELSE 0
            END
        ) AS a2c_uv_cnt_lt_1d_p,
        SUM(
            CASE
                WHEN TOLOWER(attr_model) IN ('lt_1d_p') THEN COALESCE(buyers, 0)
                ELSE 0
            END
        ) AS buyer_cnt_lt_1d_p,
        SUM(
            CASE
                WHEN TOLOWER(attr_model) IN ('lt_1d_p') THEN COALESCE(new_buyers, 0)
                ELSE 0
            END
        ) AS new_buyer_cnt_lt_1d_p,
        SUM(
            CASE
                WHEN TOLOWER(attr_model) IN ('lt_1d_p') THEN COALESCE(reacquired_buyers, 0)
                ELSE 0
            END
        ) AS reacquired_buyer_cnt_lt_1d_p,
        SUM(
            CASE
                WHEN TOLOWER(attr_model) IN ('lt_1d_p') THEN COALESCE(order_id_count, 0)
                ELSE 0
            END
        ) AS order_cnt_lt_1d_p,
        SUM(
            CASE
                WHEN TOLOWER(attr_model) IN ('lt_1d_p') THEN COALESCE(gmv, 0)
                ELSE 0
            END
        ) AS gmv_usd_lt_1d_p
    FROM lazada_ads.ads_lzd_mkt_rt_uam_campaign_hh
    WHERE 1 = 1
        AND ds > TO_CHAR(DATEADD(GETDATE(), -3, 'dd'), 'yyyymmdd')
        AND venture = 'VN'
        AND attr_model IN ('ft_1d_np', 'lt_1d_p')
    GROUP BY ds,
        hh
    UNION ALL
    SELECT ds,
        hh,
        app_visit_cnt_ft_1d_np,
        app_dau_cnt_ft_1d_np,
        a2c_lt_1d_p,
        a2c_uv_cnt_lt_1d_p,
        buyer_cnt_lt_1d_p,
        new_buyer_cnt_lt_1d_p,
        reacquired_buyer_cnt_lt_1d_p,
        order_cnt_lt_1d_p,
        gmv_usd_lt_1d_p
    FROM lazada_analyst_dev.tmp_loutruong_plt_perf_hh_1
    WHERE 1 = 1
        AND ds <= TO_CHAR(DATEADD(GETDATE(), -3, 'dd'), 'yyyymmdd')
),
t_intra_aff AS (
    SELECT t1.ds AS ds,
        t1.hh AS hh,
        COALESCE(t2.group_segment, 'unknown_group_segment') AS group_segment,
        COALESCE(t2.team_ops, 'unknown_team_ops') AS team_ops,
        COALESCE(t2.plm_l30d_lv1, 'unknown_plm_l30d_lv1') AS plm_l30d_lv1,
        COALESCE(t2.plm_l30d_lv2, 'unknown_plm_l30d_lv2') AS plm_l30d_lv2,
        COALESCE(t2.segment, 'unknown_segment') AS segment,
        t1.member_id AS member_id,
        t2.member_name AS member_name,
        t1.engaged_member_id_pc_session AS engaged_member_id_pc_session,
        t1.active_member_id_lt_1d_p AS active_member_id_lt_1d_p,
        t1.app_visit_cnt_ft_1d_np AS app_visit_cnt_ft_1d_np,
        t1.app_dau_cnt_ft_1d_np AS app_dau_cnt_ft_1d_np,
        t1.a2c_lt_1d_p AS a2c_lt_1d_p,
        t1.a2c_uv_cnt_lt_1d_p AS a2c_uv_cnt_lt_1d_p,
        t1.buyer_cnt_lt_1d_p AS buyer_cnt_lt_1d_p,
        t1.new_buyer_cnt_lt_1d_p AS new_buyer_cnt_lt_1d_p,
        t1.reacquired_buyer_cnt_lt_1d_p AS reacquired_buyer_cnt_lt_1d_p,
        t1.order_cnt_lt_1d_p AS order_cnt_lt_1d_p,
        t1.gmv_usd_lt_1d_p AS gmv_usd_lt_1d_p
    FROM (
            SELECT ds,
                hh,
                member_id,
                engaged_member_id_pc_session,
                active_member_id_lt_1d_p,
                app_visit_cnt_ft_1d_np,
                app_dau_cnt_ft_1d_np,
                a2c_lt_1d_p,
                a2c_uv_cnt_lt_1d_p,
                buyer_cnt_lt_1d_p,
                new_buyer_cnt_lt_1d_p,
                reacquired_buyer_cnt_lt_1d_p,
                order_cnt_lt_1d_p,
                gmv_usd_lt_1d_p
            FROM (
                    SELECT ds,
                        hh,
                        member_id,
CASE
                            WHEN engaged_member_id_dedup_pc_session = 1 THEN member_id
                            ELSE NULL
                        END AS engaged_member_id_pc_session,
CASE
                            WHEN active_member_id_dedup_lt_1d_p = 1 THEN member_id
                            ELSE NULL
                        END AS active_member_id_lt_1d_p,
                        app_visit_cnt_ft_1d_np,
                        app_dau_cnt_ft_1d_np,
                        a2c_lt_1d_p,
                        a2c_uv_cnt_lt_1d_p,
                        buyer_cnt_lt_1d_p,
                        new_buyer_cnt_lt_1d_p,
                        reacquired_buyer_cnt_lt_1d_p,
                        order_cnt_lt_1d_p,
                        gmv_usd_lt_1d_p
                    FROM (
                            SELECT ds,
                                hh,
                                member_id,
CASE
                                    WHEN app_visit_cnt_ft_1d_np > 0 THEN ROW_NUMBER() OVER (
                                        PARTITION BY ds,
                                        member_id,
                                        app_visit_cnt_ft_1d_np > 0
                                        ORDER BY hh ASC
                                    )
                                    ELSE NULL
                                END AS engaged_member_id_dedup_pc_session,
CASE
                                    WHEN order_cnt_lt_1d_p > 0 THEN ROW_NUMBER() OVER (
                                        PARTITION BY ds,
                                        member_id,
                                        order_cnt_lt_1d_p > 0
                                        ORDER BY hh ASC
                                    )
                                    ELSE NULL
                                END AS active_member_id_dedup_lt_1d_p,
                                app_visit_cnt_ft_1d_np,
                                app_dau_cnt_ft_1d_np,
                                a2c_lt_1d_p,
                                a2c_uv_cnt_lt_1d_p,
                                buyer_cnt_lt_1d_p,
                                new_buyer_cnt_lt_1d_p,
                                reacquired_buyer_cnt_lt_1d_p,
                                order_cnt_lt_1d_p,
                                gmv_usd_lt_1d_p
                            FROM (
                                    SELECT ds,
                                        hh,
                                        member_id,
                                        SUM(
                                            CASE
                                                WHEN TOLOWER(attr_model) IN ('ft_1d_np')
                                                AND TOLOWER(platform) IN ('app') THEN COALESCE(visits, 0)
                                                ELSE 0
                                            END
                                        ) AS app_visit_cnt_ft_1d_np,
                                        SUM(
                                            CASE
                                                WHEN TOLOWER(attr_model) IN ('ft_1d_np')
                                                AND TOLOWER(platform) IN ('app') THEN COALESCE(dau, 0)
                                                ELSE 0
                                            END
                                        ) AS app_dau_cnt_ft_1d_np,
                                        SUM(
                                            CASE
                                                WHEN TOLOWER(attr_model) IN ('lt_1d_p') THEN COALESCE(a2c, 0)
                                                ELSE 0
                                            END
                                        ) AS a2c_lt_1d_p,
                                        SUM(
                                            CASE
                                                WHEN TOLOWER(attr_model) IN ('lt_1d_p') THEN COALESCE(a2c_uv, 0)
                                                ELSE 0
                                            END
                                        ) AS a2c_uv_cnt_lt_1d_p,
                                        SUM(
                                            CASE
                                                WHEN TOLOWER(attr_model) IN ('lt_1d_p') THEN COALESCE(buyers, 0)
                                                ELSE 0
                                            END
                                        ) AS buyer_cnt_lt_1d_p,
                                        SUM(
                                            CASE
                                                WHEN TOLOWER(attr_model) IN ('lt_1d_p') THEN COALESCE(new_buyers, 0)
                                                ELSE 0
                                            END
                                        ) AS new_buyer_cnt_lt_1d_p,
                                        SUM(
                                            CASE
                                                WHEN TOLOWER(attr_model) IN ('lt_1d_p') THEN COALESCE(reacquired_buyers, 0)
                                                ELSE 0
                                            END
                                        ) AS reacquired_buyer_cnt_lt_1d_p,
                                        SUM(
                                            CASE
                                                WHEN TOLOWER(attr_model) IN ('lt_1d_p') THEN COALESCE(order_id_count, 0)
                                                ELSE 0
                                            END
                                        ) AS order_cnt_lt_1d_p,
                                        SUM(
                                            CASE
                                                WHEN TOLOWER(attr_model) IN ('lt_1d_p') THEN COALESCE(gmv, 0)
                                                ELSE 0
                                            END
                                        ) AS gmv_usd_lt_1d_p
                                    FROM lazada_ads.ads_lzd_mkt_rt_uam_campaign_hh
                                    WHERE 1 = 1
                                        AND ds > TO_CHAR(DATEADD(GETDATE(), -2, 'dd'), 'yyyymmdd')
                                        AND venture = 'VN'
                                        AND TOLOWER(funding_bucket) IN ('lazada om')
                                        AND TOLOWER(funding_type) IN ('om', 'ams')
                                        AND TOLOWER(sub_channel) IN ('cps affiliate', 'cpi affiliate')
                                        AND attr_model IN ('ft_1d_np', 'lt_1d_p')
                                    GROUP BY ds,
                                        hh,
                                        member_id
                                )
                        )
                )
            UNION ALL
            SELECT ds,
                hh,
                member_id,
                engaged_member_id_pc_session,
                active_member_id_lt_1d_p,
                app_visit_cnt_ft_1d_np,
                app_dau_cnt_ft_1d_np,
                a2c_lt_1d_p,
                a2c_uv_cnt_lt_1d_p,
                buyer_cnt_lt_1d_p,
                new_buyer_cnt_lt_1d_p,
                reacquired_buyer_cnt_lt_1d_p,
                order_cnt_lt_1d_p,
                gmv_usd_lt_1d_p
            FROM lazada_analyst_dev.tmp_loutruong_aff_perf_hh_1
            WHERE 1 = 1
                AND ds BETWEEN TO_CHAR(DATEADD(GETDATE(), -130, 'dd'), 'yyyymmdd') AND TO_CHAR(DATEADD(GETDATE(), -2, 'dd'), 'yyyymmdd')
        ) AS t1
        LEFT JOIN (
            SELECT group_segment,
                team_ops,
                plm_l30d_lv1,
                plm_l30d_lv2,
                segment,
                member_id,
                member_name
            FROM lazada_analyst_dev.hg_aff_plm_mixed
            WHERE 1 = 1
                AND ds = MAX_PT('lazada_analyst_dev.hg_aff_plm_mixed')
        ) AS t2 ON t1.member_id = t2.member_id
),
t_intra_group_segment AS (
    SELECT '01_group_segment' AS data_lable,
        t1.ds AS ds,
        t1.hh AS hh,
        t1.group_segment AS group_segment,
        '00_all' AS team_ops,
        '00_all' AS plm_l30d_lv1,
        '00_all' AS plm_l30d_lv2,
        '00_all' AS segment,
        '00_all' AS member_id,
        '00_all' AS member_name,
        t1.engaged_cnt_pc_session AS engaged_cnt_pc_session,
        t1.active_cnt_lt_1d_p AS active_cnt_lt_1d_p,
        t1.app_visit_cnt_ft_1d_np AS app_visit_cnt_ft_1d_np,
        t1.app_dau_cnt_ft_1d_np AS app_dau_cnt_ft_1d_np,
        t1.a2c_lt_1d_p AS a2c_lt_1d_p,
        t1.a2c_uv_cnt_lt_1d_p AS a2c_uv_cnt_lt_1d_p,
        t1.buyer_cnt_lt_1d_p AS buyer_cnt_lt_1d_p,
        t1.new_buyer_cnt_lt_1d_p AS new_buyer_cnt_lt_1d_p,
        t1.reacquired_buyer_cnt_lt_1d_p AS reacquired_buyer_cnt_lt_1d_p,
        t1.order_cnt_lt_1d_p AS order_cnt_lt_1d_p,
        t1.gmv_usd_lt_1d_p AS gmv_usd_lt_1d_p,
        t2.app_visit_cnt_ft_1d_np AS plt_app_visit_cnt_ft_1d_np,
        t2.app_dau_cnt_ft_1d_np AS plt_app_dau_cnt_ft_1d_np,
        t2.a2c_lt_1d_p AS plt_a2c_lt_1d_p,
        t2.a2c_uv_cnt_lt_1d_p AS plt_a2c_uv_cnt_lt_1d_p,
        t2.buyer_cnt_lt_1d_p AS plt_buyer_cnt_lt_1d_p,
        t2.new_buyer_cnt_lt_1d_p AS plt_new_buyer_cnt_lt_1d_p,
        t2.reacquired_buyer_cnt_lt_1d_p AS plt_reacquired_buyer_cnt_lt_1d_p,
        t2.order_cnt_lt_1d_p AS plt_order_cnt_lt_1d_p,
        t2.gmv_usd_lt_1d_p AS plt_gmv_usd_lt_1d_p,
        COALESCE(t3.tr_hh_engaged_cnt_pc_session, 0) AS tr_hh_engaged_cnt_pc_session,
        COALESCE(t3.tr_hh_active_cnt_lt_1d_p, 0) AS tr_hh_active_cnt_lt_1d_p,
        COALESCE(t3.tr_hh_app_dau_cnt_ft_1d_np, 0) AS tr_hh_app_dau_cnt_ft_1d_np,
        COALESCE(t3.tr_hh_buyer_cnt_lt_1d_p, 0) AS tr_hh_buyer_cnt_lt_1d_p,
        COALESCE(t3.tr_hh_order_cnt_lt_1d_p, 0) AS tr_hh_order_cnt_lt_1d_p,
        COALESCE(t3.tr_hh_gmv_usd_lt_1d_p, 0) AS tr_hh_gmv_usd_lt_1d_p,
        COALESCE(t4.tr_eod_engaged_cnt_pc_session, 0) AS tr_eod_engaged_cnt_pc_session,
        COALESCE(t4.tr_eod_active_cnt_lt_1d_p, 0) AS tr_eod_active_cnt_lt_1d_p,
        COALESCE(t4.tr_eod_app_dau_cnt_ft_1d_np, 0) AS tr_eod_app_dau_cnt_ft_1d_np,
        COALESCE(t4.tr_eod_buyer_cnt_lt_1d_p, 0) AS tr_eod_buyer_cnt_lt_1d_p,
        COALESCE(t4.tr_eod_order_cnt_lt_1d_p, 0) AS tr_eod_order_cnt_lt_1d_p,
        COALESCE(t4.tr_eod_gmv_usd_lt_1d_p, 0) AS tr_eod_gmv_usd_lt_1d_p
    FROM (
            SELECT ds,
                hh,
                COALESCE(group_segment, '00_all') AS group_segment,
                SUM(engaged_cnt_pc_session) AS engaged_cnt_pc_session,
                SUM(active_cnt_lt_1d_p) AS active_cnt_lt_1d_p,
                SUM(app_visit_cnt_ft_1d_np) AS app_visit_cnt_ft_1d_np,
                SUM(app_dau_cnt_ft_1d_np) AS app_dau_cnt_ft_1d_np,
                SUM(a2c_lt_1d_p) AS a2c_lt_1d_p,
                SUM(a2c_uv_cnt_lt_1d_p) AS a2c_uv_cnt_lt_1d_p,
                SUM(buyer_cnt_lt_1d_p) AS buyer_cnt_lt_1d_p,
                SUM(new_buyer_cnt_lt_1d_p) AS new_buyer_cnt_lt_1d_p,
                SUM(reacquired_buyer_cnt_lt_1d_p) AS reacquired_buyer_cnt_lt_1d_p,
                SUM(order_cnt_lt_1d_p) AS order_cnt_lt_1d_p,
                SUM(gmv_usd_lt_1d_p) AS gmv_usd_lt_1d_p
            FROM (
                    SELECT ds,
                        hh,
                        group_segment,
                        SUM(SUM(engaged_cnt_pc_session)) OVER (
                            PARTITION BY ds,
                            group_segment
                            ORDER BY hh ASC
                        ) AS engaged_cnt_pc_session,
                        SUM(SUM(active_cnt_lt_1d_p)) OVER (
                            PARTITION BY ds,
                            group_segment
                            ORDER BY hh ASC
                        ) AS active_cnt_lt_1d_p,
                        SUM(app_visit_cnt_ft_1d_np) AS app_visit_cnt_ft_1d_np,
                        SUM(app_dau_cnt_ft_1d_np) AS app_dau_cnt_ft_1d_np,
                        SUM(a2c_lt_1d_p) AS a2c_lt_1d_p,
                        SUM(a2c_uv_cnt_lt_1d_p) AS a2c_uv_cnt_lt_1d_p,
                        SUM(buyer_cnt_lt_1d_p) AS buyer_cnt_lt_1d_p,
                        SUM(new_buyer_cnt_lt_1d_p) AS new_buyer_cnt_lt_1d_p,
                        SUM(reacquired_buyer_cnt_lt_1d_p) AS reacquired_buyer_cnt_lt_1d_p,
                        SUM(order_cnt_lt_1d_p) AS order_cnt_lt_1d_p,
                        SUM(gmv_usd_lt_1d_p) AS gmv_usd_lt_1d_p
                    FROM (
                            SELECT ds,
                                hh,
                                group_segment,
                                COUNT(DISTINCT engaged_member_id_pc_session) AS engaged_cnt_pc_session,
                                COUNT(DISTINCT active_member_id_lt_1d_p) AS active_cnt_lt_1d_p,
                                SUM(app_visit_cnt_ft_1d_np) AS app_visit_cnt_ft_1d_np,
                                SUM(app_dau_cnt_ft_1d_np) AS app_dau_cnt_ft_1d_np,
                                SUM(a2c_lt_1d_p) AS a2c_lt_1d_p,
                                SUM(a2c_uv_cnt_lt_1d_p) AS a2c_uv_cnt_lt_1d_p,
                                SUM(buyer_cnt_lt_1d_p) AS buyer_cnt_lt_1d_p,
                                SUM(new_buyer_cnt_lt_1d_p) AS new_buyer_cnt_lt_1d_p,
                                SUM(reacquired_buyer_cnt_lt_1d_p) AS reacquired_buyer_cnt_lt_1d_p,
                                SUM(order_cnt_lt_1d_p) AS order_cnt_lt_1d_p,
                                SUM(gmv_usd_lt_1d_p) AS gmv_usd_lt_1d_p
                            FROM t_intra_aff
                            GROUP BY ds,
                                hh,
                                group_segment
                        )
                    GROUP BY ds,
                        hh,
                        group_segment
                )
            GROUP BY ds,
                hh,
                CUBE(group_segment)
        ) AS t1
        LEFT JOIN (
            SELECT ds,
                hh,
                app_visit_cnt_ft_1d_np,
                app_dau_cnt_ft_1d_np,
                a2c_lt_1d_p,
                a2c_uv_cnt_lt_1d_p,
                buyer_cnt_lt_1d_p,
                new_buyer_cnt_lt_1d_p,
                reacquired_buyer_cnt_lt_1d_p,
                order_cnt_lt_1d_p,
                gmv_usd_lt_1d_p
            FROM t_intra_plt
        ) AS t2 ON t1.ds = t2.ds
        AND t1.hh = t2.hh
        LEFT JOIN (
            SELECT ds,
                LPAD(hh, 2, '0') AS hh,
                COALESCE(group, '00_all') AS group_segment,
                SUM(engaged) AS tr_hh_engaged_cnt_pc_session,
                SUM(active) AS tr_hh_active_cnt_lt_1d_p,
                SUM(dau) AS tr_hh_app_dau_cnt_ft_1d_np,
                SUM(buyer) AS tr_hh_buyer_cnt_lt_1d_p,
                SUM(order) AS tr_hh_order_cnt_lt_1d_p,
                SUM(gmv) AS tr_hh_gmv_usd_lt_1d_p
            FROM lazada_analyst_dev.hg_affiliate_target_plm_segment
            GROUP BY ds,
                LPAD(hh, 2, '0'),
                CUBE(group)
        ) AS t3 ON t1.ds = t3.ds
        AND t1.hh = t3.hh
        AND t1.group_segment = t3.group_segment
        LEFT JOIN (
            SELECT ds,
                LPAD(hh, 2, '0') AS hh,
                COALESCE(group, '00_all') AS group_segment,
                SUM(engaged) AS tr_eod_engaged_cnt_pc_session,
                SUM(active) AS tr_eod_active_cnt_lt_1d_p,
                SUM(dau) AS tr_eod_app_dau_cnt_ft_1d_np,
                SUM(buyer) AS tr_eod_buyer_cnt_lt_1d_p,
                SUM(order) AS tr_eod_order_cnt_lt_1d_p,
                SUM(gmv) AS tr_eod_gmv_usd_lt_1d_p
            FROM lazada_analyst_dev.hg_affiliate_target_plm_segment
            WHERE 1 = 1
                AND hh = '23'
            GROUP BY ds,
                LPAD(hh, 2, '0'),
                CUBE(group)
        ) AS t4 ON t1.ds = t4.ds
        AND t1.group_segment = t4.group_segment
),
t_intra_team_ops AS (
    SELECT '02_team_ops' AS data_lable,
        t1.ds AS ds,
        t1.hh AS hh,
        '00_all' AS group_segment,
        t1.team_ops AS team_ops,
        '00_all' AS plm_l30d_lv1,
        '00_all' AS plm_l30d_lv2,
        '00_all' AS segment,
        '00_all' AS member_id,
        '00_all' AS member_name,
        t1.engaged_cnt_pc_session AS engaged_cnt_pc_session,
        t1.active_cnt_lt_1d_p AS active_cnt_lt_1d_p,
        t1.app_visit_cnt_ft_1d_np AS app_visit_cnt_ft_1d_np,
        t1.app_dau_cnt_ft_1d_np AS app_dau_cnt_ft_1d_np,
        t1.a2c_lt_1d_p AS a2c_lt_1d_p,
        t1.a2c_uv_cnt_lt_1d_p AS a2c_uv_cnt_lt_1d_p,
        t1.buyer_cnt_lt_1d_p AS buyer_cnt_lt_1d_p,
        t1.new_buyer_cnt_lt_1d_p AS new_buyer_cnt_lt_1d_p,
        t1.reacquired_buyer_cnt_lt_1d_p AS reacquired_buyer_cnt_lt_1d_p,
        t1.order_cnt_lt_1d_p AS order_cnt_lt_1d_p,
        t1.gmv_usd_lt_1d_p AS gmv_usd_lt_1d_p,
        t2.app_visit_cnt_ft_1d_np AS plt_app_visit_cnt_ft_1d_np,
        t2.app_dau_cnt_ft_1d_np AS plt_app_dau_cnt_ft_1d_np,
        t2.a2c_lt_1d_p AS plt_a2c_lt_1d_p,
        t2.a2c_uv_cnt_lt_1d_p AS plt_a2c_uv_cnt_lt_1d_p,
        t2.buyer_cnt_lt_1d_p AS plt_buyer_cnt_lt_1d_p,
        t2.new_buyer_cnt_lt_1d_p AS plt_new_buyer_cnt_lt_1d_p,
        t2.reacquired_buyer_cnt_lt_1d_p AS plt_reacquired_buyer_cnt_lt_1d_p,
        t2.order_cnt_lt_1d_p AS plt_order_cnt_lt_1d_p,
        t2.gmv_usd_lt_1d_p AS plt_gmv_usd_lt_1d_p,
        COALESCE(t3.tr_hh_engaged_cnt_pc_session, 0) AS tr_hh_engaged_cnt_pc_session,
        COALESCE(t3.tr_hh_active_cnt_lt_1d_p, 0) AS tr_hh_active_cnt_lt_1d_p,
        COALESCE(t3.tr_hh_app_dau_cnt_ft_1d_np, 0) AS tr_hh_app_dau_cnt_ft_1d_np,
        COALESCE(t3.tr_hh_buyer_cnt_lt_1d_p, 0) AS tr_hh_buyer_cnt_lt_1d_p,
        COALESCE(t3.tr_hh_order_cnt_lt_1d_p, 0) AS tr_hh_order_cnt_lt_1d_p,
        COALESCE(t3.tr_hh_gmv_usd_lt_1d_p, 0) AS tr_hh_gmv_usd_lt_1d_p,
        COALESCE(t4.tr_eod_engaged_cnt_pc_session, 0) AS tr_eod_engaged_cnt_pc_session,
        COALESCE(t4.tr_eod_active_cnt_lt_1d_p, 0) AS tr_eod_active_cnt_lt_1d_p,
        COALESCE(t4.tr_eod_app_dau_cnt_ft_1d_np, 0) AS tr_eod_app_dau_cnt_ft_1d_np,
        COALESCE(t4.tr_eod_buyer_cnt_lt_1d_p, 0) AS tr_eod_buyer_cnt_lt_1d_p,
        COALESCE(t4.tr_eod_order_cnt_lt_1d_p, 0) AS tr_eod_order_cnt_lt_1d_p,
        COALESCE(t4.tr_eod_gmv_usd_lt_1d_p, 0) AS tr_eod_gmv_usd_lt_1d_p
    FROM (
            SELECT ds,
                hh,
                COALESCE(team_ops, '00_all') AS team_ops,
                SUM(engaged_cnt_pc_session) AS engaged_cnt_pc_session,
                SUM(active_cnt_lt_1d_p) AS active_cnt_lt_1d_p,
                SUM(app_visit_cnt_ft_1d_np) AS app_visit_cnt_ft_1d_np,
                SUM(app_dau_cnt_ft_1d_np) AS app_dau_cnt_ft_1d_np,
                SUM(a2c_lt_1d_p) AS a2c_lt_1d_p,
                SUM(a2c_uv_cnt_lt_1d_p) AS a2c_uv_cnt_lt_1d_p,
                SUM(buyer_cnt_lt_1d_p) AS buyer_cnt_lt_1d_p,
                SUM(new_buyer_cnt_lt_1d_p) AS new_buyer_cnt_lt_1d_p,
                SUM(reacquired_buyer_cnt_lt_1d_p) AS reacquired_buyer_cnt_lt_1d_p,
                SUM(order_cnt_lt_1d_p) AS order_cnt_lt_1d_p,
                SUM(gmv_usd_lt_1d_p) AS gmv_usd_lt_1d_p
            FROM (
                    SELECT ds,
                        hh,
                        team_ops,
                        SUM(SUM(engaged_cnt_pc_session)) OVER (
                            PARTITION BY ds,
                            team_ops
                            ORDER BY hh ASC
                        ) AS engaged_cnt_pc_session,
                        SUM(SUM(active_cnt_lt_1d_p)) OVER (
                            PARTITION BY ds,
                            team_ops
                            ORDER BY hh ASC
                        ) AS active_cnt_lt_1d_p,
                        SUM(app_visit_cnt_ft_1d_np) AS app_visit_cnt_ft_1d_np,
                        SUM(app_dau_cnt_ft_1d_np) AS app_dau_cnt_ft_1d_np,
                        SUM(a2c_lt_1d_p) AS a2c_lt_1d_p,
                        SUM(a2c_uv_cnt_lt_1d_p) AS a2c_uv_cnt_lt_1d_p,
                        SUM(buyer_cnt_lt_1d_p) AS buyer_cnt_lt_1d_p,
                        SUM(new_buyer_cnt_lt_1d_p) AS new_buyer_cnt_lt_1d_p,
                        SUM(reacquired_buyer_cnt_lt_1d_p) AS reacquired_buyer_cnt_lt_1d_p,
                        SUM(order_cnt_lt_1d_p) AS order_cnt_lt_1d_p,
                        SUM(gmv_usd_lt_1d_p) AS gmv_usd_lt_1d_p
                    FROM (
                            SELECT ds,
                                hh,
                                team_ops,
                                COUNT(DISTINCT engaged_member_id_pc_session) AS engaged_cnt_pc_session,
                                COUNT(DISTINCT active_member_id_lt_1d_p) AS active_cnt_lt_1d_p,
                                SUM(app_visit_cnt_ft_1d_np) AS app_visit_cnt_ft_1d_np,
                                SUM(app_dau_cnt_ft_1d_np) AS app_dau_cnt_ft_1d_np,
                                SUM(a2c_lt_1d_p) AS a2c_lt_1d_p,
                                SUM(a2c_uv_cnt_lt_1d_p) AS a2c_uv_cnt_lt_1d_p,
                                SUM(buyer_cnt_lt_1d_p) AS buyer_cnt_lt_1d_p,
                                SUM(new_buyer_cnt_lt_1d_p) AS new_buyer_cnt_lt_1d_p,
                                SUM(reacquired_buyer_cnt_lt_1d_p) AS reacquired_buyer_cnt_lt_1d_p,
                                SUM(order_cnt_lt_1d_p) AS order_cnt_lt_1d_p,
                                SUM(gmv_usd_lt_1d_p) AS gmv_usd_lt_1d_p
                            FROM t_intra_aff
                            GROUP BY ds,
                                hh,
                                team_ops
                        )
                    GROUP BY ds,
                        hh,
                        team_ops
                )
            GROUP BY ds,
                hh,
                CUBE(team_ops)
        ) AS t1
        LEFT JOIN (
            SELECT ds,
                hh,
                app_visit_cnt_ft_1d_np,
                app_dau_cnt_ft_1d_np,
                a2c_lt_1d_p,
                a2c_uv_cnt_lt_1d_p,
                buyer_cnt_lt_1d_p,
                new_buyer_cnt_lt_1d_p,
                reacquired_buyer_cnt_lt_1d_p,
                order_cnt_lt_1d_p,
                gmv_usd_lt_1d_p
            FROM t_intra_plt
        ) AS t2 ON t1.ds = t2.ds
        AND t1.hh = t2.hh
        LEFT JOIN (
            SELECT ds,
                LPAD(hh, 2, '0') AS hh,
                COALESCE(team, '00_all') AS team_ops,
                SUM(engaged) AS tr_hh_engaged_cnt_pc_session,
                SUM(active) AS tr_hh_active_cnt_lt_1d_p,
                SUM(dau) AS tr_hh_app_dau_cnt_ft_1d_np,
                SUM(buyer) AS tr_hh_buyer_cnt_lt_1d_p,
                SUM(order) AS tr_hh_order_cnt_lt_1d_p,
                SUM(gmv) AS tr_hh_gmv_usd_lt_1d_p
            FROM lazada_analyst_dev.hg_affiliate_target_plm_segment
            GROUP BY ds,
                LPAD(hh, 2, '0'),
                CUBE(team)
        ) AS t3 ON t1.ds = t3.ds
        AND t1.hh = t3.hh
        AND t1.team_ops = t3.team_ops
        LEFT JOIN (
            SELECT ds,
                LPAD(hh, 2, '0') AS hh,
                COALESCE(team, '00_all') AS team_ops,
                SUM(engaged) AS tr_eod_engaged_cnt_pc_session,
                SUM(active) AS tr_eod_active_cnt_lt_1d_p,
                SUM(dau) AS tr_eod_app_dau_cnt_ft_1d_np,
                SUM(buyer) AS tr_eod_buyer_cnt_lt_1d_p,
                SUM(order) AS tr_eod_order_cnt_lt_1d_p,
                SUM(gmv) AS tr_eod_gmv_usd_lt_1d_p
            FROM lazada_analyst_dev.hg_affiliate_target_plm_segment
            WHERE 1 = 1
                AND hh = '23'
            GROUP BY ds,
                LPAD(hh, 2, '0'),
                CUBE(team)
        ) AS t4 ON t1.ds = t4.ds
        AND t1.team_ops = t4.team_ops
),
t_intra_plm_l30d_lv1 AS (
    SELECT '03_plm_l30d_lv1' AS data_lable,
        t1.ds AS ds,
        t1.hh AS hh,
        '00_all' AS group_segment,
        '00_all' AS team_ops,
        t1.plm_l30d_lv1 AS plm_l30d_lv1,
        '00_all' AS plm_l30d_lv2,
        '00_all' AS segment,
        '00_all' AS member_id,
        '00_all' AS member_name,
        t1.engaged_cnt_pc_session AS engaged_cnt_pc_session,
        t1.active_cnt_lt_1d_p AS active_cnt_lt_1d_p,
        t1.app_visit_cnt_ft_1d_np AS app_visit_cnt_ft_1d_np,
        t1.app_dau_cnt_ft_1d_np AS app_dau_cnt_ft_1d_np,
        t1.a2c_lt_1d_p AS a2c_lt_1d_p,
        t1.a2c_uv_cnt_lt_1d_p AS a2c_uv_cnt_lt_1d_p,
        t1.buyer_cnt_lt_1d_p AS buyer_cnt_lt_1d_p,
        t1.new_buyer_cnt_lt_1d_p AS new_buyer_cnt_lt_1d_p,
        t1.reacquired_buyer_cnt_lt_1d_p AS reacquired_buyer_cnt_lt_1d_p,
        t1.order_cnt_lt_1d_p AS order_cnt_lt_1d_p,
        t1.gmv_usd_lt_1d_p AS gmv_usd_lt_1d_p,
        t2.app_visit_cnt_ft_1d_np AS plt_app_visit_cnt_ft_1d_np,
        t2.app_dau_cnt_ft_1d_np AS plt_app_dau_cnt_ft_1d_np,
        t2.a2c_lt_1d_p AS plt_a2c_lt_1d_p,
        t2.a2c_uv_cnt_lt_1d_p AS plt_a2c_uv_cnt_lt_1d_p,
        t2.buyer_cnt_lt_1d_p AS plt_buyer_cnt_lt_1d_p,
        t2.new_buyer_cnt_lt_1d_p AS plt_new_buyer_cnt_lt_1d_p,
        t2.reacquired_buyer_cnt_lt_1d_p AS plt_reacquired_buyer_cnt_lt_1d_p,
        t2.order_cnt_lt_1d_p AS plt_order_cnt_lt_1d_p,
        t2.gmv_usd_lt_1d_p AS plt_gmv_usd_lt_1d_p,
        0 AS tr_hh_engaged_cnt_pc_session,
        0 AS tr_hh_active_cnt_lt_1d_p,
        0 AS tr_hh_app_dau_cnt_ft_1d_np,
        0 AS tr_hh_buyer_cnt_lt_1d_p,
        0 AS tr_hh_order_cnt_lt_1d_p,
        0 AS tr_hh_gmv_usd_lt_1d_p,
        0 AS tr_eod_engaged_cnt_pc_session,
        0 AS tr_eod_active_cnt_lt_1d_p,
        0 AS tr_eod_app_dau_cnt_ft_1d_np,
        0 AS tr_eod_buyer_cnt_lt_1d_p,
        0 AS tr_eod_order_cnt_lt_1d_p,
        0 AS tr_eod_gmv_usd_lt_1d_p
    FROM (
            SELECT ds,
                hh,
                COALESCE(plm_l30d_lv1, '00_all') AS plm_l30d_lv1,
                SUM(engaged_cnt_pc_session) AS engaged_cnt_pc_session,
                SUM(active_cnt_lt_1d_p) AS active_cnt_lt_1d_p,
                SUM(app_visit_cnt_ft_1d_np) AS app_visit_cnt_ft_1d_np,
                SUM(app_dau_cnt_ft_1d_np) AS app_dau_cnt_ft_1d_np,
                SUM(a2c_lt_1d_p) AS a2c_lt_1d_p,
                SUM(a2c_uv_cnt_lt_1d_p) AS a2c_uv_cnt_lt_1d_p,
                SUM(buyer_cnt_lt_1d_p) AS buyer_cnt_lt_1d_p,
                SUM(new_buyer_cnt_lt_1d_p) AS new_buyer_cnt_lt_1d_p,
                SUM(reacquired_buyer_cnt_lt_1d_p) AS reacquired_buyer_cnt_lt_1d_p,
                SUM(order_cnt_lt_1d_p) AS order_cnt_lt_1d_p,
                SUM(gmv_usd_lt_1d_p) AS gmv_usd_lt_1d_p
            FROM (
                    SELECT ds,
                        hh,
                        plm_l30d_lv1,
                        SUM(SUM(engaged_cnt_pc_session)) OVER (
                            PARTITION BY ds,
                            plm_l30d_lv1
                            ORDER BY hh ASC
                        ) AS engaged_cnt_pc_session,
                        SUM(SUM(active_cnt_lt_1d_p)) OVER (
                            PARTITION BY ds,
                            plm_l30d_lv1
                            ORDER BY hh ASC
                        ) AS active_cnt_lt_1d_p,
                        SUM(app_visit_cnt_ft_1d_np) AS app_visit_cnt_ft_1d_np,
                        SUM(app_dau_cnt_ft_1d_np) AS app_dau_cnt_ft_1d_np,
                        SUM(a2c_lt_1d_p) AS a2c_lt_1d_p,
                        SUM(a2c_uv_cnt_lt_1d_p) AS a2c_uv_cnt_lt_1d_p,
                        SUM(buyer_cnt_lt_1d_p) AS buyer_cnt_lt_1d_p,
                        SUM(new_buyer_cnt_lt_1d_p) AS new_buyer_cnt_lt_1d_p,
                        SUM(reacquired_buyer_cnt_lt_1d_p) AS reacquired_buyer_cnt_lt_1d_p,
                        SUM(order_cnt_lt_1d_p) AS order_cnt_lt_1d_p,
                        SUM(gmv_usd_lt_1d_p) AS gmv_usd_lt_1d_p
                    FROM (
                            SELECT ds,
                                hh,
                                plm_l30d_lv1,
                                COUNT(DISTINCT engaged_member_id_pc_session) AS engaged_cnt_pc_session,
                                COUNT(DISTINCT active_member_id_lt_1d_p) AS active_cnt_lt_1d_p,
                                SUM(app_visit_cnt_ft_1d_np) AS app_visit_cnt_ft_1d_np,
                                SUM(app_dau_cnt_ft_1d_np) AS app_dau_cnt_ft_1d_np,
                                SUM(a2c_lt_1d_p) AS a2c_lt_1d_p,
                                SUM(a2c_uv_cnt_lt_1d_p) AS a2c_uv_cnt_lt_1d_p,
                                SUM(buyer_cnt_lt_1d_p) AS buyer_cnt_lt_1d_p,
                                SUM(new_buyer_cnt_lt_1d_p) AS new_buyer_cnt_lt_1d_p,
                                SUM(reacquired_buyer_cnt_lt_1d_p) AS reacquired_buyer_cnt_lt_1d_p,
                                SUM(order_cnt_lt_1d_p) AS order_cnt_lt_1d_p,
                                SUM(gmv_usd_lt_1d_p) AS gmv_usd_lt_1d_p
                            FROM t_intra_aff
                            GROUP BY ds,
                                hh,
                                plm_l30d_lv1
                        )
                    GROUP BY ds,
                        hh,
                        plm_l30d_lv1
                )
            GROUP BY ds,
                hh,
                CUBE(plm_l30d_lv1)
        ) AS t1
        LEFT JOIN (
            SELECT ds,
                hh,
                app_visit_cnt_ft_1d_np,
                app_dau_cnt_ft_1d_np,
                a2c_lt_1d_p,
                a2c_uv_cnt_lt_1d_p,
                buyer_cnt_lt_1d_p,
                new_buyer_cnt_lt_1d_p,
                reacquired_buyer_cnt_lt_1d_p,
                order_cnt_lt_1d_p,
                gmv_usd_lt_1d_p
            FROM t_intra_plt
        ) AS t2 ON t1.ds = t2.ds
        AND t1.hh = t2.hh
),
t_intra_plm_l30d_lv2 AS (
    SELECT '04_plm_l30d_lv2' AS data_lable,
        t1.ds AS ds,
        t1.hh AS hh,
        '00_all' AS group_segment,
        '00_all' AS team_ops,
        '00_all' AS plm_l30d_lv1,
        t1.plm_l30d_lv2 AS plm_l30d_lv2,
        '00_all' AS segment,
        '00_all' AS member_id,
        '00_all' AS member_name,
        t1.engaged_cnt_pc_session AS engaged_cnt_pc_session,
        t1.active_cnt_lt_1d_p AS active_cnt_lt_1d_p,
        t1.app_visit_cnt_ft_1d_np AS app_visit_cnt_ft_1d_np,
        t1.app_dau_cnt_ft_1d_np AS app_dau_cnt_ft_1d_np,
        t1.a2c_lt_1d_p AS a2c_lt_1d_p,
        t1.a2c_uv_cnt_lt_1d_p AS a2c_uv_cnt_lt_1d_p,
        t1.buyer_cnt_lt_1d_p AS buyer_cnt_lt_1d_p,
        t1.new_buyer_cnt_lt_1d_p AS new_buyer_cnt_lt_1d_p,
        t1.reacquired_buyer_cnt_lt_1d_p AS reacquired_buyer_cnt_lt_1d_p,
        t1.order_cnt_lt_1d_p AS order_cnt_lt_1d_p,
        t1.gmv_usd_lt_1d_p AS gmv_usd_lt_1d_p,
        t2.app_visit_cnt_ft_1d_np AS plt_app_visit_cnt_ft_1d_np,
        t2.app_dau_cnt_ft_1d_np AS plt_app_dau_cnt_ft_1d_np,
        t2.a2c_lt_1d_p AS plt_a2c_lt_1d_p,
        t2.a2c_uv_cnt_lt_1d_p AS plt_a2c_uv_cnt_lt_1d_p,
        t2.buyer_cnt_lt_1d_p AS plt_buyer_cnt_lt_1d_p,
        t2.new_buyer_cnt_lt_1d_p AS plt_new_buyer_cnt_lt_1d_p,
        t2.reacquired_buyer_cnt_lt_1d_p AS plt_reacquired_buyer_cnt_lt_1d_p,
        t2.order_cnt_lt_1d_p AS plt_order_cnt_lt_1d_p,
        t2.gmv_usd_lt_1d_p AS plt_gmv_usd_lt_1d_p,
        0 AS tr_hh_engaged_cnt_pc_session,
        0 AS tr_hh_active_cnt_lt_1d_p,
        0 AS tr_hh_app_dau_cnt_ft_1d_np,
        0 AS tr_hh_buyer_cnt_lt_1d_p,
        0 AS tr_hh_order_cnt_lt_1d_p,
        0 AS tr_hh_gmv_usd_lt_1d_p,
        0 AS tr_eod_engaged_cnt_pc_session,
        0 AS tr_eod_active_cnt_lt_1d_p,
        0 AS tr_eod_app_dau_cnt_ft_1d_np,
        0 AS tr_eod_buyer_cnt_lt_1d_p,
        0 AS tr_eod_order_cnt_lt_1d_p,
        0 AS tr_eod_gmv_usd_lt_1d_p
    FROM (
            SELECT ds,
                hh,
                COALESCE(plm_l30d_lv2, '00_all') AS plm_l30d_lv2,
                SUM(engaged_cnt_pc_session) AS engaged_cnt_pc_session,
                SUM(active_cnt_lt_1d_p) AS active_cnt_lt_1d_p,
                SUM(app_visit_cnt_ft_1d_np) AS app_visit_cnt_ft_1d_np,
                SUM(app_dau_cnt_ft_1d_np) AS app_dau_cnt_ft_1d_np,
                SUM(a2c_lt_1d_p) AS a2c_lt_1d_p,
                SUM(a2c_uv_cnt_lt_1d_p) AS a2c_uv_cnt_lt_1d_p,
                SUM(buyer_cnt_lt_1d_p) AS buyer_cnt_lt_1d_p,
                SUM(new_buyer_cnt_lt_1d_p) AS new_buyer_cnt_lt_1d_p,
                SUM(reacquired_buyer_cnt_lt_1d_p) AS reacquired_buyer_cnt_lt_1d_p,
                SUM(order_cnt_lt_1d_p) AS order_cnt_lt_1d_p,
                SUM(gmv_usd_lt_1d_p) AS gmv_usd_lt_1d_p
            FROM (
                    SELECT ds,
                        hh,
                        plm_l30d_lv2,
                        SUM(SUM(engaged_cnt_pc_session)) OVER (
                            PARTITION BY ds,
                            plm_l30d_lv2
                            ORDER BY hh ASC
                        ) AS engaged_cnt_pc_session,
                        SUM(SUM(active_cnt_lt_1d_p)) OVER (
                            PARTITION BY ds,
                            plm_l30d_lv2
                            ORDER BY hh ASC
                        ) AS active_cnt_lt_1d_p,
                        SUM(app_visit_cnt_ft_1d_np) AS app_visit_cnt_ft_1d_np,
                        SUM(app_dau_cnt_ft_1d_np) AS app_dau_cnt_ft_1d_np,
                        SUM(a2c_lt_1d_p) AS a2c_lt_1d_p,
                        SUM(a2c_uv_cnt_lt_1d_p) AS a2c_uv_cnt_lt_1d_p,
                        SUM(buyer_cnt_lt_1d_p) AS buyer_cnt_lt_1d_p,
                        SUM(new_buyer_cnt_lt_1d_p) AS new_buyer_cnt_lt_1d_p,
                        SUM(reacquired_buyer_cnt_lt_1d_p) AS reacquired_buyer_cnt_lt_1d_p,
                        SUM(order_cnt_lt_1d_p) AS order_cnt_lt_1d_p,
                        SUM(gmv_usd_lt_1d_p) AS gmv_usd_lt_1d_p
                    FROM (
                            SELECT ds,
                                hh,
                                plm_l30d_lv2,
                                COUNT(DISTINCT engaged_member_id_pc_session) AS engaged_cnt_pc_session,
                                COUNT(DISTINCT active_member_id_lt_1d_p) AS active_cnt_lt_1d_p,
                                SUM(app_visit_cnt_ft_1d_np) AS app_visit_cnt_ft_1d_np,
                                SUM(app_dau_cnt_ft_1d_np) AS app_dau_cnt_ft_1d_np,
                                SUM(a2c_lt_1d_p) AS a2c_lt_1d_p,
                                SUM(a2c_uv_cnt_lt_1d_p) AS a2c_uv_cnt_lt_1d_p,
                                SUM(buyer_cnt_lt_1d_p) AS buyer_cnt_lt_1d_p,
                                SUM(new_buyer_cnt_lt_1d_p) AS new_buyer_cnt_lt_1d_p,
                                SUM(reacquired_buyer_cnt_lt_1d_p) AS reacquired_buyer_cnt_lt_1d_p,
                                SUM(order_cnt_lt_1d_p) AS order_cnt_lt_1d_p,
                                SUM(gmv_usd_lt_1d_p) AS gmv_usd_lt_1d_p
                            FROM t_intra_aff
                            GROUP BY ds,
                                hh,
                                plm_l30d_lv2
                        )
                    GROUP BY ds,
                        hh,
                        plm_l30d_lv2
                )
            GROUP BY ds,
                hh,
                CUBE(plm_l30d_lv2)
        ) AS t1
        LEFT JOIN (
            SELECT ds,
                hh,
                app_visit_cnt_ft_1d_np,
                app_dau_cnt_ft_1d_np,
                a2c_lt_1d_p,
                a2c_uv_cnt_lt_1d_p,
                buyer_cnt_lt_1d_p,
                new_buyer_cnt_lt_1d_p,
                reacquired_buyer_cnt_lt_1d_p,
                order_cnt_lt_1d_p,
                gmv_usd_lt_1d_p
            FROM t_intra_plt
        ) AS t2 ON t1.ds = t2.ds
        AND t1.hh = t2.hh
),
t_intra_segment AS (
    SELECT '05_segment' AS data_lable,
        t1.ds AS ds,
        t1.hh AS hh,
        '00_all' AS group_segment,
        '00_all' AS team_ops,
        '00_all' AS plm_l30d_lv1,
        '00_all' AS plm_l30d_lv2,
        t1.segment AS segment,
        '00_all' AS member_id,
        '00_all' AS member_name,
        t1.engaged_cnt_pc_session AS engaged_cnt_pc_session,
        t1.active_cnt_lt_1d_p AS active_cnt_lt_1d_p,
        t1.app_visit_cnt_ft_1d_np AS app_visit_cnt_ft_1d_np,
        t1.app_dau_cnt_ft_1d_np AS app_dau_cnt_ft_1d_np,
        t1.a2c_lt_1d_p AS a2c_lt_1d_p,
        t1.a2c_uv_cnt_lt_1d_p AS a2c_uv_cnt_lt_1d_p,
        t1.buyer_cnt_lt_1d_p AS buyer_cnt_lt_1d_p,
        t1.new_buyer_cnt_lt_1d_p AS new_buyer_cnt_lt_1d_p,
        t1.reacquired_buyer_cnt_lt_1d_p AS reacquired_buyer_cnt_lt_1d_p,
        t1.order_cnt_lt_1d_p AS order_cnt_lt_1d_p,
        t1.gmv_usd_lt_1d_p AS gmv_usd_lt_1d_p,
        t2.app_visit_cnt_ft_1d_np AS plt_app_visit_cnt_ft_1d_np,
        t2.app_dau_cnt_ft_1d_np AS plt_app_dau_cnt_ft_1d_np,
        t2.a2c_lt_1d_p AS plt_a2c_lt_1d_p,
        t2.a2c_uv_cnt_lt_1d_p AS plt_a2c_uv_cnt_lt_1d_p,
        t2.buyer_cnt_lt_1d_p AS plt_buyer_cnt_lt_1d_p,
        t2.new_buyer_cnt_lt_1d_p AS plt_new_buyer_cnt_lt_1d_p,
        t2.reacquired_buyer_cnt_lt_1d_p AS plt_reacquired_buyer_cnt_lt_1d_p,
        t2.order_cnt_lt_1d_p AS plt_order_cnt_lt_1d_p,
        t2.gmv_usd_lt_1d_p AS plt_gmv_usd_lt_1d_p,
        COALESCE(t3.tr_hh_engaged_cnt_pc_session, 0) AS tr_hh_engaged_cnt_pc_session,
        COALESCE(t3.tr_hh_active_cnt_lt_1d_p, 0) AS tr_hh_active_cnt_lt_1d_p,
        COALESCE(t3.tr_hh_app_dau_cnt_ft_1d_np, 0) AS tr_hh_app_dau_cnt_ft_1d_np,
        COALESCE(t3.tr_hh_buyer_cnt_lt_1d_p, 0) AS tr_hh_buyer_cnt_lt_1d_p,
        COALESCE(t3.tr_hh_order_cnt_lt_1d_p, 0) AS tr_hh_order_cnt_lt_1d_p,
        COALESCE(t3.tr_hh_gmv_usd_lt_1d_p, 0) AS tr_hh_gmv_usd_lt_1d_p,
        COALESCE(t4.tr_eod_engaged_cnt_pc_session, 0) AS tr_eod_engaged_cnt_pc_session,
        COALESCE(t4.tr_eod_active_cnt_lt_1d_p, 0) AS tr_eod_active_cnt_lt_1d_p,
        COALESCE(t4.tr_eod_app_dau_cnt_ft_1d_np, 0) AS tr_eod_app_dau_cnt_ft_1d_np,
        COALESCE(t4.tr_eod_buyer_cnt_lt_1d_p, 0) AS tr_eod_buyer_cnt_lt_1d_p,
        COALESCE(t4.tr_eod_order_cnt_lt_1d_p, 0) AS tr_eod_order_cnt_lt_1d_p,
        COALESCE(t4.tr_eod_gmv_usd_lt_1d_p, 0) AS tr_eod_gmv_usd_lt_1d_p
    FROM (
            SELECT ds,
                hh,
                COALESCE(segment, '00_all') AS segment,
                SUM(engaged_cnt_pc_session) AS engaged_cnt_pc_session,
                SUM(active_cnt_lt_1d_p) AS active_cnt_lt_1d_p,
                SUM(app_visit_cnt_ft_1d_np) AS app_visit_cnt_ft_1d_np,
                SUM(app_dau_cnt_ft_1d_np) AS app_dau_cnt_ft_1d_np,
                SUM(a2c_lt_1d_p) AS a2c_lt_1d_p,
                SUM(a2c_uv_cnt_lt_1d_p) AS a2c_uv_cnt_lt_1d_p,
                SUM(buyer_cnt_lt_1d_p) AS buyer_cnt_lt_1d_p,
                SUM(new_buyer_cnt_lt_1d_p) AS new_buyer_cnt_lt_1d_p,
                SUM(reacquired_buyer_cnt_lt_1d_p) AS reacquired_buyer_cnt_lt_1d_p,
                SUM(order_cnt_lt_1d_p) AS order_cnt_lt_1d_p,
                SUM(gmv_usd_lt_1d_p) AS gmv_usd_lt_1d_p
            FROM (
                    SELECT ds,
                        hh,
                        segment,
                        SUM(SUM(engaged_cnt_pc_session)) OVER (
                            PARTITION BY ds,
                            segment
                            ORDER BY hh ASC
                        ) AS engaged_cnt_pc_session,
                        SUM(SUM(active_cnt_lt_1d_p)) OVER (
                            PARTITION BY ds,
                            segment
                            ORDER BY hh ASC
                        ) AS active_cnt_lt_1d_p,
                        SUM(app_visit_cnt_ft_1d_np) AS app_visit_cnt_ft_1d_np,
                        SUM(app_dau_cnt_ft_1d_np) AS app_dau_cnt_ft_1d_np,
                        SUM(a2c_lt_1d_p) AS a2c_lt_1d_p,
                        SUM(a2c_uv_cnt_lt_1d_p) AS a2c_uv_cnt_lt_1d_p,
                        SUM(buyer_cnt_lt_1d_p) AS buyer_cnt_lt_1d_p,
                        SUM(new_buyer_cnt_lt_1d_p) AS new_buyer_cnt_lt_1d_p,
                        SUM(reacquired_buyer_cnt_lt_1d_p) AS reacquired_buyer_cnt_lt_1d_p,
                        SUM(order_cnt_lt_1d_p) AS order_cnt_lt_1d_p,
                        SUM(gmv_usd_lt_1d_p) AS gmv_usd_lt_1d_p
                    FROM (
                            SELECT ds,
                                hh,
                                segment,
                                COUNT(DISTINCT engaged_member_id_pc_session) AS engaged_cnt_pc_session,
                                COUNT(DISTINCT active_member_id_lt_1d_p) AS active_cnt_lt_1d_p,
                                SUM(app_visit_cnt_ft_1d_np) AS app_visit_cnt_ft_1d_np,
                                SUM(app_dau_cnt_ft_1d_np) AS app_dau_cnt_ft_1d_np,
                                SUM(a2c_lt_1d_p) AS a2c_lt_1d_p,
                                SUM(a2c_uv_cnt_lt_1d_p) AS a2c_uv_cnt_lt_1d_p,
                                SUM(buyer_cnt_lt_1d_p) AS buyer_cnt_lt_1d_p,
                                SUM(new_buyer_cnt_lt_1d_p) AS new_buyer_cnt_lt_1d_p,
                                SUM(reacquired_buyer_cnt_lt_1d_p) AS reacquired_buyer_cnt_lt_1d_p,
                                SUM(order_cnt_lt_1d_p) AS order_cnt_lt_1d_p,
                                SUM(gmv_usd_lt_1d_p) AS gmv_usd_lt_1d_p
                            FROM t_intra_aff
                            GROUP BY ds,
                                hh,
                                segment
                        )
                    GROUP BY ds,
                        hh,
                        segment
                )
            GROUP BY ds,
                hh,
                CUBE(segment)
        ) AS t1
        LEFT JOIN (
            SELECT ds,
                hh,
                app_visit_cnt_ft_1d_np,
                app_dau_cnt_ft_1d_np,
                a2c_lt_1d_p,
                a2c_uv_cnt_lt_1d_p,
                buyer_cnt_lt_1d_p,
                new_buyer_cnt_lt_1d_p,
                reacquired_buyer_cnt_lt_1d_p,
                order_cnt_lt_1d_p,
                gmv_usd_lt_1d_p
            FROM t_intra_plt
        ) AS t2 ON t1.ds = t2.ds
        AND t1.hh = t2.hh
        LEFT JOIN (
            SELECT ds,
                LPAD(hh, 2, '0') AS hh,
                COALESCE(segment, '00_all') AS segment,
                SUM(engaged) AS tr_hh_engaged_cnt_pc_session,
                SUM(active) AS tr_hh_active_cnt_lt_1d_p,
                SUM(dau) AS tr_hh_app_dau_cnt_ft_1d_np,
                SUM(buyer) AS tr_hh_buyer_cnt_lt_1d_p,
                SUM(order) AS tr_hh_order_cnt_lt_1d_p,
                SUM(gmv) AS tr_hh_gmv_usd_lt_1d_p
            FROM lazada_analyst_dev.hg_affiliate_target_plm_segment
            GROUP BY ds,
                LPAD(hh, 2, '0'),
                CUBE(segment)
        ) AS t3 ON t1.ds = t3.ds
        AND t1.hh = t3.hh
        AND t1.segment = t3.segment
        LEFT JOIN (
            SELECT ds,
                LPAD(hh, 2, '0') AS hh,
                COALESCE(segment, '00_all') AS segment,
                SUM(engaged) AS tr_eod_engaged_cnt_pc_session,
                SUM(active) AS tr_eod_active_cnt_lt_1d_p,
                SUM(dau) AS tr_eod_app_dau_cnt_ft_1d_np,
                SUM(buyer) AS tr_eod_buyer_cnt_lt_1d_p,
                SUM(order) AS tr_eod_order_cnt_lt_1d_p,
                SUM(gmv) AS tr_eod_gmv_usd_lt_1d_p
            FROM lazada_analyst_dev.hg_affiliate_target_plm_segment
            WHERE 1 = 1
                AND hh = '23'
            GROUP BY ds,
                LPAD(hh, 2, '0'),
                CUBE(segment)
        ) AS t4 ON t1.ds = t4.ds
        AND t1.segment = t4.segment
),
t_intra_member AS (
    SELECT '06_member' AS data_lable,
        t1.ds AS ds,
        t1.hh AS hh,
        t1.group_segment AS group_segment,
        t1.team_ops AS team_ops,
        t1.plm_l30d_lv1 AS plm_l30d_lv1,
        t1.plm_l30d_lv2 AS plm_l30d_lv2,
        t1.segment AS segment,
        t1.member_id AS member_id,
        t1.member_name AS member_name,
        0 AS engaged_cnt_pc_session,
        0 AS active_cnt_lt_1d_p,
        t1.app_visit_cnt_ft_1d_np AS app_visit_cnt_ft_1d_np,
        t1.app_dau_cnt_ft_1d_np AS app_dau_cnt_ft_1d_np,
        t1.a2c_lt_1d_p AS a2c_lt_1d_p,
        t1.a2c_uv_cnt_lt_1d_p AS a2c_uv_cnt_lt_1d_p,
        t1.buyer_cnt_lt_1d_p AS buyer_cnt_lt_1d_p,
        t1.new_buyer_cnt_lt_1d_p AS new_buyer_cnt_lt_1d_p,
        t1.reacquired_buyer_cnt_lt_1d_p AS reacquired_buyer_cnt_lt_1d_p,
        t1.order_cnt_lt_1d_p AS order_cnt_lt_1d_p,
        t1.gmv_usd_lt_1d_p AS gmv_usd_lt_1d_p,
        t2.app_visit_cnt_ft_1d_np AS plt_app_visit_cnt_ft_1d_np,
        t2.app_dau_cnt_ft_1d_np AS plt_app_dau_cnt_ft_1d_np,
        t2.a2c_lt_1d_p AS plt_a2c_lt_1d_p,
        t2.a2c_uv_cnt_lt_1d_p AS plt_a2c_uv_cnt_lt_1d_p,
        t2.buyer_cnt_lt_1d_p AS plt_buyer_cnt_lt_1d_p,
        t2.new_buyer_cnt_lt_1d_p AS plt_new_buyer_cnt_lt_1d_p,
        t2.reacquired_buyer_cnt_lt_1d_p AS plt_reacquired_buyer_cnt_lt_1d_p,
        t2.order_cnt_lt_1d_p AS plt_order_cnt_lt_1d_p,
        t2.gmv_usd_lt_1d_p AS plt_gmv_usd_lt_1d_p,
        0 AS tr_hh_engaged_cnt_pc_session,
        0 AS tr_hh_active_cnt_lt_1d_p,
        0 AS tr_hh_app_dau_cnt_ft_1d_np,
        0 AS tr_hh_buyer_cnt_lt_1d_p,
        0 AS tr_hh_order_cnt_lt_1d_p,
        0 AS tr_hh_gmv_usd_lt_1d_p,
        0 AS tr_eod_engaged_cnt_pc_session,
        0 AS tr_eod_active_cnt_lt_1d_p,
        0 AS tr_eod_app_dau_cnt_ft_1d_np,
        0 AS tr_eod_buyer_cnt_lt_1d_p,
        0 AS tr_eod_order_cnt_lt_1d_p,
        0 AS tr_eod_gmv_usd_lt_1d_p
    FROM (
            SELECT ds,
                hh,
                group_segment,
                team_ops,
                plm_l30d_lv1,
                plm_l30d_lv2,
                segment,
                member_id,
                member_name,
                app_visit_cnt_ft_1d_np,
                app_dau_cnt_ft_1d_np,
                a2c_lt_1d_p,
                a2c_uv_cnt_lt_1d_p,
                buyer_cnt_lt_1d_p,
                new_buyer_cnt_lt_1d_p,
                reacquired_buyer_cnt_lt_1d_p,
                order_cnt_lt_1d_p,
                gmv_usd_lt_1d_p
            FROM t_intra_aff
        ) AS t1
        LEFT JOIN (
            SELECT ds,
                hh,
                app_visit_cnt_ft_1d_np,
                app_dau_cnt_ft_1d_np,
                a2c_lt_1d_p,
                a2c_uv_cnt_lt_1d_p,
                buyer_cnt_lt_1d_p,
                new_buyer_cnt_lt_1d_p,
                reacquired_buyer_cnt_lt_1d_p,
                order_cnt_lt_1d_p,
                gmv_usd_lt_1d_p
            FROM t_intra_plt
        ) AS t2 ON t1.ds = t2.ds
        AND t1.hh = t2.hh
)
SELECT data_lable,
    TO_CHAR(TO_DATE(ds, 'yyyymmdd'), 'yyyy-mm-dd') AS ds_1,
    ds AS ds_2,
    hh,
    group_segment,
    team_ops,
    plm_l30d_lv1,
    plm_l30d_lv2,
    segment,
    member_id,
    member_name,
    engaged_fixed,
    engaged_original,
    active,
    app_visit,
    app_dau,
    a2c,
    a2c_uv,
    buyer,
    new_buyer,
    reacq_buyer,
    order,
    gmv_usd,
    IF(app_dau = 0, 0, buyer / app_dau) AS purchase_rate,
    IF(app_dau = 0, 0, a2c_uv / app_dau) AS a2c_rate,
    IF(order = 0, 0, gmv_usd / order) AS aov,
    IF(buyer = 0, 0, order / buyer) AS order_per_buyer,
    IF(buyer = 0, 0, gmv_usd / buyer) AS arpu,
    plt_app_visit,
    plt_app_dau,
    plt_a2c,
    plt_a2c_uv,
    plt_buyer,
    plt_new_buyer,
    plt_reacq_buyer,
    plt_order,
    plt_gmv_usd,
    IF(plt_app_dau = 0, 0, plt_buyer / plt_app_dau) AS plt_purchase_rate,
    IF(plt_app_dau = 0, 0, plt_a2c_uv / plt_app_dau) AS plt_a2c_rate,
    IF(plt_order = 0, 0, plt_gmv_usd / plt_order) AS plt_aov,
    IF(plt_buyer = 0, 0, plt_order / plt_buyer) AS plt_order_per_buyer,
    IF(plt_buyer = 0, 0, plt_gmv_usd / plt_buyer) AS plt_arpu,
    tr_hh_engaged,
    tr_hh_active,
    tr_hh_app_dau,
    tr_hh_buyer,
    tr_hh_order,
    tr_hh_gmv_usd,
    tr_eod_engaged,
    tr_eod_active,
    tr_eod_app_dau,
    tr_eod_buyer,
    tr_eod_order,
    tr_eod_gmv_usd,
    IF(plt_app_visit = 0, 0, app_visit / plt_app_visit) AS contr_app_visit,
    IF(plt_app_dau = 0, 0, app_dau / plt_app_dau) AS contr_app_dau,
    IF(plt_a2c = 0, 0, a2c / plt_a2c) AS contr_a2c,
    IF(plt_a2c_uv = 0, 0, a2c_uv / plt_a2c_uv) AS contr_a2c_uv,
    IF(plt_buyer = 0, 0, buyer / plt_buyer) AS contr_buyer,
    IF(plt_new_buyer = 0, 0, new_buyer / plt_new_buyer) AS contr_new_buyer,
    IF(
        plt_reacq_buyer = 0,
        0,
        reacq_buyer / plt_reacq_buyer
    ) AS contr_reacq_buyer,
    IF(plt_order = 0, 0, order / plt_order) AS contr_order,
    IF(plt_gmv_usd = 0, 0, gmv_usd / plt_gmv_usd) AS contr_gmv_usd,
    IF(
        tr_hh_engaged = 0,
        0,
        engaged_fixed / tr_hh_engaged - 1
    ) AS vs_per_engaged,
    IF(tr_hh_active = 0, 0, active / tr_hh_active - 1) AS vs_per_active,
    IF(tr_hh_app_dau = 0, 0, app_dau / tr_hh_app_dau - 1) AS vs_per_app_dau,
    IF(tr_hh_buyer = 0, 0, buyer / tr_hh_buyer - 1) AS vs_per_buyer,
    IF(tr_hh_order = 0, 0, order / tr_hh_order - 1) AS vs_per_order,
    IF(tr_hh_gmv_usd = 0, 0, gmv_usd / tr_hh_gmv_usd - 1) AS vs_per_gmv_usd,
    IF(
        tr_hh_engaged = 0,
        0,
        engaged_fixed - tr_hh_engaged
    ) AS vs_abs_engaged,
    IF(tr_hh_active = 0, 0, active - tr_hh_active) AS vs_abs_active,
    IF(tr_hh_app_dau = 0, 0, app_dau - tr_hh_app_dau) AS vs_abs_app_dau,
    IF(tr_hh_buyer = 0, 0, buyer - tr_hh_buyer) AS vs_abs_buyer,
    IF(tr_hh_order = 0, 0, order - tr_hh_order) AS vs_abs_order,
    IF(tr_hh_gmv_usd = 0, 0, gmv_usd - tr_hh_gmv_usd) AS vs_abs_gmv_usd,
    IF(
        tr_hh_engaged = 0,
        0,
        engaged_fixed / tr_hh_engaged * tr_eod_engaged
    ) AS eod_engaged,
    IF(
        tr_hh_active = 0,
        0,
        active / tr_hh_active * tr_eod_active
    ) AS eod_active,
    IF(
        tr_hh_app_dau = 0,
        0,
        app_dau / tr_hh_app_dau * tr_eod_app_dau
    ) AS eod_app_dau,
    IF(
        tr_hh_buyer = 0,
        0,
        buyer / tr_hh_buyer * tr_eod_buyer
    ) AS eod_buyer,
    IF(
        tr_hh_order = 0,
        0,
        order / tr_hh_order * tr_eod_order
    ) AS eod_order,
    IF(
        tr_hh_gmv_usd = 0,
        0,
        gmv_usd / tr_hh_gmv_usd * tr_eod_gmv_usd
    ) AS eod_gmv_usd
FROM (
        SELECT data_lable,
            ds,
            hh,
            group_segment,
            team_ops,
            plm_l30d_lv1,
            plm_l30d_lv2,
            segment,
            member_id,
            member_name,
CASE
                WHEN ds > TO_CHAR(DATEADD(GETDATE(), -3, 'dd'), 'yyyymmdd') THEN ROUND(engaged_cnt_pc_session * 1.7, 0)
                ELSE engaged_cnt_pc_session
            END AS engaged_fixed,
            engaged_cnt_pc_session AS engaged_original,
            active_cnt_lt_1d_p AS active,
            app_visit_cnt_ft_1d_np AS app_visit,
            app_dau_cnt_ft_1d_np AS app_dau,
            a2c_lt_1d_p AS a2c,
            a2c_uv_cnt_lt_1d_p AS a2c_uv,
            buyer_cnt_lt_1d_p AS buyer,
            new_buyer_cnt_lt_1d_p AS new_buyer,
            reacquired_buyer_cnt_lt_1d_p AS reacq_buyer,
            order_cnt_lt_1d_p AS order,
            gmv_usd_lt_1d_p AS gmv_usd,
            plt_app_visit_cnt_ft_1d_np AS plt_app_visit,
            plt_app_dau_cnt_ft_1d_np AS plt_app_dau,
            plt_a2c_lt_1d_p AS plt_a2c,
            plt_a2c_uv_cnt_lt_1d_p AS plt_a2c_uv,
            plt_buyer_cnt_lt_1d_p AS plt_buyer,
            plt_new_buyer_cnt_lt_1d_p AS plt_new_buyer,
            plt_reacquired_buyer_cnt_lt_1d_p AS plt_reacq_buyer,
            plt_order_cnt_lt_1d_p AS plt_order,
            plt_gmv_usd_lt_1d_p AS plt_gmv_usd,
            tr_hh_engaged_cnt_pc_session AS tr_hh_engaged,
            tr_hh_active_cnt_lt_1d_p AS tr_hh_active,
            tr_hh_app_dau_cnt_ft_1d_np AS tr_hh_app_dau,
            tr_hh_buyer_cnt_lt_1d_p AS tr_hh_buyer,
            tr_hh_order_cnt_lt_1d_p AS tr_hh_order,
            tr_hh_gmv_usd_lt_1d_p AS tr_hh_gmv_usd,
            tr_eod_engaged_cnt_pc_session AS tr_eod_engaged,
            tr_eod_active_cnt_lt_1d_p AS tr_eod_active,
            tr_eod_app_dau_cnt_ft_1d_np AS tr_eod_app_dau,
            tr_eod_buyer_cnt_lt_1d_p AS tr_eod_buyer,
            tr_eod_order_cnt_lt_1d_p AS tr_eod_order,
            tr_eod_gmv_usd_lt_1d_p AS tr_eod_gmv_usd
        FROM (
                SELECT *
                FROM t_intra_group_segment
                UNION ALL
                SELECT *
                FROM t_intra_team_ops
                UNION ALL
                SELECT *
                FROM t_intra_plm_l30d_lv1
                UNION ALL
                SELECT *
                FROM t_intra_plm_l30d_lv2
                UNION ALL
                SELECT *
                FROM t_intra_segment
                UNION ALL
                SELECT *
                FROM t_intra_member
            )
    )
ORDER BY ds,
    hh,
    data_lable,
    group_segment,
    team_ops,
    plm_l30d_lv1,
    plm_l30d_lv2,
    segment,
    member_id,
    member_name;
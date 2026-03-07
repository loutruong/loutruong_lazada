--odps sql 
--********************************************************************--
--author:Truong, Van Thanh
--create time:2022-11-17 15:19:08
--********************************************************************--
--@@ Input = lazada_ads.ads_lzd_mkt_steering_dau_overview_1d
--@@ Input = lazada_ads.ads_lzd_mkt_steering_channel_overview_perf_1d
--@@ Input = lazada_ads.ads_lzd_mkt_mau_contribution_1d
--@@ Input = lazada_ads.ads_lzd_mkt_mab_contribution_1d
SELECT CONCAT(
        SUBSTR(ds, 1, 4),
        '-',
        SUBSTR(ds, 5, 2),
        '-',
        SUBSTR(ds, 7, 2)
    ) AS ds,
    venture,
    COALESCE(SUM(total_dau), 0) AS total_dau,
    COALESCE(SUM(organic_dau), 0) AS organic_dau,
    COALESCE(SUM(mkt_dau), 0) AS mkt_dau,
    COALESCE(SUM(other_mkt_dau), 0) AS other_mkt_dau,
    COALESCE(SUM(crm_dau), 0) AS crm_dau,
    COALESCE(SUM(om_dau), 0) AS om_dau,
    COALESCE(SUM(ppc_dau), 0) AS ppc_dau,
    COALESCE(SUM(affiliate_dau), 0) AS affiliate_dau,
    COALESCE(SUM(rta_dau), 0) AS rta_dau,
    COALESCE(SUM(total_buyer), 0) AS total_buyer,
    COALESCE(SUM(organic_buyer), 0) AS organic_buyer,
    COALESCE(SUM(mkt_buyer), 0) AS mkt_buyer,
    COALESCE(SUM(other_mkt_buyer), 0) AS other_mkt_buyer,
    COALESCE(SUM(crm_buyer), 0) AS crm_buyer,
    COALESCE(SUM(om_buyer), 0) AS om_buyer,
    COALESCE(SUM(ppc_buyer), 0) AS ppc_buyer,
    COALESCE(SUM(affiliate_buyer), 0) AS affiliate_buyer,
    COALESCE(SUM(rta_buyer), 0) AS rta_buyer
FROM (
        SELECT ds,
            venture,
            total_dau,
            organic_dau,
            mkt_dau,
            other_mkt_dau,
            crm_dau,
            om_dau,
            ppc_dau,
            affiliate_dau,
            rta_dau,
            total_buyer,
            organic_buyer,
            mkt_buyer,
            other_mkt_buyer,
            crm_buyer,
            om_buyer,
            0 AS ppc_buyer,
            0 AS affiliate_buyer,
            0 AS rta_buyer
        FROM lazada_ads.ads_lzd_mkt_steering_dau_overview_1d
        WHERE 1 = 1
        UNION ALL
        SELECT ds,
            venture,
            0 AS total_dau,
            0 AS organic_dau,
            0 AS mkt_dau,
            0 AS other_mkt_dau,
            0 AS crm_dau,
            0 AS om_dau,
            0 AS ppc_dau,
            0 AS affiliate_dau,
            0 AS rta_dau,
            0 AS total_buyer,
            0 AS organic_buyer,
            0 AS mkt_buyer,
            0 AS other_mkt_buyer,
            0 AS crm_buyer,
            0 AS om_buyer,
            SUM(buyers) AS ppc_buyer,
            0 AS affiliate_buyer,
            0 AS rta_buyer
        FROM lazada_ads.ads_lzd_mkt_steering_channel_overview_perf_1d
        WHERE 1 = 1
            AND free_paid = 'ALL'
            AND funding_bucket = 'Lazada OM'
            AND funding_type = 'ALL'
            AND platform = 'ALL'
            AND channel NOT IN ('RTA', 'CPS Affiliate', 'CPI Affiliate')
            AND channel <> 'ALL'
            AND attr_model = 'lt_1d_p'
        GROUP BY ds,
            venture
        UNION ALL
        SELECT ds,
            venture,
            0 AS total_dau,
            0 AS organic_dau,
            0 AS mkt_dau,
            0 AS other_mkt_dau,
            0 AS crm_dau,
            0 AS om_dau,
            0 AS ppc_dau,
            0 AS affiliate_dau,
            0 AS rta_dau,
            0 AS total_buyer,
            0 AS organic_buyer,
            0 AS mkt_buyer,
            0 AS other_mkt_buyer,
            0 AS crm_buyer,
            0 AS om_buyer,
            0 AS ppc_buyer,
            SUM(
                CASE
                    WHEN channel IN ('CPS Affiliate', 'CPI Affiliate')
                    AND attr_model = 'lt_1d_p'
                    AND platform = 'ALL' THEN buyers
                    ELSE 0
                END
            ) AS affiliate_buyer,
            SUM(
                CASE
                    WHEN channel = ('RTA')
                    AND attr_model = 'lt_1d_p'
                    AND platform = 'ALL' THEN buyers
                    ELSE 0
                END
            ) AS rta_buyer
        FROM lazada_ads.ads_lzd_mkt_steering_channel_overview_perf_1d
        WHERE 1 = 1
            AND free_paid = 'ALL'
            AND funding_bucket = 'Lazada OM'
            AND funding_type = 'ALL'
            AND platform IN ('App', 'ALL')
            AND channel <> 'ALL'
            AND attr_model IN ('ft_1d_np', 'lt_1d', 'lt_1d_p')
        GROUP BY ds,
            venture
    )
WHERE 1 = 1
    AND venture IN ('SG', 'MY', 'PH', 'VN', 'ID', 'TH')
    AND ds >= (
        SELECT MIN(ds)
        FROM lazada_ads.ads_lzd_mkt_steering_channel_overview_perf_1d
    )
    AND ds <= (
        SELECT MAX(ds)
        FROM lazada_ads.ads_lzd_mkt_steering_channel_overview_perf_1d
    )
GROUP BY ds,
    venture
ORDER BY ds ASC;
SELECT ds,
    venture,
    attr_model,
    sub_channel,
    rt_audience,
    visit_frequency_level,
    purchase_frequency_level,
    metric,
    result
FROM (
        SELECT report_date AS ds,
            venture,
CASE
                WHEN TOLOWER(attr_model) IN ('any touch') THEN 'pc_session'
                ELSE 'ex_1d'
            END AS attr_model,
            sub_channel,
            rt_audience,
            visit_frequency_level,
            '' AS purchase_frequency_level,
            uv AS mau_app,
            0 AS mab
        FROM lazada_ads.ads_lzd_mkt_mau_contribution_1d
        UNION ALL
        SELECT report_date AS ds,
            venture,
            'lt_1d_p' AS attr_model,
            sub_channel,
            rt_audience,
            visit_frequency_level,
            purchase_frequency_level,
            0 AS mau_app,
            buyer AS mab
        FROM lazada_ads.ads_lzd_mkt_mab_contribution_1d
    ) UNPIVOT (result FOR metric IN (mau_app, mab))
WHERE 1 = 1
    AND result <> 0;
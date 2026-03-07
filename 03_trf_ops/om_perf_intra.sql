-- MaxCompute SQL 
-- ********************************************************************--
-- author:vanthanh.truong
-- create time:2023-09-05 12:21:03
-- ********************************************************************--
--@@ Input = lazada_ads.ads_lzd_mkt_cost_sub_channel_campaign_hh
--@@ Input = lazada_ads.ads_lzd_mkt_rt_uam_campaign_hh
SELECT CONCAT(
        SUBSTR(ds, 1, 4),
        '-',
        SUBSTR(ds, 5, 2),
        '-',
        SUBSTR(ds, 7, 2)
    ) AS ds,
    hh,
    channel,
    sub_channel,
    rt_audience,
    campaign,
    COALESCE(SUM(spend), 0) AS spend,
    COALESCE(SUM(app_spend), 0) AS app_spend,
    COALESCE(SUM(impressions), 0) AS impressions,
    COALESCE(SUM(app_impressions), 0) AS app_impressions,
    COALESCE(SUM(clicks), 0) AS clicks,
    COALESCE(SUM(app_clicks), 0) AS app_clicks,
    COALESCE(SUM(installs), 0) AS installs,
    COALESCE(SUM(app_installs), 0) AS app_installs,
    COALESCE(SUM(reinstalls), 0) AS reinstalls,
    COALESCE(SUM(app_reinstalls), 0) AS app_reinstalls,
    COALESCE(SUM(visits), 0) AS visits,
    COALESCE(SUM(app_visits), 0) AS app_visits,
    COALESCE(SUM(dau), 0) AS dau,
    COALESCE(SUM(app_dau), 0) AS app_dau,
    COALESCE(SUM(pdp_pv), 0) AS pdp_pv,
    COALESCE(SUM(app_pdp_pv), 0) AS app_pdp_pv,
    COALESCE(SUM(pdp_uv), 0) AS pdp_uv,
    COALESCE(SUM(app_pdp_uv), 0) AS app_pdp_uv,
    COALESCE(SUM(a2c), 0) AS a2c,
    COALESCE(SUM(app_a2c), 0) AS app_a2c,
    COALESCE(SUM(a2c_uv), 0) AS a2c_uv,
    COALESCE(SUM(app_a2c_uv), 0) AS app_a2c_uv,
    COALESCE(SUM(buyers), 0) AS buyers,
    COALESCE(SUM(app_buyers), 0) AS app_buyers,
    COALESCE(SUM(new_buyers), 0) AS new_buyers,
    COALESCE(SUM(app_new_buyers), 0) AS app_new_buyers,
    COALESCE(SUM(reacquired_buyers), 0) AS reacquired_buyers,
    COALESCE(SUM(app_reacquired_buyers), 0) AS app_reacquired_buyers,
    COALESCE(SUM(sales_order_id_count), 0) AS sales_order_id_count,
    COALESCE(SUM(app_sales_order_id_count), 0) AS app_sales_order_id_count,
    COALESCE(SUM(order_id_count), 0) AS order_id_count --<< Order using
,
    COALESCE(SUM(app_order_id_count), 0) AS app_order_id_count --<< Order using
,
    COALESCE(SUM(gmv), 0) AS gmv,
    COALESCE(SUM(app_gmv), 0) AS app_gmv
FROM (
        SELECT ds,
            hh,
            channel,
            CASE
                WHEN TOLOWER(sub_channel) IN ('arm') THEN 'Social SKU - ARM'
                ELSE sub_channel
            END AS sub_channel,
            rt_audience,
            campaign,
            SUM(spend) AS spend,
            SUM(app_spend) AS app_spend,
            SUM(impressions) AS impressions,
            SUM(app_impressions) AS app_impressions,
            SUM(clicks) AS clicks,
            SUM(app_clicks) AS app_clicks,
            0 AS installs,
            0 AS app_installs,
            0 AS reinstalls,
            0 AS app_reinstalls,
            0 AS visits,
            0 AS app_visits,
            0 AS dau,
            0 AS app_dau,
            0 AS pdp_pv,
            0 AS app_pdp_pv,
            0 AS pdp_uv,
            0 AS app_pdp_uv,
            0 AS a2c,
            0 AS app_a2c,
            0 AS a2c_uv,
            0 AS app_a2c_uv,
            0 AS buyers,
            0 AS app_buyers,
            0 AS new_buyers,
            0 AS app_new_buyers,
            0 AS reacquired_buyers,
            0 AS app_reacquired_buyers,
            0 AS sales_order_id_count,
            0 AS app_sales_order_id_count,
            0 AS order_id_count --<< Order using
,
            0 AS app_order_id_count --<< Order using
,
            0 AS gmv,
            0 AS app_gmv
        FROM (
                --<< OM table Channel
                SELECT ds,
                    hh,
                    channel,
                    sub_channel,
                    COALESCE(rt_audience, 'All') AS rt_audience,
                    COALESCE(campaign, 'All') AS campaign,
                    SUM(
                        CASE
                            WHEN platform IN ('App', 'Pc', 'Wap', 'Web', 'Unknown') THEN spend
                            ELSE 0
                        END
                    ) AS spend,
                    SUM(
                        CASE
                            WHEN platform IN ('App') THEN spend
                            ELSE 0
                        END
                    ) AS app_spend,
                    SUM(
                        CASE
                            WHEN platform IN ('App', 'Pc', 'Wap', 'Web', 'Unknown') THEN external_impressions
                            ELSE 0
                        END
                    ) AS impressions,
                    SUM(
                        CASE
                            WHEN platform IN ('App') THEN external_impressions
                            ELSE 0
                        END
                    ) AS app_impressions,
                    SUM(
                        CASE
                            WHEN platform IN ('App', 'Pc', 'Wap', 'Web', 'Unknown') THEN external_clicks
                            ELSE 0
                        END
                    ) AS clicks,
                    SUM(
                        CASE
                            WHEN platform IN ('App') THEN external_clicks
                            ELSE 0
                        END
                    ) AS app_clicks
                FROM lazada_ads.ads_lzd_mkt_cost_sub_channel_campaign_hh
                WHERE 1 = 1
                    AND funding_bucket IN ('Lazada OM')
                    AND funding_type IN ('OM', 'AMS')
                    AND channel IN (
                        'SEM',
                        'Social',
                        'Apple Search',
                        'Display',
                        'TikTok',
                        'ARM',
                        'RTA'
                    )
                    AND sub_channel NOT IN (
                        'SEM Shopping',
                        'Social - Campaign',
                        'Social - D0 Churn User'
                    )
                    AND venture = 'VN'
                    AND ds BETWEEN TO_CHAR(DATEADD(GETDATE(), -2, 'dd'), 'yyyymmdd') AND TO_CHAR(DATEADD(GETDATE(), 0, 'dd'), 'yyyymmdd')
                GROUP BY ds,
                    hh,
                    channel,
                    sub_channel,
                    rt_audience,
                    campaign GROUPING SETS ((ds, hh, channel, sub_channel))
                UNION ALL
                --<< OM table Channel: SEM Shopping
                SELECT ds,
                    hh,
                    channel,
                    sub_channel,
                    CASE
                        WHEN rt_audience IS NOT NULL THEN 'All'
                        ELSE COALESCE(rt_audience, 'All')
                    END AS rt_audience,
                    COALESCE(campaign, 'All') AS campaign,
                    SUM(
                        CASE
                            WHEN platform IN ('App', 'Pc', 'Wap', 'Web', 'Unknown') THEN spend
                            ELSE 0
                        END
                    ) AS spend,
                    SUM(
                        CASE
                            WHEN platform IN ('App') THEN spend
                            ELSE 0
                        END
                    ) AS app_spend,
                    SUM(
                        CASE
                            WHEN platform IN ('App', 'Pc', 'Wap', 'Web', 'Unknown') THEN external_impressions
                            ELSE 0
                        END
                    ) AS impressions,
                    SUM(
                        CASE
                            WHEN platform IN ('App') THEN external_impressions
                            ELSE 0
                        END
                    ) AS app_impressions,
                    SUM(
                        CASE
                            WHEN platform IN ('App', 'Pc', 'Wap', 'Web', 'Unknown') THEN external_clicks
                            ELSE 0
                        END
                    ) AS clicks,
                    SUM(
                        CASE
                            WHEN platform IN ('App') THEN external_clicks
                            ELSE 0
                        END
                    ) AS app_clicks
                FROM lazada_ads.ads_lzd_mkt_cost_sub_channel_campaign_hh
                WHERE 1 = 1
                    AND funding_bucket IN ('Lazada OM')
                    AND funding_type IN ('OM')
                    AND purpose IN ('Broad')
                    AND channel IN ('SEM')
                    AND sub_channel IN ('SEM Shopping')
                    AND venture = 'VN'
                    AND ds BETWEEN TO_CHAR(DATEADD(GETDATE(), -2, 'dd'), 'yyyymmdd') AND TO_CHAR(DATEADD(GETDATE(), 0, 'dd'), 'yyyymmdd')
                GROUP BY ds,
                    hh,
                    channel,
                    sub_channel,
                    rt_audience,
                    campaign GROUPING SETS (
                        (
                            ds,
                            hh,
                            channel,
                            sub_channel,
                            rt_audience,
                            campaign
                        ),
                        (ds, hh, channel, sub_channel)
                    )
                UNION ALL
                --<< OM table Channel: Social - Campaign
                SELECT ds,
                    hh,
                    channel,
                    sub_channel,
                    CASE
                        WHEN rt_audience IN ('Unknown') THEN 'All'
                        ELSE rt_audience
                    END AS rt_audience,
                    COALESCE(campaign, 'All') AS campaign,
                    SUM(
                        CASE
                            WHEN platform IN ('App', 'Pc', 'Wap', 'Web', 'Unknown') THEN spend
                            ELSE 0
                        END
                    ) AS spend,
                    SUM(
                        CASE
                            WHEN platform IN ('App') THEN spend
                            ELSE 0
                        END
                    ) AS app_spend,
                    SUM(
                        CASE
                            WHEN platform IN ('App', 'Pc', 'Wap', 'Web', 'Unknown') THEN external_impressions
                            ELSE 0
                        END
                    ) AS impressions,
                    SUM(
                        CASE
                            WHEN platform IN ('App') THEN external_impressions
                            ELSE 0
                        END
                    ) AS app_impressions,
                    SUM(
                        CASE
                            WHEN platform IN ('App', 'Pc', 'Wap', 'Web', 'Unknown') THEN external_clicks
                            ELSE 0
                        END
                    ) AS clicks,
                    SUM(
                        CASE
                            WHEN platform IN ('App') THEN external_clicks
                            ELSE 0
                        END
                    ) AS app_clicks
                FROM lazada_ads.ads_lzd_mkt_cost_sub_channel_campaign_hh
                WHERE 1 = 1
                    AND funding_bucket IN ('Lazada OM')
                    AND funding_type IN ('OM')
                    AND purpose IN ('Broad - Campaign')
                    AND channel IN ('Social')
                    AND sub_channel IN ('Social - Campaign')
                    AND campaign NOT LIKE '%SA%'
                    AND venture = 'VN'
                    AND ds BETWEEN TO_CHAR(DATEADD(GETDATE(), -2, 'dd'), 'yyyymmdd') AND TO_CHAR(DATEADD(GETDATE(), 0, 'dd'), 'yyyymmdd')
                GROUP BY ds,
                    hh,
                    channel,
                    sub_channel,
                    rt_audience,
                    campaign GROUPING SETS ((ds, hh, channel, sub_channel, rt_audience))
                UNION ALL
                --<< OM table Channel: Social - Campaign (Campaign level)
                SELECT ds,
                    hh,
                    channel,
                    sub_channel,
                    REPLACE('Unknown', 'Unknown', 'All') AS rt_audience,
                    campaign,
                    SUM(
                        CASE
                            WHEN platform IN ('App', 'Pc', 'Wap', 'Web', 'Unknown') THEN spend
                            ELSE 0
                        END
                    ) AS spend,
                    SUM(
                        CASE
                            WHEN platform IN ('App') THEN spend
                            ELSE 0
                        END
                    ) AS app_spend,
                    SUM(
                        CASE
                            WHEN platform IN ('App', 'Pc', 'Wap', 'Web', 'Unknown') THEN external_impressions
                            ELSE 0
                        END
                    ) AS impressions,
                    SUM(
                        CASE
                            WHEN platform IN ('App') THEN external_impressions
                            ELSE 0
                        END
                    ) AS app_impressions,
                    SUM(
                        CASE
                            WHEN platform IN ('App', 'Pc', 'Wap', 'Web', 'Unknown') THEN external_clicks
                            ELSE 0
                        END
                    ) AS clicks,
                    SUM(
                        CASE
                            WHEN platform IN ('App') THEN external_clicks
                            ELSE 0
                        END
                    ) AS app_clicks
                FROM lazada_ads.ads_lzd_mkt_cost_sub_channel_campaign_hh
                WHERE 1 = 1
                    AND funding_bucket IN ('Lazada OM')
                    AND funding_type IN ('OM')
                    AND purpose IN ('Broad - Campaign')
                    AND channel IN ('Social')
                    AND sub_channel IN ('Social - Campaign')
                    AND campaign NOT LIKE '%SA%'
                    AND campaign LIKE '%VN%'
                    AND venture = 'VN'
                    AND ds BETWEEN TO_CHAR(DATEADD(GETDATE(), -2, 'dd'), 'yyyymmdd') AND TO_CHAR(DATEADD(GETDATE(), 0, 'dd'), 'yyyymmdd')
                GROUP BY ds,
                    hh,
                    channel,
                    sub_channel,
                    rt_audience,
                    campaign
                UNION ALL
                --<< OM table Channel: Social - D0 (Campaign level)
                SELECT ds,
                    hh,
                    channel,
                    sub_channel,
                    rt_audience,
                    COALESCE(campaign, 'All') AS campaign,
                    SUM(
                        CASE
                            WHEN platform IN ('App', 'Pc', 'Wap', 'Web', 'Unknown') THEN spend
                            ELSE 0
                        END
                    ) AS spend,
                    SUM(
                        CASE
                            WHEN platform IN ('App') THEN spend
                            ELSE 0
                        END
                    ) AS app_spend,
                    SUM(
                        CASE
                            WHEN platform IN ('App', 'Pc', 'Wap', 'Web', 'Unknown') THEN external_impressions
                            ELSE 0
                        END
                    ) AS impressions,
                    SUM(
                        CASE
                            WHEN platform IN ('App') THEN external_impressions
                            ELSE 0
                        END
                    ) AS app_impressions,
                    SUM(
                        CASE
                            WHEN platform IN ('App', 'Pc', 'Wap', 'Web', 'Unknown') THEN external_clicks
                            ELSE 0
                        END
                    ) AS clicks,
                    SUM(
                        CASE
                            WHEN platform IN ('App') THEN external_clicks
                            ELSE 0
                        END
                    ) AS app_clicks
                FROM lazada_ads.ads_lzd_mkt_cost_sub_channel_campaign_hh
                WHERE 1 = 1
                    AND funding_bucket IN ('Lazada OM')
                    AND funding_type IN ('OM')
                    AND purpose IN ('Reacquired Buyer')
                    AND channel IN ('Social')
                    AND sub_channel IN ('Social - D0 Churn User')
                    AND venture = 'VN'
                    AND ds BETWEEN TO_CHAR(DATEADD(GETDATE(), -2, 'dd'), 'yyyymmdd') AND TO_CHAR(DATEADD(GETDATE(), 0, 'dd'), 'yyyymmdd')
                GROUP BY ds,
                    hh,
                    channel,
                    sub_channel,
                    rt_audience,
                    campaign GROUPING SETS ((ds, hh, channel, sub_channel, rt_audience))
                UNION ALL
                --<< OM table Channel: PPC - ARM (From Social Campaign)
                SELECT ds,
                    hh,
                    channel,
                    sub_channel,
                    rt_audience,
                    COALESCE(campaign, 'All') AS campaign,
                    SUM(spend) AS spend,
                    SUM(app_spend) AS app_spend,
                    SUM(impressions) AS impressions,
                    SUM(app_impressions) AS app_impressions,
                    SUM(clicks) AS clicks,
                    SUM(app_clicks) AS app_clicks
                FROM (
                        SELECT ds,
                            hh,
                            COALESCE(channel, 'ARM') AS channel,
                            CASE
                                WHEN campaign LIKE '%SA%SKU%' THEN 'Social SKU - ARM'
                                WHEN campaign LIKE '%Cluster%'
                                OR campaign LIKE '%SA-TopSubCatlist%' THEN 'Social Cluster - ARM'
                                WHEN campaign LIKE '%SBD%' THEN 'Social SBD - ARM'
                                WHEN campaign LIKE '%MMM%' THEN 'Social MMM - ARM'
                                ELSE 0
                            END AS sub_channel,
                            REPLACE('Unknown', 'Unknown', 'All') AS rt_audience,
                            campaign,
                            SUM(
                                CASE
                                    WHEN platform IN ('App', 'Pc', 'Wap', 'Web', 'Unknown') THEN spend
                                    ELSE 0
                                END
                            ) AS spend,
                            SUM(
                                CASE
                                    WHEN platform IN ('App') THEN spend
                                    ELSE 0
                                END
                            ) AS app_spend,
                            SUM(
                                CASE
                                    WHEN platform IN ('App', 'Pc', 'Wap', 'Web', 'Unknown') THEN external_impressions
                                    ELSE 0
                                END
                            ) AS impressions,
                            SUM(
                                CASE
                                    WHEN platform IN ('App') THEN external_impressions
                                    ELSE 0
                                END
                            ) AS app_impressions,
                            SUM(
                                CASE
                                    WHEN platform IN ('App', 'Pc', 'Wap', 'Web', 'Unknown') THEN external_clicks
                                    ELSE 0
                                END
                            ) AS clicks,
                            SUM(
                                CASE
                                    WHEN platform IN ('App') THEN external_clicks
                                    ELSE 0
                                END
                            ) AS app_clicks
                        FROM lazada_ads.ads_lzd_mkt_cost_sub_channel_campaign_hh
                        WHERE 1 = 1
                            AND funding_bucket IN ('Lazada OM')
                            AND funding_type IN ('OM')
                            AND purpose IN ('Broad - Campaign')
                            AND channel IN ('Social')
                            AND sub_channel IN ('Social - Campaign')
                            AND campaign LIKE '%SA%'
                            AND venture = 'VN'
                            AND ds BETWEEN TO_CHAR(DATEADD(GETDATE(), -2, 'dd'), 'yyyymmdd') AND TO_CHAR(DATEADD(GETDATE(), 0, 'dd'), 'yyyymmdd')
                        GROUP BY ds,
                            hh,
                            channel,
                            sub_channel,
                            rt_audience,
                            campaign GROUPING SETS ((ds, hh, campaign))
                    )
                GROUP BY ds,
                    hh,
                    channel,
                    sub_channel,
                    rt_audience,
                    campaign GROUPING SETS ((ds, hh, channel, sub_channel, rt_audience))
                UNION ALL
                --<< OM table Channel: PPC - ARM (From Retail Affiliates)
                SELECT ds,
                    hh,
                    channel,
                    sub_channel,
                    rt_audience,
                    COALESCE(campaign, 'All') AS campaign,
                    SUM(spend) AS spend,
                    SUM(app_spend) AS app_spend,
                    SUM(impressions) AS impressions,
                    SUM(app_impressions) AS app_impressions,
                    SUM(clicks) AS clicks,
                    SUM(app_clicks) AS app_clicks
                FROM (
                        SELECT ds,
                            hh,
                            COALESCE(channel, 'ARM') AS channel,
                            CASE
                                WHEN campaign LIKE '%SBD%' THEN 'Social SBD - ARM'
                                ELSE 'Social SKU - ARM'
                            END AS sub_channel,
                            REPLACE('Unknown', 'Unknown', 'All') AS rt_audience,
                            campaign,
                            SUM(
                                CASE
                                    WHEN platform IN ('App', 'Pc', 'Wap', 'Web', 'Unknown') THEN spend
                                    ELSE 0
                                END
                            ) AS spend,
                            SUM(
                                CASE
                                    WHEN platform IN ('App') THEN spend
                                    ELSE 0
                                END
                            ) AS app_spend,
                            SUM(
                                CASE
                                    WHEN platform IN ('App', 'Pc', 'Wap', 'Web', 'Unknown') THEN external_impressions
                                    ELSE 0
                                END
                            ) AS impressions,
                            SUM(
                                CASE
                                    WHEN platform IN ('App') THEN external_impressions
                                    ELSE 0
                                END
                            ) AS app_impressions,
                            SUM(
                                CASE
                                    WHEN platform IN ('App', 'Pc', 'Wap', 'Web', 'Unknown') THEN external_clicks
                                    ELSE 0
                                END
                            ) AS clicks,
                            SUM(
                                CASE
                                    WHEN platform IN ('App') THEN external_clicks
                                    ELSE 0
                                END
                            ) AS app_clicks
                        FROM lazada_ads.ads_lzd_mkt_cost_sub_channel_campaign_hh
                        WHERE 1 = 1
                            AND funding_bucket IN ('Retail Affiliates')
                            AND campaign LIKE '%VN%'
                            AND venture = 'VN'
                            AND ds BETWEEN TO_CHAR(DATEADD(GETDATE(), -2, 'dd'), 'yyyymmdd') AND TO_CHAR(DATEADD(GETDATE(), 0, 'dd'), 'yyyymmdd')
                        GROUP BY ds,
                            hh,
                            channel,
                            sub_channel,
                            rt_audience,
                            campaign GROUPING SETS ((ds, hh, campaign))
                    )
                GROUP BY ds,
                    hh,
                    channel,
                    sub_channel,
                    rt_audience,
                    campaign GROUPING SETS ((ds, hh, channel, sub_channel, rt_audience))
            )
        GROUP BY ds,
            hh,
            channel,
            sub_channel,
            rt_audience,
            campaign
        UNION ALL
        -- << BIG BREAK POINT
        SELECT ds,
            hh,
            channel,
            CASE
                WHEN TOLOWER(sub_channel) IN ('arm') THEN 'Social SKU - ARM'
                ELSE sub_channel
            END AS sub_channel,
            rt_audience,
            campaign,
            0 AS spend,
            0 AS app_spend,
            0 AS impressions,
            0 AS app_impressions,
            0 AS clicks,
            0 AS app_clicks,
            SUM(installs) AS installs,
            SUM(app_installs) AS app_installs,
            SUM(reinstalls) AS reinstalls,
            SUM(app_reinstalls) AS app_reinstalls,
            SUM(visits) AS visits,
            SUM(app_visits) AS app_visits,
            SUM(dau) AS dau,
            SUM(app_dau) AS app_dau,
            SUM(pdp_pv) AS pdp_pv,
            SUM(app_pdp_pv) AS app_pdp_pv,
            SUM(pdp_uv) AS pdp_uv,
            SUM(app_pdp_uv) AS app_pdp_uv,
            SUM(a2c) AS a2c,
            SUM(app_a2c) AS app_a2c,
            SUM(a2c_uv) AS a2c_uv,
            SUM(app_a2c_uv) AS app_a2c_uv,
            SUM(buyers) AS buyers,
            SUM(app_buyers) AS app_buyers,
            SUM(new_buyers) AS new_buyers,
            SUM(app_new_buyers) AS app_new_buyers,
            SUM(reacquired_buyers) AS reacquired_buyers,
            SUM(app_reacquired_buyers) AS app_reacquired_buyers,
            SUM(sales_order_id_count) AS sales_order_id_count,
            SUM(app_sales_order_id_count) AS app_sales_order_id_count,
            SUM(order_id_count) AS order_id_count --<< Order using
,
            SUM(app_order_id_count) AS app_order_id_count --<< Order using
,
            SUM(gmv) AS gmv,
            SUM(app_gmv) AS app_gmv
        FROM (
                --<< OM table Channel
                SELECT ds,
                    hh,
                    channel,
                    sub_channel,
                    COALESCE(rt_audience, 'All') AS rt_audience,
                    COALESCE(campaign, 'All') AS campaign,
                    SUM(
                        CASE
                            WHEN platform IN ('App', 'Pc', 'Wap', 'Unknown')
                            AND attr_model = 'lt_1d' THEN installs
                            ELSE 0
                        END
                    ) AS installs,
                    SUM(
                        CASE
                            WHEN platform IN ('App')
                            AND attr_model = 'lt_1d' THEN installs
                            ELSE 0
                        END
                    ) AS app_installs,
                    SUM(
                        CASE
                            WHEN platform IN ('App', 'Pc', 'Wap', 'Unknown')
                            AND attr_model = 'lt_1d' THEN reinstalls
                            ELSE 0
                        END
                    ) AS reinstalls,
                    SUM(
                        CASE
                            WHEN platform IN ('App')
                            AND attr_model = 'lt_1d' THEN reinstalls
                            ELSE 0
                        END
                    ) AS app_reinstalls,
                    SUM(
                        CASE
                            WHEN platform IN ('App', 'Pc', 'Wap', 'Unknown')
                            AND attr_model = 'ft_1d_np' THEN visits
                            ELSE 0
                        END
                    ) AS visits,
                    SUM(
                        CASE
                            WHEN platform IN ('App')
                            AND attr_model = 'ft_1d_np' THEN visits
                            ELSE 0
                        END
                    ) AS app_visits,
                    SUM(
                        CASE
                            WHEN platform IN ('App', 'Pc', 'Wap', 'Unknown')
                            AND attr_model = 'ft_1d_np' THEN dau
                            ELSE 0
                        END
                    ) AS dau,
                    SUM(
                        CASE
                            WHEN platform IN ('App')
                            AND attr_model = 'ft_1d_np' THEN dau
                            ELSE 0
                        END
                    ) AS app_dau,
                    SUM(
                        CASE
                            WHEN platform IN ('App', 'Pc', 'Wap', 'Unknown')
                            AND attr_model = 'lt_1d' THEN pdp_pv
                            ELSE 0
                        END
                    ) AS pdp_pv,
                    SUM(
                        CASE
                            WHEN platform IN ('App')
                            AND attr_model = 'lt_1d' THEN pdp_pv
                            ELSE 0
                        END
                    ) AS app_pdp_pv,
                    SUM(
                        CASE
                            WHEN platform IN ('App', 'Pc', 'Wap', 'Unknown')
                            AND attr_model = 'lt_1d' THEN pdp_uv
                            ELSE 0
                        END
                    ) AS pdp_uv,
                    SUM(
                        CASE
                            WHEN platform IN ('App')
                            AND attr_model = 'lt_1d' THEN pdp_uv
                            ELSE 0
                        END
                    ) AS app_pdp_uv,
                    SUM(
                        CASE
                            WHEN platform IN ('App', 'Pc', 'Wap', 'Unknown')
                            AND attr_model = 'lt_1d_p' THEN a2c
                            ELSE 0
                        END
                    ) AS a2c,
                    SUM(
                        CASE
                            WHEN platform IN ('App')
                            AND attr_model = 'lt_1d_p' THEN a2c
                            ELSE 0
                        END
                    ) AS app_a2c,
                    SUM(
                        CASE
                            WHEN platform IN ('App', 'Pc', 'Wap', 'Unknown')
                            AND attr_model = 'lt_1d_p' THEN a2c_uv
                            ELSE 0
                        END
                    ) AS a2c_uv,
                    SUM(
                        CASE
                            WHEN platform IN ('App')
                            AND attr_model = 'lt_1d_p' THEN a2c_uv
                            ELSE 0
                        END
                    ) AS app_a2c_uv,
                    SUM(
                        CASE
                            WHEN platform IN ('App', 'Pc', 'Wap', 'Unknown')
                            AND attr_model = 'lt_1d_p' THEN buyers
                            ELSE 0
                        END
                    ) AS buyers,
                    SUM(
                        CASE
                            WHEN platform IN ('App')
                            AND attr_model = 'lt_1d_p' THEN buyers
                            ELSE 0
                        END
                    ) AS app_buyers,
                    SUM(
                        CASE
                            WHEN platform IN ('App', 'Pc', 'Wap', 'Unknown')
                            AND attr_model = 'lt_1d_p' THEN new_buyers
                            ELSE 0
                        END
                    ) AS new_buyers,
                    SUM(
                        CASE
                            WHEN platform IN ('App')
                            AND attr_model = 'lt_1d_p' THEN new_buyers
                            ELSE 0
                        END
                    ) AS app_new_buyers,
                    SUM(
                        CASE
                            WHEN platform IN ('App', 'Pc', 'Wap', 'Unknown')
                            AND attr_model = 'lt_1d_p' THEN reacquired_buyers
                            ELSE 0
                        END
                    ) AS reacquired_buyers,
                    SUM(
                        CASE
                            WHEN platform IN ('App')
                            AND attr_model = 'lt_1d_p' THEN reacquired_buyers
                            ELSE 0
                        END
                    ) AS app_reacquired_buyers,
                    SUM(
                        CASE
                            WHEN platform IN ('App', 'Pc', 'Wap', 'Unknown')
                            AND attr_model = 'lt_1d_p' THEN sales_order_id_count
                            ELSE 0
                        END
                    ) AS sales_order_id_count,
                    SUM(
                        CASE
                            WHEN platform IN ('App')
                            AND attr_model = 'lt_1d_p' THEN sales_order_id_count
                            ELSE 0
                        END
                    ) AS app_sales_order_id_count,
                    SUM(
                        CASE
                            WHEN platform IN ('App', 'Pc', 'Wap', 'Unknown')
                            AND attr_model = 'lt_1d_p' THEN order_id_count
                            ELSE 0
                        END
                    ) AS order_id_count,
                    SUM(
                        CASE
                            WHEN platform IN ('App')
                            AND attr_model = 'lt_1d_p' THEN order_id_count
                            ELSE 0
                        END
                    ) AS app_order_id_count,
                    SUM(
                        CASE
                            WHEN platform IN ('App', 'Pc', 'Wap', 'Unknown')
                            AND attr_model = 'lt_1d_p' THEN gmv
                            ELSE 0
                        END
                    ) AS gmv,
                    SUM(
                        CASE
                            WHEN platform IN ('App')
                            AND attr_model = 'lt_1d_p' THEN gmv
                            ELSE 0
                        END
                    ) AS app_gmv
                FROM lazada_ads.ads_lzd_mkt_rt_uam_campaign_hh
                WHERE 1 = 1
                    AND funding_bucket IN ('Lazada OM')
                    AND funding_type IN ('OM', 'AMS')
                    AND purpose IN (
                        'Broad',
                        'Broad - Campaign',
                        'Grow MAB',
                        'New Buyer - Conversion',
                        'New Buyer - Install',
                        'New Buyer - NB3order',
                        'Newbuyer-NB 30D 3order',
                        'Newbuyer-Non AAB',
                        'Reacquired Buyer',
                        'Retention'
                    )
                    AND channel IN (
                        'SEM',
                        'Social',
                        'Apple Search',
                        'Display',
                        'TikTok',
                        'ARM',
                        'RTA'
                    )
                    AND sub_channel NOT IN (
                        'SEM Shopping',
                        'Social - Campaign',
                        'Social - D0 Churn User'
                    )
                    AND attr_model IN ('ft_1d_np', 'lt_1d', 'lt_1d_p')
                    AND venture = 'VN'
                    AND ds BETWEEN TO_CHAR(DATEADD(GETDATE(), -2, 'dd'), 'yyyymmdd') AND TO_CHAR(DATEADD(GETDATE(), 0, 'dd'), 'yyyymmdd')
                GROUP BY ds,
                    hh,
                    channel,
                    sub_channel,
                    rt_audience,
                    campaign GROUPING SETS ((ds, hh, channel, sub_channel))
                UNION ALL
                --<< OM table Channel: SEM Shopping
                SELECT ds,
                    hh,
                    channel,
                    sub_channel,
                    CASE
                        WHEN rt_audience IS NOT NULL THEN 'All'
                        ELSE COALESCE(rt_audience, 'All')
                    END AS rt_audience,
                    COALESCE(campaign, 'All') AS campaign,
                    SUM(
                        CASE
                            WHEN platform IN ('App', 'Pc', 'Wap', 'Unknown')
                            AND attr_model = 'lt_1d' THEN installs
                            ELSE 0
                        END
                    ) AS installs,
                    SUM(
                        CASE
                            WHEN platform IN ('App')
                            AND attr_model = 'lt_1d' THEN installs
                            ELSE 0
                        END
                    ) AS app_installs,
                    SUM(
                        CASE
                            WHEN platform IN ('App', 'Pc', 'Wap', 'Unknown')
                            AND attr_model = 'lt_1d' THEN reinstalls
                            ELSE 0
                        END
                    ) AS reinstalls,
                    SUM(
                        CASE
                            WHEN platform IN ('App')
                            AND attr_model = 'lt_1d' THEN reinstalls
                            ELSE 0
                        END
                    ) AS app_reinstalls,
                    SUM(
                        CASE
                            WHEN platform IN ('App', 'Pc', 'Wap', 'Unknown')
                            AND attr_model = 'ft_1d_np' THEN visits
                            ELSE 0
                        END
                    ) AS visits,
                    SUM(
                        CASE
                            WHEN platform IN ('App')
                            AND attr_model = 'ft_1d_np' THEN visits
                            ELSE 0
                        END
                    ) AS app_visits,
                    SUM(
                        CASE
                            WHEN platform IN ('App', 'Pc', 'Wap', 'Unknown')
                            AND attr_model = 'ft_1d_np' THEN dau
                            ELSE 0
                        END
                    ) AS dau,
                    SUM(
                        CASE
                            WHEN platform IN ('App')
                            AND attr_model = 'ft_1d_np' THEN dau
                            ELSE 0
                        END
                    ) AS app_dau,
                    SUM(
                        CASE
                            WHEN platform IN ('App', 'Pc', 'Wap', 'Unknown')
                            AND attr_model = 'lt_1d' THEN pdp_pv
                            ELSE 0
                        END
                    ) AS pdp_pv,
                    SUM(
                        CASE
                            WHEN platform IN ('App')
                            AND attr_model = 'lt_1d' THEN pdp_pv
                            ELSE 0
                        END
                    ) AS app_pdp_pv,
                    SUM(
                        CASE
                            WHEN platform IN ('App', 'Pc', 'Wap', 'Unknown')
                            AND attr_model = 'lt_1d' THEN pdp_uv
                            ELSE 0
                        END
                    ) AS pdp_uv,
                    SUM(
                        CASE
                            WHEN platform IN ('App')
                            AND attr_model = 'lt_1d' THEN pdp_uv
                            ELSE 0
                        END
                    ) AS app_pdp_uv,
                    SUM(
                        CASE
                            WHEN platform IN ('App', 'Pc', 'Wap', 'Unknown')
                            AND attr_model = 'lt_1d_p' THEN a2c
                            ELSE 0
                        END
                    ) AS a2c,
                    SUM(
                        CASE
                            WHEN platform IN ('App')
                            AND attr_model = 'lt_1d_p' THEN a2c
                            ELSE 0
                        END
                    ) AS app_a2c,
                    SUM(
                        CASE
                            WHEN platform IN ('App', 'Pc', 'Wap', 'Unknown')
                            AND attr_model = 'lt_1d_p' THEN a2c_uv
                            ELSE 0
                        END
                    ) AS a2c_uv,
                    SUM(
                        CASE
                            WHEN platform IN ('App')
                            AND attr_model = 'lt_1d_p' THEN a2c_uv
                            ELSE 0
                        END
                    ) AS app_a2c_uv,
                    SUM(
                        CASE
                            WHEN platform IN ('App', 'Pc', 'Wap', 'Unknown')
                            AND attr_model = 'lt_1d_p' THEN buyers
                            ELSE 0
                        END
                    ) AS buyers,
                    SUM(
                        CASE
                            WHEN platform IN ('App')
                            AND attr_model = 'lt_1d_p' THEN buyers
                            ELSE 0
                        END
                    ) AS app_buyers,
                    SUM(
                        CASE
                            WHEN platform IN ('App', 'Pc', 'Wap', 'Unknown')
                            AND attr_model = 'lt_1d_p' THEN new_buyers
                            ELSE 0
                        END
                    ) AS new_buyers,
                    SUM(
                        CASE
                            WHEN platform IN ('App')
                            AND attr_model = 'lt_1d_p' THEN new_buyers
                            ELSE 0
                        END
                    ) AS app_new_buyers,
                    SUM(
                        CASE
                            WHEN platform IN ('App', 'Pc', 'Wap', 'Unknown')
                            AND attr_model = 'lt_1d_p' THEN reacquired_buyers
                            ELSE 0
                        END
                    ) AS reacquired_buyers,
                    SUM(
                        CASE
                            WHEN platform IN ('App')
                            AND attr_model = 'lt_1d_p' THEN reacquired_buyers
                            ELSE 0
                        END
                    ) AS app_reacquired_buyers,
                    SUM(
                        CASE
                            WHEN platform IN ('App', 'Pc', 'Wap', 'Unknown')
                            AND attr_model = 'lt_1d_p' THEN sales_order_id_count
                            ELSE 0
                        END
                    ) AS sales_order_id_count,
                    SUM(
                        CASE
                            WHEN platform IN ('App')
                            AND attr_model = 'lt_1d_p' THEN sales_order_id_count
                            ELSE 0
                        END
                    ) AS app_sales_order_id_count,
                    SUM(
                        CASE
                            WHEN platform IN ('App', 'Pc', 'Wap', 'Unknown')
                            AND attr_model = 'lt_1d_p' THEN order_id_count
                            ELSE 0
                        END
                    ) AS order_id_count,
                    SUM(
                        CASE
                            WHEN platform IN ('App')
                            AND attr_model = 'lt_1d_p' THEN order_id_count
                            ELSE 0
                        END
                    ) AS app_order_id_count,
                    SUM(
                        CASE
                            WHEN platform IN ('App', 'Pc', 'Wap', 'Unknown')
                            AND attr_model = 'lt_1d_p' THEN gmv
                            ELSE 0
                        END
                    ) AS gmv,
                    SUM(
                        CASE
                            WHEN platform IN ('App')
                            AND attr_model = 'lt_1d_p' THEN gmv
                            ELSE 0
                        END
                    ) AS app_gmv
                FROM lazada_ads.ads_lzd_mkt_rt_uam_campaign_hh
                WHERE 1 = 1
                    AND funding_bucket IN ('Lazada OM')
                    AND funding_type IN ('OM')
                    AND purpose IN ('Broad')
                    AND channel IN ('SEM')
                    AND sub_channel IN ('SEM Shopping')
                    AND attr_model IN ('ft_1d_np', 'lt_1d', 'lt_1d_p')
                    AND venture = 'VN'
                    AND ds BETWEEN TO_CHAR(DATEADD(GETDATE(), -2, 'dd'), 'yyyymmdd') AND TO_CHAR(DATEADD(GETDATE(), 0, 'dd'), 'yyyymmdd')
                GROUP BY ds,
                    hh,
                    channel,
                    sub_channel,
                    rt_audience,
                    campaign GROUPING SETS (
                        (
                            ds,
                            hh,
                            channel,
                            sub_channel,
                            rt_audience,
                            campaign
                        ),
                        (ds, hh, channel, sub_channel)
                    )
                UNION ALL
                --<< OM table Channel: Social - Campaign
                SELECT ds,
                    hh,
                    channel,
                    sub_channel,
                    CASE
                        WHEN rt_audience IN ('Unknown') THEN 'All'
                        ELSE rt_audience
                    END AS rt_audience,
                    COALESCE(campaign, 'All') AS campaign,
                    SUM(
                        CASE
                            WHEN platform IN ('App', 'Pc', 'Wap', 'Unknown')
                            AND attr_model = 'lt_1d' THEN installs
                            ELSE 0
                        END
                    ) AS installs,
                    SUM(
                        CASE
                            WHEN platform IN ('App')
                            AND attr_model = 'lt_1d' THEN installs
                            ELSE 0
                        END
                    ) AS app_installs,
                    SUM(
                        CASE
                            WHEN platform IN ('App', 'Pc', 'Wap', 'Unknown')
                            AND attr_model = 'lt_1d' THEN reinstalls
                            ELSE 0
                        END
                    ) AS reinstalls,
                    SUM(
                        CASE
                            WHEN platform IN ('App')
                            AND attr_model = 'lt_1d' THEN reinstalls
                            ELSE 0
                        END
                    ) AS app_reinstalls,
                    SUM(
                        CASE
                            WHEN platform IN ('App', 'Pc', 'Wap', 'Unknown')
                            AND attr_model = 'ft_1d_np' THEN visits
                            ELSE 0
                        END
                    ) AS visits,
                    SUM(
                        CASE
                            WHEN platform IN ('App')
                            AND attr_model = 'ft_1d_np' THEN visits
                            ELSE 0
                        END
                    ) AS app_visits,
                    SUM(
                        CASE
                            WHEN platform IN ('App', 'Pc', 'Wap', 'Unknown')
                            AND attr_model = 'ft_1d_np' THEN dau
                            ELSE 0
                        END
                    ) AS dau,
                    SUM(
                        CASE
                            WHEN platform IN ('App')
                            AND attr_model = 'ft_1d_np' THEN dau
                            ELSE 0
                        END
                    ) AS app_dau,
                    SUM(
                        CASE
                            WHEN platform IN ('App', 'Pc', 'Wap', 'Unknown')
                            AND attr_model = 'lt_1d' THEN pdp_pv
                            ELSE 0
                        END
                    ) AS pdp_pv,
                    SUM(
                        CASE
                            WHEN platform IN ('App')
                            AND attr_model = 'lt_1d' THEN pdp_pv
                            ELSE 0
                        END
                    ) AS app_pdp_pv,
                    SUM(
                        CASE
                            WHEN platform IN ('App', 'Pc', 'Wap', 'Unknown')
                            AND attr_model = 'lt_1d' THEN pdp_uv
                            ELSE 0
                        END
                    ) AS pdp_uv,
                    SUM(
                        CASE
                            WHEN platform IN ('App')
                            AND attr_model = 'lt_1d' THEN pdp_uv
                            ELSE 0
                        END
                    ) AS app_pdp_uv,
                    SUM(
                        CASE
                            WHEN platform IN ('App', 'Pc', 'Wap', 'Unknown')
                            AND attr_model = 'lt_1d_p' THEN a2c
                            ELSE 0
                        END
                    ) AS a2c,
                    SUM(
                        CASE
                            WHEN platform IN ('App')
                            AND attr_model = 'lt_1d_p' THEN a2c
                            ELSE 0
                        END
                    ) AS app_a2c,
                    SUM(
                        CASE
                            WHEN platform IN ('App', 'Pc', 'Wap', 'Unknown')
                            AND attr_model = 'lt_1d_p' THEN a2c_uv
                            ELSE 0
                        END
                    ) AS a2c_uv,
                    SUM(
                        CASE
                            WHEN platform IN ('App')
                            AND attr_model = 'lt_1d_p' THEN a2c_uv
                            ELSE 0
                        END
                    ) AS app_a2c_uv,
                    SUM(
                        CASE
                            WHEN platform IN ('App', 'Pc', 'Wap', 'Unknown')
                            AND attr_model = 'lt_1d_p' THEN buyers
                            ELSE 0
                        END
                    ) AS buyers,
                    SUM(
                        CASE
                            WHEN platform IN ('App')
                            AND attr_model = 'lt_1d_p' THEN buyers
                            ELSE 0
                        END
                    ) AS app_buyers,
                    SUM(
                        CASE
                            WHEN platform IN ('App', 'Pc', 'Wap', 'Unknown')
                            AND attr_model = 'lt_1d_p' THEN new_buyers
                            ELSE 0
                        END
                    ) AS new_buyers,
                    SUM(
                        CASE
                            WHEN platform IN ('App')
                            AND attr_model = 'lt_1d_p' THEN new_buyers
                            ELSE 0
                        END
                    ) AS app_new_buyers,
                    SUM(
                        CASE
                            WHEN platform IN ('App', 'Pc', 'Wap', 'Unknown')
                            AND attr_model = 'lt_1d_p' THEN reacquired_buyers
                            ELSE 0
                        END
                    ) AS reacquired_buyers,
                    SUM(
                        CASE
                            WHEN platform IN ('App')
                            AND attr_model = 'lt_1d_p' THEN reacquired_buyers
                            ELSE 0
                        END
                    ) AS app_reacquired_buyers,
                    SUM(
                        CASE
                            WHEN platform IN ('App', 'Pc', 'Wap', 'Unknown')
                            AND attr_model = 'lt_1d_p' THEN sales_order_id_count
                            ELSE 0
                        END
                    ) AS sales_order_id_count,
                    SUM(
                        CASE
                            WHEN platform IN ('App')
                            AND attr_model = 'lt_1d_p' THEN sales_order_id_count
                            ELSE 0
                        END
                    ) AS app_sales_order_id_count,
                    SUM(
                        CASE
                            WHEN platform IN ('App', 'Pc', 'Wap', 'Unknown')
                            AND attr_model = 'lt_1d_p' THEN order_id_count
                            ELSE 0
                        END
                    ) AS order_id_count,
                    SUM(
                        CASE
                            WHEN platform IN ('App')
                            AND attr_model = 'lt_1d_p' THEN order_id_count
                            ELSE 0
                        END
                    ) AS app_order_id_count,
                    SUM(
                        CASE
                            WHEN platform IN ('App', 'Pc', 'Wap', 'Unknown')
                            AND attr_model = 'lt_1d_p' THEN gmv
                            ELSE 0
                        END
                    ) AS gmv,
                    SUM(
                        CASE
                            WHEN platform IN ('App')
                            AND attr_model = 'lt_1d_p' THEN gmv
                            ELSE 0
                        END
                    ) AS app_gmv
                FROM lazada_ads.ads_lzd_mkt_rt_uam_campaign_hh
                WHERE 1 = 1
                    AND funding_bucket IN ('Lazada OM')
                    AND funding_type IN ('OM')
                    AND purpose IN ('Broad - Campaign')
                    AND channel IN ('Social')
                    AND sub_channel IN ('Social - Campaign')
                    AND attr_model IN ('ft_1d_np', 'lt_1d', 'lt_1d_p')
                    AND venture = 'VN'
                    AND ds BETWEEN TO_CHAR(DATEADD(GETDATE(), -2, 'dd'), 'yyyymmdd') AND TO_CHAR(DATEADD(GETDATE(), 0, 'dd'), 'yyyymmdd')
                GROUP BY ds,
                    hh,
                    channel,
                    sub_channel,
                    rt_audience,
                    campaign GROUPING SETS ((ds, hh, channel, sub_channel, rt_audience))
                UNION ALL
                --<< OM table Channel: Social - Campaign (Campaign level)
                SELECT ds,
                    hh,
                    channel,
                    sub_channel,
                    REPLACE('Unknown', 'Unknown', 'All') AS rt_audience,
                    campaign,
                    SUM(
                        CASE
                            WHEN platform IN ('App', 'Pc', 'Wap', 'Unknown')
                            AND attr_model = 'lt_1d' THEN installs
                            ELSE 0
                        END
                    ) AS installs,
                    SUM(
                        CASE
                            WHEN platform IN ('App')
                            AND attr_model = 'lt_1d' THEN installs
                            ELSE 0
                        END
                    ) AS app_installs,
                    SUM(
                        CASE
                            WHEN platform IN ('App', 'Pc', 'Wap', 'Unknown')
                            AND attr_model = 'lt_1d' THEN reinstalls
                            ELSE 0
                        END
                    ) AS reinstalls,
                    SUM(
                        CASE
                            WHEN platform IN ('App')
                            AND attr_model = 'lt_1d' THEN reinstalls
                            ELSE 0
                        END
                    ) AS app_reinstalls,
                    SUM(
                        CASE
                            WHEN platform IN ('App', 'Pc', 'Wap', 'Unknown')
                            AND attr_model = 'ft_1d_np' THEN visits
                            ELSE 0
                        END
                    ) AS visits,
                    SUM(
                        CASE
                            WHEN platform IN ('App')
                            AND attr_model = 'ft_1d_np' THEN visits
                            ELSE 0
                        END
                    ) AS app_visits,
                    SUM(
                        CASE
                            WHEN platform IN ('App', 'Pc', 'Wap', 'Unknown')
                            AND attr_model = 'ft_1d_np' THEN dau
                            ELSE 0
                        END
                    ) AS dau,
                    SUM(
                        CASE
                            WHEN platform IN ('App')
                            AND attr_model = 'ft_1d_np' THEN dau
                            ELSE 0
                        END
                    ) AS app_dau,
                    SUM(
                        CASE
                            WHEN platform IN ('App', 'Pc', 'Wap', 'Unknown')
                            AND attr_model = 'lt_1d' THEN pdp_pv
                            ELSE 0
                        END
                    ) AS pdp_pv,
                    SUM(
                        CASE
                            WHEN platform IN ('App')
                            AND attr_model = 'lt_1d' THEN pdp_pv
                            ELSE 0
                        END
                    ) AS app_pdp_pv,
                    SUM(
                        CASE
                            WHEN platform IN ('App', 'Pc', 'Wap', 'Unknown')
                            AND attr_model = 'lt_1d' THEN pdp_uv
                            ELSE 0
                        END
                    ) AS pdp_uv,
                    SUM(
                        CASE
                            WHEN platform IN ('App')
                            AND attr_model = 'lt_1d' THEN pdp_uv
                            ELSE 0
                        END
                    ) AS app_pdp_uv,
                    SUM(
                        CASE
                            WHEN platform IN ('App', 'Pc', 'Wap', 'Unknown')
                            AND attr_model = 'lt_1d_p' THEN a2c
                            ELSE 0
                        END
                    ) AS a2c,
                    SUM(
                        CASE
                            WHEN platform IN ('App')
                            AND attr_model = 'lt_1d_p' THEN a2c
                            ELSE 0
                        END
                    ) AS app_a2c,
                    SUM(
                        CASE
                            WHEN platform IN ('App', 'Pc', 'Wap', 'Unknown')
                            AND attr_model = 'lt_1d_p' THEN a2c_uv
                            ELSE 0
                        END
                    ) AS a2c_uv,
                    SUM(
                        CASE
                            WHEN platform IN ('App')
                            AND attr_model = 'lt_1d_p' THEN a2c_uv
                            ELSE 0
                        END
                    ) AS app_a2c_uv,
                    SUM(
                        CASE
                            WHEN platform IN ('App', 'Pc', 'Wap', 'Unknown')
                            AND attr_model = 'lt_1d_p' THEN buyers
                            ELSE 0
                        END
                    ) AS buyers,
                    SUM(
                        CASE
                            WHEN platform IN ('App')
                            AND attr_model = 'lt_1d_p' THEN buyers
                            ELSE 0
                        END
                    ) AS app_buyers,
                    SUM(
                        CASE
                            WHEN platform IN ('App', 'Pc', 'Wap', 'Unknown')
                            AND attr_model = 'lt_1d_p' THEN new_buyers
                            ELSE 0
                        END
                    ) AS new_buyers,
                    SUM(
                        CASE
                            WHEN platform IN ('App')
                            AND attr_model = 'lt_1d_p' THEN new_buyers
                            ELSE 0
                        END
                    ) AS app_new_buyers,
                    SUM(
                        CASE
                            WHEN platform IN ('App', 'Pc', 'Wap', 'Unknown')
                            AND attr_model = 'lt_1d_p' THEN reacquired_buyers
                            ELSE 0
                        END
                    ) AS reacquired_buyers,
                    SUM(
                        CASE
                            WHEN platform IN ('App')
                            AND attr_model = 'lt_1d_p' THEN reacquired_buyers
                            ELSE 0
                        END
                    ) AS app_reacquired_buyers,
                    SUM(
                        CASE
                            WHEN platform IN ('App', 'Pc', 'Wap', 'Unknown')
                            AND attr_model = 'lt_1d_p' THEN sales_order_id_count
                            ELSE 0
                        END
                    ) AS sales_order_id_count,
                    SUM(
                        CASE
                            WHEN platform IN ('App')
                            AND attr_model = 'lt_1d_p' THEN sales_order_id_count
                            ELSE 0
                        END
                    ) AS app_sales_order_id_count,
                    SUM(
                        CASE
                            WHEN platform IN ('App', 'Pc', 'Wap', 'Unknown')
                            AND attr_model = 'lt_1d_p' THEN order_id_count
                            ELSE 0
                        END
                    ) AS order_id_count,
                    SUM(
                        CASE
                            WHEN platform IN ('App')
                            AND attr_model = 'lt_1d_p' THEN order_id_count
                            ELSE 0
                        END
                    ) AS app_order_id_count,
                    SUM(
                        CASE
                            WHEN platform IN ('App', 'Pc', 'Wap', 'Unknown')
                            AND attr_model = 'lt_1d_p' THEN gmv
                            ELSE 0
                        END
                    ) AS gmv,
                    SUM(
                        CASE
                            WHEN platform IN ('App')
                            AND attr_model = 'lt_1d_p' THEN gmv
                            ELSE 0
                        END
                    ) AS app_gmv
                FROM lazada_ads.ads_lzd_mkt_rt_uam_campaign_hh
                WHERE 1 = 1
                    AND funding_bucket IN ('Lazada OM')
                    AND funding_type IN ('OM')
                    AND purpose IN ('Broad - Campaign')
                    AND channel IN ('Social')
                    AND sub_channel IN ('Social - Campaign')
                    AND campaign LIKE '%VN%'
                    AND rt_audience IN ('Unknown')
                    AND attr_model IN ('ft_1d_np', 'lt_1d', 'lt_1d_p')
                    AND venture = 'VN'
                    AND ds BETWEEN TO_CHAR(DATEADD(GETDATE(), -2, 'dd'), 'yyyymmdd') AND TO_CHAR(DATEADD(GETDATE(), 0, 'dd'), 'yyyymmdd')
                GROUP BY ds,
                    hh,
                    channel,
                    sub_channel,
                    rt_audience,
                    campaign
                UNION ALL
                --<< OM table Channel: Social - D0 (Campaign level)
                SELECT ds,
                    hh,
                    channel,
                    sub_channel,
                    rt_audience,
                    COALESCE(campaign, 'All') AS campaign,
                    SUM(
                        CASE
                            WHEN platform IN ('App', 'Pc', 'Wap', 'Unknown')
                            AND attr_model = 'lt_1d' THEN installs
                            ELSE 0
                        END
                    ) AS installs,
                    SUM(
                        CASE
                            WHEN platform IN ('App')
                            AND attr_model = 'lt_1d' THEN installs
                            ELSE 0
                        END
                    ) AS app_installs,
                    SUM(
                        CASE
                            WHEN platform IN ('App', 'Pc', 'Wap', 'Unknown')
                            AND attr_model = 'lt_1d' THEN reinstalls
                            ELSE 0
                        END
                    ) AS reinstalls,
                    SUM(
                        CASE
                            WHEN platform IN ('App')
                            AND attr_model = 'lt_1d' THEN reinstalls
                            ELSE 0
                        END
                    ) AS app_reinstalls,
                    SUM(
                        CASE
                            WHEN platform IN ('App', 'Pc', 'Wap', 'Unknown')
                            AND attr_model = 'ft_1d_np' THEN visits
                            ELSE 0
                        END
                    ) AS visits,
                    SUM(
                        CASE
                            WHEN platform IN ('App')
                            AND attr_model = 'ft_1d_np' THEN visits
                            ELSE 0
                        END
                    ) AS app_visits,
                    SUM(
                        CASE
                            WHEN platform IN ('App', 'Pc', 'Wap', 'Unknown')
                            AND attr_model = 'ft_1d_np' THEN dau
                            ELSE 0
                        END
                    ) AS dau,
                    SUM(
                        CASE
                            WHEN platform IN ('App')
                            AND attr_model = 'ft_1d_np' THEN dau
                            ELSE 0
                        END
                    ) AS app_dau,
                    SUM(
                        CASE
                            WHEN platform IN ('App', 'Pc', 'Wap', 'Unknown')
                            AND attr_model = 'lt_1d' THEN pdp_pv
                            ELSE 0
                        END
                    ) AS pdp_pv,
                    SUM(
                        CASE
                            WHEN platform IN ('App')
                            AND attr_model = 'lt_1d' THEN pdp_pv
                            ELSE 0
                        END
                    ) AS app_pdp_pv,
                    SUM(
                        CASE
                            WHEN platform IN ('App', 'Pc', 'Wap', 'Unknown')
                            AND attr_model = 'lt_1d' THEN pdp_uv
                            ELSE 0
                        END
                    ) AS pdp_uv,
                    SUM(
                        CASE
                            WHEN platform IN ('App')
                            AND attr_model = 'lt_1d' THEN pdp_uv
                            ELSE 0
                        END
                    ) AS app_pdp_uv,
                    SUM(
                        CASE
                            WHEN platform IN ('App', 'Pc', 'Wap', 'Unknown')
                            AND attr_model = 'lt_1d_p' THEN a2c
                            ELSE 0
                        END
                    ) AS a2c,
                    SUM(
                        CASE
                            WHEN platform IN ('App')
                            AND attr_model = 'lt_1d_p' THEN a2c
                            ELSE 0
                        END
                    ) AS app_a2c,
                    SUM(
                        CASE
                            WHEN platform IN ('App', 'Pc', 'Wap', 'Unknown')
                            AND attr_model = 'lt_1d_p' THEN a2c_uv
                            ELSE 0
                        END
                    ) AS a2c_uv,
                    SUM(
                        CASE
                            WHEN platform IN ('App')
                            AND attr_model = 'lt_1d_p' THEN a2c_uv
                            ELSE 0
                        END
                    ) AS app_a2c_uv,
                    SUM(
                        CASE
                            WHEN platform IN ('App', 'Pc', 'Wap', 'Unknown')
                            AND attr_model = 'lt_1d_p' THEN buyers
                            ELSE 0
                        END
                    ) AS buyers,
                    SUM(
                        CASE
                            WHEN platform IN ('App')
                            AND attr_model = 'lt_1d_p' THEN buyers
                            ELSE 0
                        END
                    ) AS app_buyers,
                    SUM(
                        CASE
                            WHEN platform IN ('App', 'Pc', 'Wap', 'Unknown')
                            AND attr_model = 'lt_1d_p' THEN new_buyers
                            ELSE 0
                        END
                    ) AS new_buyers,
                    SUM(
                        CASE
                            WHEN platform IN ('App')
                            AND attr_model = 'lt_1d_p' THEN new_buyers
                            ELSE 0
                        END
                    ) AS app_new_buyers,
                    SUM(
                        CASE
                            WHEN platform IN ('App', 'Pc', 'Wap', 'Unknown')
                            AND attr_model = 'lt_1d_p' THEN reacquired_buyers
                            ELSE 0
                        END
                    ) AS reacquired_buyers,
                    SUM(
                        CASE
                            WHEN platform IN ('App')
                            AND attr_model = 'lt_1d_p' THEN reacquired_buyers
                            ELSE 0
                        END
                    ) AS app_reacquired_buyers,
                    SUM(
                        CASE
                            WHEN platform IN ('App', 'Pc', 'Wap', 'Unknown')
                            AND attr_model = 'lt_1d_p' THEN sales_order_id_count
                            ELSE 0
                        END
                    ) AS sales_order_id_count,
                    SUM(
                        CASE
                            WHEN platform IN ('App')
                            AND attr_model = 'lt_1d_p' THEN sales_order_id_count
                            ELSE 0
                        END
                    ) AS app_sales_order_id_count,
                    SUM(
                        CASE
                            WHEN platform IN ('App', 'Pc', 'Wap', 'Unknown')
                            AND attr_model = 'lt_1d_p' THEN order_id_count
                            ELSE 0
                        END
                    ) AS order_id_count,
                    SUM(
                        CASE
                            WHEN platform IN ('App')
                            AND attr_model = 'lt_1d_p' THEN order_id_count
                            ELSE 0
                        END
                    ) AS app_order_id_count,
                    SUM(
                        CASE
                            WHEN platform IN ('App', 'Pc', 'Wap', 'Unknown')
                            AND attr_model = 'lt_1d_p' THEN gmv
                            ELSE 0
                        END
                    ) AS gmv,
                    SUM(
                        CASE
                            WHEN platform IN ('App')
                            AND attr_model = 'lt_1d_p' THEN gmv
                            ELSE 0
                        END
                    ) AS app_gmv
                FROM lazada_ads.ads_lzd_mkt_rt_uam_campaign_hh
                WHERE 1 = 1
                    AND funding_bucket IN ('Lazada OM')
                    AND funding_type IN ('OM')
                    AND purpose IN ('Reacquired Buyer')
                    AND channel IN ('Social')
                    AND sub_channel IN ('Social - D0 Churn User')
                    AND attr_model IN ('ft_1d_np', 'lt_1d', 'lt_1d_p')
                    AND venture = 'VN'
                    AND ds BETWEEN TO_CHAR(DATEADD(GETDATE(), -2, 'dd'), 'yyyymmdd') AND TO_CHAR(DATEADD(GETDATE(), 0, 'dd'), 'yyyymmdd')
                GROUP BY ds,
                    hh,
                    channel,
                    sub_channel,
                    rt_audience,
                    campaign GROUPING SETS ((ds, hh, channel, sub_channel, rt_audience))
                UNION ALL
                --<< OM table Channel: PPC - ARM
                SELECT ds,
                    hh,
                    channel,
                    sub_channel,
                    rt_audience,
                    COALESCE(campaign, 'All') AS campaign,
                    SUM(installs) AS installs,
                    SUM(app_installs) AS app_installs,
                    SUM(reinstalls) AS reinstalls,
                    SUM(app_reinstalls) AS app_reinstalls,
                    SUM(visits) AS visits,
                    SUM(app_visits) AS app_visits,
                    SUM(dau) AS dau,
                    SUM(app_dau) AS app_dau,
                    SUM(pdp_pv) AS pdp_pv,
                    SUM(app_pdp_pv) AS app_pdp_pv,
                    SUM(pdp_uv) AS pdp_uv,
                    SUM(app_pdp_uv) AS app_pdp_uv,
                    SUM(a2c) AS a2c,
                    SUM(app_a2c) AS app_a2c,
                    SUM(a2c_uv) AS a2c_uv,
                    SUM(app_a2c_uv) AS app_a2c_uv,
                    SUM(buyers) AS buyers,
                    SUM(app_buyers) AS app_buyers,
                    SUM(new_buyers) AS new_buyers,
                    SUM(app_new_buyers) AS app_new_buyers,
                    SUM(reacquired_buyers) AS reacquired_buyers,
                    SUM(app_reacquired_buyers) AS app_reacquired_buyers,
                    SUM(sales_order_id_count) AS sales_order_id_count,
                    SUM(app_sales_order_id_count) AS app_sales_order_id_count,
                    SUM(order_id_count) AS order_id_count,
                    SUM(app_order_id_count) AS app_order_id_count,
                    SUM(gmv) AS gmv,
                    SUM(app_gmv) AS app_gmv
                FROM (
                        SELECT ds,
                            hh,
                            COALESCE(channel, 'ARM') AS channel,
                            CASE
                                WHEN campaign LIKE '%SBD%' THEN 'Social SBD - ARM'
                                ELSE 'Social SKU - ARM'
                            END AS sub_channel,
                            REPLACE('Unknown', 'Unknown', 'All') AS rt_audience,
                            campaign,
                            SUM(
                                CASE
                                    WHEN platform IN ('App', 'Pc', 'Wap', 'Unknown')
                                    AND attr_model = 'lt_1d' THEN installs
                                    ELSE 0
                                END
                            ) AS installs,
                            SUM(
                                CASE
                                    WHEN platform IN ('App')
                                    AND attr_model = 'lt_1d' THEN installs
                                    ELSE 0
                                END
                            ) AS app_installs,
                            SUM(
                                CASE
                                    WHEN platform IN ('App', 'Pc', 'Wap', 'Unknown')
                                    AND attr_model = 'lt_1d' THEN reinstalls
                                    ELSE 0
                                END
                            ) AS reinstalls,
                            SUM(
                                CASE
                                    WHEN platform IN ('App')
                                    AND attr_model = 'lt_1d' THEN reinstalls
                                    ELSE 0
                                END
                            ) AS app_reinstalls,
                            SUM(
                                CASE
                                    WHEN platform IN ('App', 'Pc', 'Wap', 'Unknown')
                                    AND attr_model = 'ft_1d_np' THEN visits
                                    ELSE 0
                                END
                            ) AS visits,
                            SUM(
                                CASE
                                    WHEN platform IN ('App')
                                    AND attr_model = 'ft_1d_np' THEN visits
                                    ELSE 0
                                END
                            ) AS app_visits,
                            SUM(
                                CASE
                                    WHEN platform IN ('App', 'Pc', 'Wap', 'Unknown')
                                    AND attr_model = 'ft_1d_np' THEN dau
                                    ELSE 0
                                END
                            ) AS dau,
                            SUM(
                                CASE
                                    WHEN platform IN ('App')
                                    AND attr_model = 'ft_1d_np' THEN dau
                                    ELSE 0
                                END
                            ) AS app_dau,
                            SUM(
                                CASE
                                    WHEN platform IN ('App', 'Pc', 'Wap', 'Unknown')
                                    AND attr_model = 'lt_1d' THEN pdp_pv
                                    ELSE 0
                                END
                            ) AS pdp_pv,
                            SUM(
                                CASE
                                    WHEN platform IN ('App')
                                    AND attr_model = 'lt_1d' THEN pdp_pv
                                    ELSE 0
                                END
                            ) AS app_pdp_pv,
                            SUM(
                                CASE
                                    WHEN platform IN ('App', 'Pc', 'Wap', 'Unknown')
                                    AND attr_model = 'lt_1d' THEN pdp_uv
                                    ELSE 0
                                END
                            ) AS pdp_uv,
                            SUM(
                                CASE
                                    WHEN platform IN ('App')
                                    AND attr_model = 'lt_1d' THEN pdp_uv
                                    ELSE 0
                                END
                            ) AS app_pdp_uv,
                            SUM(
                                CASE
                                    WHEN platform IN ('App', 'Pc', 'Wap', 'Unknown')
                                    AND attr_model = 'lt_1d_p' THEN a2c
                                    ELSE 0
                                END
                            ) AS a2c,
                            SUM(
                                CASE
                                    WHEN platform IN ('App')
                                    AND attr_model = 'lt_1d_p' THEN a2c
                                    ELSE 0
                                END
                            ) AS app_a2c,
                            SUM(
                                CASE
                                    WHEN platform IN ('App', 'Pc', 'Wap', 'Unknown')
                                    AND attr_model = 'lt_1d_p' THEN a2c_uv
                                    ELSE 0
                                END
                            ) AS a2c_uv,
                            SUM(
                                CASE
                                    WHEN platform IN ('App')
                                    AND attr_model = 'lt_1d_p' THEN a2c_uv
                                    ELSE 0
                                END
                            ) AS app_a2c_uv,
                            SUM(
                                CASE
                                    WHEN platform IN ('App', 'Pc', 'Wap', 'Unknown')
                                    AND attr_model = 'lt_1d_p' THEN buyers
                                    ELSE 0
                                END
                            ) AS buyers,
                            SUM(
                                CASE
                                    WHEN platform IN ('App')
                                    AND attr_model = 'lt_1d_p' THEN buyers
                                    ELSE 0
                                END
                            ) AS app_buyers,
                            SUM(
                                CASE
                                    WHEN platform IN ('App', 'Pc', 'Wap', 'Unknown')
                                    AND attr_model = 'lt_1d_p' THEN new_buyers
                                    ELSE 0
                                END
                            ) AS new_buyers,
                            SUM(
                                CASE
                                    WHEN platform IN ('App')
                                    AND attr_model = 'lt_1d_p' THEN new_buyers
                                    ELSE 0
                                END
                            ) AS app_new_buyers,
                            SUM(
                                CASE
                                    WHEN platform IN ('App', 'Pc', 'Wap', 'Unknown')
                                    AND attr_model = 'lt_1d_p' THEN reacquired_buyers
                                    ELSE 0
                                END
                            ) AS reacquired_buyers,
                            SUM(
                                CASE
                                    WHEN platform IN ('App')
                                    AND attr_model = 'lt_1d_p' THEN reacquired_buyers
                                    ELSE 0
                                END
                            ) AS app_reacquired_buyers,
                            SUM(
                                CASE
                                    WHEN platform IN ('App', 'Pc', 'Wap', 'Unknown')
                                    AND attr_model = 'lt_1d_p' THEN sales_order_id_count
                                    ELSE 0
                                END
                            ) AS sales_order_id_count,
                            SUM(
                                CASE
                                    WHEN platform IN ('App')
                                    AND attr_model = 'lt_1d_p' THEN sales_order_id_count
                                    ELSE 0
                                END
                            ) AS app_sales_order_id_count,
                            SUM(
                                CASE
                                    WHEN platform IN ('App', 'Pc', 'Wap', 'Unknown')
                                    AND attr_model = 'lt_1d_p' THEN order_id_count
                                    ELSE 0
                                END
                            ) AS order_id_count,
                            SUM(
                                CASE
                                    WHEN platform IN ('App')
                                    AND attr_model = 'lt_1d_p' THEN order_id_count
                                    ELSE 0
                                END
                            ) AS app_order_id_count,
                            SUM(
                                CASE
                                    WHEN platform IN ('App', 'Pc', 'Wap', 'Unknown')
                                    AND attr_model = 'lt_1d_p' THEN gmv
                                    ELSE 0
                                END
                            ) AS gmv,
                            SUM(
                                CASE
                                    WHEN platform IN ('App')
                                    AND attr_model = 'lt_1d_p' THEN gmv
                                    ELSE 0
                                END
                            ) AS app_gmv
                        FROM lazada_ads.ads_lzd_mkt_rt_uam_campaign_hh
                        WHERE 1 = 1
                            AND funding_bucket IN ('Retail Affiliates')
                            AND campaign LIKE '%VN%'
                            AND attr_model IN ('ft_1d_np', 'lt_1d', 'lt_1d_p')
                            AND venture = 'VN'
                            AND ds BETWEEN TO_CHAR(DATEADD(GETDATE(), -2, 'dd'), 'yyyymmdd') AND TO_CHAR(DATEADD(GETDATE(), 0, 'dd'), 'yyyymmdd')
                        GROUP BY ds,
                            hh,
                            channel,
                            sub_channel,
                            rt_audience,
                            campaign GROUPING SETS ((ds, hh, campaign))
                    )
                GROUP BY ds,
                    hh,
                    channel,
                    sub_channel,
                    rt_audience,
                    campaign GROUPING SETS ((ds, hh, channel, sub_channel, rt_audience))
            )
        GROUP BY ds,
            hh,
            channel,
            sub_channel,
            rt_audience,
            campaign
    )
GROUP BY ds,
    hh,
    channel,
    sub_channel,
    rt_audience,
    campaign;
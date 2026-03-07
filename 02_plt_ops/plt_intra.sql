--odps sql 
--********************************************************************--
--author:Truong, Van Thanh
--create time:2023-01-19 17:03:16
--********************************************************************--
--@@ Input = lazada_ads.ads_lzd_mkt_rt_uam_campaign_hi
--********************************************************************-- Merge table: channel 1st, metrics total 2nd, metrics app 3rd
SELECT CONCAT(
        SUBSTR(ds, 1, 4),
        '-',
        SUBSTR(ds, 5, 2),
        '-',
        SUBSTR(ds, 7, 2)
    ) AS ds,
    hh,
    venture,
    COALESCE(
        SUM(
            CASE
                WHEN channel IN ('total') THEN installs
                ELSE 0
            END
        ),
        0
    ) AS total_installs --<< Start installs, app_installs
,
    COALESCE(
        SUM(
            CASE
                WHEN channel IN ('total') THEN app_installs
                ELSE 0
            END
        ),
        0
    ) AS total_app_installs,
    COALESCE(
        SUM(
            CASE
                WHEN channel IN ('organic') THEN installs
                ELSE 0
            END
        ),
        0
    ) AS organic_installs,
    COALESCE(
        SUM(
            CASE
                WHEN channel IN ('organic') THEN app_installs
                ELSE 0
            END
        ),
        0
    ) AS organic_app_installs,
    COALESCE(
        SUM(
            CASE
                WHEN channel IN ('mkt') THEN installs
                ELSE 0
            END
        ),
        0
    ) AS mkt_installs,
    COALESCE(
        SUM(
            CASE
                WHEN channel IN ('mkt') THEN app_installs
                ELSE 0
            END
        ),
        0
    ) AS mkt_app_installs,
    COALESCE(
        SUM(
            CASE
                WHEN channel IN ('other_mkt') THEN installs
                ELSE 0
            END
        ),
        0
    ) AS other_mkt_installs,
    COALESCE(
        SUM(
            CASE
                WHEN channel IN ('other_mkt') THEN app_installs
                ELSE 0
            END
        ),
        0
    ) AS other_mkt_app_installs,
    COALESCE(
        SUM(
            CASE
                WHEN channel IN ('crm') THEN installs
                ELSE 0
            END
        ),
        0
    ) AS crm_installs,
    COALESCE(
        SUM(
            CASE
                WHEN channel IN ('crm') THEN app_installs
                ELSE 0
            END
        ),
        0
    ) AS crm_app_installs,
    COALESCE(
        SUM(
            CASE
                WHEN channel IN ('om') THEN installs
                ELSE 0
            END
        ),
        0
    ) AS om_installs,
    COALESCE(
        SUM(
            CASE
                WHEN channel IN ('om') THEN app_installs
                ELSE 0
            END
        ),
        0
    ) AS om_app_installs,
    COALESCE(
        SUM(
            CASE
                WHEN channel IN ('ppc') THEN installs
                ELSE 0
            END
        ),
        0
    ) AS ppc_installs,
    COALESCE(
        SUM(
            CASE
                WHEN channel IN ('ppc') THEN app_installs
                ELSE 0
            END
        ),
        0
    ) AS ppc_app_installs,
    COALESCE(
        SUM(
            CASE
                WHEN channel IN ('affiliate') THEN installs
                ELSE 0
            END
        ),
        0
    ) AS affiliate_installs,
    COALESCE(
        SUM(
            CASE
                WHEN channel IN ('affiliate') THEN app_installs
                ELSE 0
            END
        ),
        0
    ) AS affiliate_app_installs,
    COALESCE(
        SUM(
            CASE
                WHEN channel IN ('rta') THEN installs
                ELSE 0
            END
        ),
        0
    ) AS rta_installs,
    COALESCE(
        SUM(
            CASE
                WHEN channel IN ('rta') THEN app_installs
                ELSE 0
            END
        ),
        0
    ) AS rta_app_installs --<< End
,
    COALESCE(
        SUM(
            CASE
                WHEN channel IN ('total') THEN reinstalls
                ELSE 0
            END
        ),
        0
    ) AS total_reinstalls --<< Start rereinstalls, app_rereinstalls
,
    COALESCE(
        SUM(
            CASE
                WHEN channel IN ('total') THEN app_reinstalls
                ELSE 0
            END
        ),
        0
    ) AS total_app_reinstalls,
    COALESCE(
        SUM(
            CASE
                WHEN channel IN ('organic') THEN reinstalls
                ELSE 0
            END
        ),
        0
    ) AS organic_reinstalls,
    COALESCE(
        SUM(
            CASE
                WHEN channel IN ('organic') THEN app_reinstalls
                ELSE 0
            END
        ),
        0
    ) AS organic_app_reinstalls,
    COALESCE(
        SUM(
            CASE
                WHEN channel IN ('mkt') THEN reinstalls
                ELSE 0
            END
        ),
        0
    ) AS mkt_reinstalls,
    COALESCE(
        SUM(
            CASE
                WHEN channel IN ('mkt') THEN app_reinstalls
                ELSE 0
            END
        ),
        0
    ) AS mkt_app_reinstalls,
    COALESCE(
        SUM(
            CASE
                WHEN channel IN ('other_mkt') THEN reinstalls
                ELSE 0
            END
        ),
        0
    ) AS other_mkt_reinstalls,
    COALESCE(
        SUM(
            CASE
                WHEN channel IN ('other_mkt') THEN app_reinstalls
                ELSE 0
            END
        ),
        0
    ) AS other_mkt_app_reinstalls,
    COALESCE(
        SUM(
            CASE
                WHEN channel IN ('crm') THEN reinstalls
                ELSE 0
            END
        ),
        0
    ) AS crm_reinstalls,
    COALESCE(
        SUM(
            CASE
                WHEN channel IN ('crm') THEN app_reinstalls
                ELSE 0
            END
        ),
        0
    ) AS crm_app_reinstalls,
    COALESCE(
        SUM(
            CASE
                WHEN channel IN ('om') THEN reinstalls
                ELSE 0
            END
        ),
        0
    ) AS om_reinstalls,
    COALESCE(
        SUM(
            CASE
                WHEN channel IN ('om') THEN app_reinstalls
                ELSE 0
            END
        ),
        0
    ) AS om_app_reinstalls,
    COALESCE(
        SUM(
            CASE
                WHEN channel IN ('ppc') THEN reinstalls
                ELSE 0
            END
        ),
        0
    ) AS ppc_reinstalls,
    COALESCE(
        SUM(
            CASE
                WHEN channel IN ('ppc') THEN app_reinstalls
                ELSE 0
            END
        ),
        0
    ) AS ppc_app_reinstalls,
    COALESCE(
        SUM(
            CASE
                WHEN channel IN ('affiliate') THEN reinstalls
                ELSE 0
            END
        ),
        0
    ) AS affiliate_reinstalls,
    COALESCE(
        SUM(
            CASE
                WHEN channel IN ('affiliate') THEN app_reinstalls
                ELSE 0
            END
        ),
        0
    ) AS affiliate_app_reinstalls,
    COALESCE(
        SUM(
            CASE
                WHEN channel IN ('rta') THEN reinstalls
                ELSE 0
            END
        ),
        0
    ) AS rta_reinstalls,
    COALESCE(
        SUM(
            CASE
                WHEN channel IN ('rta') THEN app_reinstalls
                ELSE 0
            END
        ),
        0
    ) AS rta_app_reinstalls --<< End
,
    COALESCE(
        SUM(
            CASE
                WHEN channel IN ('total') THEN visits
                ELSE 0
            END
        ),
        0
    ) AS total_visits --<< Start visits, app_visits
,
    COALESCE(
        SUM(
            CASE
                WHEN channel IN ('total') THEN app_visits
                ELSE 0
            END
        ),
        0
    ) AS total_app_visits,
    COALESCE(
        SUM(
            CASE
                WHEN channel IN ('organic') THEN visits
                ELSE 0
            END
        ),
        0
    ) AS organic_visits,
    COALESCE(
        SUM(
            CASE
                WHEN channel IN ('organic') THEN app_visits
                ELSE 0
            END
        ),
        0
    ) AS organic_app_visits,
    COALESCE(
        SUM(
            CASE
                WHEN channel IN ('mkt') THEN visits
                ELSE 0
            END
        ),
        0
    ) AS mkt_visits,
    COALESCE(
        SUM(
            CASE
                WHEN channel IN ('mkt') THEN app_visits
                ELSE 0
            END
        ),
        0
    ) AS mkt_app_visits,
    COALESCE(
        SUM(
            CASE
                WHEN channel IN ('other_mkt') THEN visits
                ELSE 0
            END
        ),
        0
    ) AS other_mkt_visits,
    COALESCE(
        SUM(
            CASE
                WHEN channel IN ('other_mkt') THEN app_visits
                ELSE 0
            END
        ),
        0
    ) AS other_mkt_app_visits,
    COALESCE(
        SUM(
            CASE
                WHEN channel IN ('crm') THEN visits
                ELSE 0
            END
        ),
        0
    ) AS crm_visits,
    COALESCE(
        SUM(
            CASE
                WHEN channel IN ('crm') THEN app_visits
                ELSE 0
            END
        ),
        0
    ) AS crm_app_visits,
    COALESCE(
        SUM(
            CASE
                WHEN channel IN ('om') THEN visits
                ELSE 0
            END
        ),
        0
    ) AS om_visits,
    COALESCE(
        SUM(
            CASE
                WHEN channel IN ('om') THEN app_visits
                ELSE 0
            END
        ),
        0
    ) AS om_app_visits,
    COALESCE(
        SUM(
            CASE
                WHEN channel IN ('ppc') THEN visits
                ELSE 0
            END
        ),
        0
    ) AS ppc_visits,
    COALESCE(
        SUM(
            CASE
                WHEN channel IN ('ppc') THEN app_visits
                ELSE 0
            END
        ),
        0
    ) AS ppc_app_visits,
    COALESCE(
        SUM(
            CASE
                WHEN channel IN ('affiliate') THEN visits
                ELSE 0
            END
        ),
        0
    ) AS affiliate_visits,
    COALESCE(
        SUM(
            CASE
                WHEN channel IN ('affiliate') THEN app_visits
                ELSE 0
            END
        ),
        0
    ) AS affiliate_app_visits,
    COALESCE(
        SUM(
            CASE
                WHEN channel IN ('rta') THEN visits
                ELSE 0
            END
        ),
        0
    ) AS rta_visits,
    COALESCE(
        SUM(
            CASE
                WHEN channel IN ('rta') THEN app_visits
                ELSE 0
            END
        ),
        0
    ) AS rta_app_visits --<< End
,
    COALESCE(
        SUM(
            CASE
                WHEN channel IN ('total') THEN dau
                ELSE 0
            END
        ),
        0
    ) AS total_dau --<< Start dau, app_dau
,
    COALESCE(
        SUM(
            CASE
                WHEN channel IN ('total') THEN app_dau
                ELSE 0
            END
        ),
        0
    ) AS total_app_dau,
    COALESCE(
        SUM(
            CASE
                WHEN channel IN ('organic') THEN dau
                ELSE 0
            END
        ),
        0
    ) AS organic_dau,
    COALESCE(
        SUM(
            CASE
                WHEN channel IN ('organic') THEN app_dau
                ELSE 0
            END
        ),
        0
    ) AS organic_app_dau,
    COALESCE(
        SUM(
            CASE
                WHEN channel IN ('mkt') THEN dau
                ELSE 0
            END
        ),
        0
    ) AS mkt_dau,
    COALESCE(
        SUM(
            CASE
                WHEN channel IN ('mkt') THEN app_dau
                ELSE 0
            END
        ),
        0
    ) AS mkt_app_dau,
    COALESCE(
        SUM(
            CASE
                WHEN channel IN ('other_mkt') THEN dau
                ELSE 0
            END
        ),
        0
    ) AS other_mkt_dau,
    COALESCE(
        SUM(
            CASE
                WHEN channel IN ('other_mkt') THEN app_dau
                ELSE 0
            END
        ),
        0
    ) AS other_mkt_app_dau,
    COALESCE(
        SUM(
            CASE
                WHEN channel IN ('crm') THEN dau
                ELSE 0
            END
        ),
        0
    ) AS crm_dau,
    COALESCE(
        SUM(
            CASE
                WHEN channel IN ('crm') THEN app_dau
                ELSE 0
            END
        ),
        0
    ) AS crm_app_dau,
    COALESCE(
        SUM(
            CASE
                WHEN channel IN ('om') THEN dau
                ELSE 0
            END
        ),
        0
    ) AS om_dau,
    COALESCE(
        SUM(
            CASE
                WHEN channel IN ('om') THEN app_dau
                ELSE 0
            END
        ),
        0
    ) AS om_app_dau,
    COALESCE(
        SUM(
            CASE
                WHEN channel IN ('ppc') THEN dau
                ELSE 0
            END
        ),
        0
    ) AS ppc_dau,
    COALESCE(
        SUM(
            CASE
                WHEN channel IN ('ppc') THEN app_dau
                ELSE 0
            END
        ),
        0
    ) AS ppc_app_dau,
    COALESCE(
        SUM(
            CASE
                WHEN channel IN ('affiliate') THEN dau
                ELSE 0
            END
        ),
        0
    ) AS affiliate_dau,
    COALESCE(
        SUM(
            CASE
                WHEN channel IN ('affiliate') THEN app_dau
                ELSE 0
            END
        ),
        0
    ) AS affiliate_app_dau,
    COALESCE(
        SUM(
            CASE
                WHEN channel IN ('rta') THEN dau
                ELSE 0
            END
        ),
        0
    ) AS rta_dau,
    COALESCE(
        SUM(
            CASE
                WHEN channel IN ('rta') THEN app_dau
                ELSE 0
            END
        ),
        0
    ) AS rta_app_dau --<< End
,
    COALESCE(
        SUM(
            CASE
                WHEN channel IN ('total') THEN pdp_pv
                ELSE 0
            END
        ),
        0
    ) AS total_pdp_pv --<< Start pdp_pv, app_pdp_pv
,
    COALESCE(
        SUM(
            CASE
                WHEN channel IN ('total') THEN app_pdp_pv
                ELSE 0
            END
        ),
        0
    ) AS total_app_pdp_pv,
    COALESCE(
        SUM(
            CASE
                WHEN channel IN ('organic') THEN pdp_pv
                ELSE 0
            END
        ),
        0
    ) AS organic_pdp_pv,
    COALESCE(
        SUM(
            CASE
                WHEN channel IN ('organic') THEN app_pdp_pv
                ELSE 0
            END
        ),
        0
    ) AS organic_app_pdp_pv,
    COALESCE(
        SUM(
            CASE
                WHEN channel IN ('mkt') THEN pdp_pv
                ELSE 0
            END
        ),
        0
    ) AS mkt_pdp_pv,
    COALESCE(
        SUM(
            CASE
                WHEN channel IN ('mkt') THEN app_pdp_pv
                ELSE 0
            END
        ),
        0
    ) AS mkt_app_pdp_pv,
    COALESCE(
        SUM(
            CASE
                WHEN channel IN ('other_mkt') THEN pdp_pv
                ELSE 0
            END
        ),
        0
    ) AS other_mkt_pdp_pv,
    COALESCE(
        SUM(
            CASE
                WHEN channel IN ('other_mkt') THEN app_pdp_pv
                ELSE 0
            END
        ),
        0
    ) AS other_mkt_app_pdp_pv,
    COALESCE(
        SUM(
            CASE
                WHEN channel IN ('crm') THEN pdp_pv
                ELSE 0
            END
        ),
        0
    ) AS crm_pdp_pv,
    COALESCE(
        SUM(
            CASE
                WHEN channel IN ('crm') THEN app_pdp_pv
                ELSE 0
            END
        ),
        0
    ) AS crm_app_pdp_pv,
    COALESCE(
        SUM(
            CASE
                WHEN channel IN ('om') THEN pdp_pv
                ELSE 0
            END
        ),
        0
    ) AS om_pdp_pv,
    COALESCE(
        SUM(
            CASE
                WHEN channel IN ('om') THEN app_pdp_pv
                ELSE 0
            END
        ),
        0
    ) AS om_app_pdp_pv,
    COALESCE(
        SUM(
            CASE
                WHEN channel IN ('ppc') THEN pdp_pv
                ELSE 0
            END
        ),
        0
    ) AS ppc_pdp_pv,
    COALESCE(
        SUM(
            CASE
                WHEN channel IN ('ppc') THEN app_pdp_pv
                ELSE 0
            END
        ),
        0
    ) AS ppc_app_pdp_pv,
    COALESCE(
        SUM(
            CASE
                WHEN channel IN ('affiliate') THEN pdp_pv
                ELSE 0
            END
        ),
        0
    ) AS affiliate_pdp_pv,
    COALESCE(
        SUM(
            CASE
                WHEN channel IN ('affiliate') THEN app_pdp_pv
                ELSE 0
            END
        ),
        0
    ) AS affiliate_app_pdp_pv,
    COALESCE(
        SUM(
            CASE
                WHEN channel IN ('rta') THEN pdp_pv
                ELSE 0
            END
        ),
        0
    ) AS rta_pdp_pv,
    COALESCE(
        SUM(
            CASE
                WHEN channel IN ('rta') THEN app_pdp_pv
                ELSE 0
            END
        ),
        0
    ) AS rta_app_pdp_pv --<< End
,
    COALESCE(
        SUM(
            CASE
                WHEN channel IN ('total') THEN pdp_uv
                ELSE 0
            END
        ),
        0
    ) AS total_pdp_uv --<< Start pdp_uv, app_pdp_uv
,
    COALESCE(
        SUM(
            CASE
                WHEN channel IN ('total') THEN app_pdp_uv
                ELSE 0
            END
        ),
        0
    ) AS total_app_pdp_uv,
    COALESCE(
        SUM(
            CASE
                WHEN channel IN ('organic') THEN pdp_uv
                ELSE 0
            END
        ),
        0
    ) AS organic_pdp_uv,
    COALESCE(
        SUM(
            CASE
                WHEN channel IN ('organic') THEN app_pdp_uv
                ELSE 0
            END
        ),
        0
    ) AS organic_app_pdp_uv,
    COALESCE(
        SUM(
            CASE
                WHEN channel IN ('mkt') THEN pdp_uv
                ELSE 0
            END
        ),
        0
    ) AS mkt_pdp_uv,
    COALESCE(
        SUM(
            CASE
                WHEN channel IN ('mkt') THEN app_pdp_uv
                ELSE 0
            END
        ),
        0
    ) AS mkt_app_pdp_uv,
    COALESCE(
        SUM(
            CASE
                WHEN channel IN ('other_mkt') THEN pdp_uv
                ELSE 0
            END
        ),
        0
    ) AS other_mkt_pdp_uv,
    COALESCE(
        SUM(
            CASE
                WHEN channel IN ('other_mkt') THEN app_pdp_uv
                ELSE 0
            END
        ),
        0
    ) AS other_mkt_app_pdp_uv,
    COALESCE(
        SUM(
            CASE
                WHEN channel IN ('crm') THEN pdp_uv
                ELSE 0
            END
        ),
        0
    ) AS crm_pdp_uv,
    COALESCE(
        SUM(
            CASE
                WHEN channel IN ('crm') THEN app_pdp_uv
                ELSE 0
            END
        ),
        0
    ) AS crm_app_pdp_uv,
    COALESCE(
        SUM(
            CASE
                WHEN channel IN ('om') THEN pdp_uv
                ELSE 0
            END
        ),
        0
    ) AS om_pdp_uv,
    COALESCE(
        SUM(
            CASE
                WHEN channel IN ('om') THEN app_pdp_uv
                ELSE 0
            END
        ),
        0
    ) AS om_app_pdp_uv,
    COALESCE(
        SUM(
            CASE
                WHEN channel IN ('ppc') THEN pdp_uv
                ELSE 0
            END
        ),
        0
    ) AS ppc_pdp_uv,
    COALESCE(
        SUM(
            CASE
                WHEN channel IN ('ppc') THEN app_pdp_uv
                ELSE 0
            END
        ),
        0
    ) AS ppc_app_pdp_uv,
    COALESCE(
        SUM(
            CASE
                WHEN channel IN ('affiliate') THEN pdp_uv
                ELSE 0
            END
        ),
        0
    ) AS affiliate_pdp_uv,
    COALESCE(
        SUM(
            CASE
                WHEN channel IN ('affiliate') THEN app_pdp_uv
                ELSE 0
            END
        ),
        0
    ) AS affiliate_app_pdp_uv,
    COALESCE(
        SUM(
            CASE
                WHEN channel IN ('rta') THEN pdp_uv
                ELSE 0
            END
        ),
        0
    ) AS rta_pdp_uv,
    COALESCE(
        SUM(
            CASE
                WHEN channel IN ('rta') THEN app_pdp_uv
                ELSE 0
            END
        ),
        0
    ) AS rta_app_pdp_uv --<< End
,
    COALESCE(
        SUM(
            CASE
                WHEN channel IN ('total') THEN a2c
                ELSE 0
            END
        ),
        0
    ) AS total_a2c --<< Start a2c, app_a2c
,
    COALESCE(
        SUM(
            CASE
                WHEN channel IN ('total') THEN app_a2c
                ELSE 0
            END
        ),
        0
    ) AS total_app_a2c,
    COALESCE(
        SUM(
            CASE
                WHEN channel IN ('organic') THEN a2c
                ELSE 0
            END
        ),
        0
    ) AS organic_a2c,
    COALESCE(
        SUM(
            CASE
                WHEN channel IN ('organic') THEN app_a2c
                ELSE 0
            END
        ),
        0
    ) AS organic_app_a2c,
    COALESCE(
        SUM(
            CASE
                WHEN channel IN ('mkt') THEN a2c
                ELSE 0
            END
        ),
        0
    ) AS mkt_a2c,
    COALESCE(
        SUM(
            CASE
                WHEN channel IN ('mkt') THEN app_a2c
                ELSE 0
            END
        ),
        0
    ) AS mkt_app_a2c,
    COALESCE(
        SUM(
            CASE
                WHEN channel IN ('other_mkt') THEN a2c
                ELSE 0
            END
        ),
        0
    ) AS other_mkt_a2c,
    COALESCE(
        SUM(
            CASE
                WHEN channel IN ('other_mkt') THEN app_a2c
                ELSE 0
            END
        ),
        0
    ) AS other_mkt_app_a2c,
    COALESCE(
        SUM(
            CASE
                WHEN channel IN ('crm') THEN a2c
                ELSE 0
            END
        ),
        0
    ) AS crm_a2c,
    COALESCE(
        SUM(
            CASE
                WHEN channel IN ('crm') THEN app_a2c
                ELSE 0
            END
        ),
        0
    ) AS crm_app_a2c,
    COALESCE(
        SUM(
            CASE
                WHEN channel IN ('om') THEN a2c
                ELSE 0
            END
        ),
        0
    ) AS om_a2c,
    COALESCE(
        SUM(
            CASE
                WHEN channel IN ('om') THEN app_a2c
                ELSE 0
            END
        ),
        0
    ) AS om_app_a2c,
    COALESCE(
        SUM(
            CASE
                WHEN channel IN ('ppc') THEN a2c
                ELSE 0
            END
        ),
        0
    ) AS ppc_a2c,
    COALESCE(
        SUM(
            CASE
                WHEN channel IN ('ppc') THEN app_a2c
                ELSE 0
            END
        ),
        0
    ) AS ppc_app_a2c,
    COALESCE(
        SUM(
            CASE
                WHEN channel IN ('affiliate') THEN a2c
                ELSE 0
            END
        ),
        0
    ) AS affiliate_a2c,
    COALESCE(
        SUM(
            CASE
                WHEN channel IN ('affiliate') THEN app_a2c
                ELSE 0
            END
        ),
        0
    ) AS affiliate_app_a2c,
    COALESCE(
        SUM(
            CASE
                WHEN channel IN ('rta') THEN a2c
                ELSE 0
            END
        ),
        0
    ) AS rta_a2c,
    COALESCE(
        SUM(
            CASE
                WHEN channel IN ('rta') THEN app_a2c
                ELSE 0
            END
        ),
        0
    ) AS rta_app_a2c --<< End
,
    COALESCE(
        SUM(
            CASE
                WHEN channel IN ('total') THEN a2c_uv
                ELSE 0
            END
        ),
        0
    ) AS total_a2c_uv --<< Start a2c_uv, app_a2c_uv
,
    COALESCE(
        SUM(
            CASE
                WHEN channel IN ('total') THEN app_a2c_uv
                ELSE 0
            END
        ),
        0
    ) AS total_app_a2c_uv,
    COALESCE(
        SUM(
            CASE
                WHEN channel IN ('organic') THEN a2c_uv
                ELSE 0
            END
        ),
        0
    ) AS organic_a2c_uv,
    COALESCE(
        SUM(
            CASE
                WHEN channel IN ('organic') THEN app_a2c_uv
                ELSE 0
            END
        ),
        0
    ) AS organic_app_a2c_uv,
    COALESCE(
        SUM(
            CASE
                WHEN channel IN ('mkt') THEN a2c_uv
                ELSE 0
            END
        ),
        0
    ) AS mkt_a2c_uv,
    COALESCE(
        SUM(
            CASE
                WHEN channel IN ('mkt') THEN app_a2c_uv
                ELSE 0
            END
        ),
        0
    ) AS mkt_app_a2c_uv,
    COALESCE(
        SUM(
            CASE
                WHEN channel IN ('other_mkt') THEN a2c_uv
                ELSE 0
            END
        ),
        0
    ) AS other_mkt_a2c_uv,
    COALESCE(
        SUM(
            CASE
                WHEN channel IN ('other_mkt') THEN app_a2c_uv
                ELSE 0
            END
        ),
        0
    ) AS other_mkt_app_a2c_uv,
    COALESCE(
        SUM(
            CASE
                WHEN channel IN ('crm') THEN a2c_uv
                ELSE 0
            END
        ),
        0
    ) AS crm_a2c_uv,
    COALESCE(
        SUM(
            CASE
                WHEN channel IN ('crm') THEN app_a2c_uv
                ELSE 0
            END
        ),
        0
    ) AS crm_app_a2c_uv,
    COALESCE(
        SUM(
            CASE
                WHEN channel IN ('om') THEN a2c_uv
                ELSE 0
            END
        ),
        0
    ) AS om_a2c_uv,
    COALESCE(
        SUM(
            CASE
                WHEN channel IN ('om') THEN app_a2c_uv
                ELSE 0
            END
        ),
        0
    ) AS om_app_a2c_uv,
    COALESCE(
        SUM(
            CASE
                WHEN channel IN ('ppc') THEN a2c_uv
                ELSE 0
            END
        ),
        0
    ) AS ppc_a2c_uv,
    COALESCE(
        SUM(
            CASE
                WHEN channel IN ('ppc') THEN app_a2c_uv
                ELSE 0
            END
        ),
        0
    ) AS ppc_app_a2c_uv,
    COALESCE(
        SUM(
            CASE
                WHEN channel IN ('affiliate') THEN a2c_uv
                ELSE 0
            END
        ),
        0
    ) AS affiliate_a2c_uv,
    COALESCE(
        SUM(
            CASE
                WHEN channel IN ('affiliate') THEN app_a2c_uv
                ELSE 0
            END
        ),
        0
    ) AS affiliate_app_a2c_uv,
    COALESCE(
        SUM(
            CASE
                WHEN channel IN ('rta') THEN a2c_uv
                ELSE 0
            END
        ),
        0
    ) AS rta_a2c_uv,
    COALESCE(
        SUM(
            CASE
                WHEN channel IN ('rta') THEN app_a2c_uv
                ELSE 0
            END
        ),
        0
    ) AS rta_app_a2c_uv --<< End
,
    COALESCE(
        SUM(
            CASE
                WHEN channel IN ('total') THEN buyers
                ELSE 0
            END
        ),
        0
    ) AS total_buyers --<< Start buyers, app_buyers
,
    COALESCE(
        SUM(
            CASE
                WHEN channel IN ('total') THEN app_buyers
                ELSE 0
            END
        ),
        0
    ) AS total_app_buyers,
    COALESCE(
        SUM(
            CASE
                WHEN channel IN ('organic') THEN buyers
                ELSE 0
            END
        ),
        0
    ) AS organic_buyers,
    COALESCE(
        SUM(
            CASE
                WHEN channel IN ('organic') THEN app_buyers
                ELSE 0
            END
        ),
        0
    ) AS organic_app_buyers,
    COALESCE(
        SUM(
            CASE
                WHEN channel IN ('mkt') THEN buyers
                ELSE 0
            END
        ),
        0
    ) AS mkt_buyers,
    COALESCE(
        SUM(
            CASE
                WHEN channel IN ('mkt') THEN app_buyers
                ELSE 0
            END
        ),
        0
    ) AS mkt_app_buyers,
    COALESCE(
        SUM(
            CASE
                WHEN channel IN ('other_mkt') THEN buyers
                ELSE 0
            END
        ),
        0
    ) AS other_mkt_buyers,
    COALESCE(
        SUM(
            CASE
                WHEN channel IN ('other_mkt') THEN app_buyers
                ELSE 0
            END
        ),
        0
    ) AS other_mkt_app_buyers,
    COALESCE(
        SUM(
            CASE
                WHEN channel IN ('crm') THEN buyers
                ELSE 0
            END
        ),
        0
    ) AS crm_buyers,
    COALESCE(
        SUM(
            CASE
                WHEN channel IN ('crm') THEN app_buyers
                ELSE 0
            END
        ),
        0
    ) AS crm_app_buyers,
    COALESCE(
        SUM(
            CASE
                WHEN channel IN ('om') THEN buyers
                ELSE 0
            END
        ),
        0
    ) AS om_buyers,
    COALESCE(
        SUM(
            CASE
                WHEN channel IN ('om') THEN app_buyers
                ELSE 0
            END
        ),
        0
    ) AS om_app_buyers,
    COALESCE(
        SUM(
            CASE
                WHEN channel IN ('ppc') THEN buyers
                ELSE 0
            END
        ),
        0
    ) AS ppc_buyers,
    COALESCE(
        SUM(
            CASE
                WHEN channel IN ('ppc') THEN app_buyers
                ELSE 0
            END
        ),
        0
    ) AS ppc_app_buyers,
    COALESCE(
        SUM(
            CASE
                WHEN channel IN ('affiliate') THEN buyers
                ELSE 0
            END
        ),
        0
    ) AS affiliate_buyers,
    COALESCE(
        SUM(
            CASE
                WHEN channel IN ('affiliate') THEN app_buyers
                ELSE 0
            END
        ),
        0
    ) AS affiliate_app_buyers,
    COALESCE(
        SUM(
            CASE
                WHEN channel IN ('rta') THEN buyers
                ELSE 0
            END
        ),
        0
    ) AS rta_buyers,
    COALESCE(
        SUM(
            CASE
                WHEN channel IN ('rta') THEN app_buyers
                ELSE 0
            END
        ),
        0
    ) AS rta_app_buyers --<< End
,
    COALESCE(
        SUM(
            CASE
                WHEN channel IN ('total') THEN new_buyers
                ELSE 0
            END
        ),
        0
    ) AS total_new_buyers --<< Start new_buyers, app_new_buyers
,
    COALESCE(
        SUM(
            CASE
                WHEN channel IN ('total') THEN app_new_buyers
                ELSE 0
            END
        ),
        0
    ) AS total_app_new_buyers,
    COALESCE(
        SUM(
            CASE
                WHEN channel IN ('organic') THEN new_buyers
                ELSE 0
            END
        ),
        0
    ) AS organic_new_buyers,
    COALESCE(
        SUM(
            CASE
                WHEN channel IN ('organic') THEN app_new_buyers
                ELSE 0
            END
        ),
        0
    ) AS organic_app_new_buyers,
    COALESCE(
        SUM(
            CASE
                WHEN channel IN ('mkt') THEN new_buyers
                ELSE 0
            END
        ),
        0
    ) AS mkt_new_buyers,
    COALESCE(
        SUM(
            CASE
                WHEN channel IN ('mkt') THEN app_new_buyers
                ELSE 0
            END
        ),
        0
    ) AS mkt_app_new_buyers,
    COALESCE(
        SUM(
            CASE
                WHEN channel IN ('other_mkt') THEN new_buyers
                ELSE 0
            END
        ),
        0
    ) AS other_mkt_new_buyers,
    COALESCE(
        SUM(
            CASE
                WHEN channel IN ('other_mkt') THEN app_new_buyers
                ELSE 0
            END
        ),
        0
    ) AS other_mkt_app_new_buyers,
    COALESCE(
        SUM(
            CASE
                WHEN channel IN ('crm') THEN new_buyers
                ELSE 0
            END
        ),
        0
    ) AS crm_new_buyers,
    COALESCE(
        SUM(
            CASE
                WHEN channel IN ('crm') THEN app_new_buyers
                ELSE 0
            END
        ),
        0
    ) AS crm_app_new_buyers,
    COALESCE(
        SUM(
            CASE
                WHEN channel IN ('om') THEN new_buyers
                ELSE 0
            END
        ),
        0
    ) AS om_new_buyers,
    COALESCE(
        SUM(
            CASE
                WHEN channel IN ('om') THEN app_new_buyers
                ELSE 0
            END
        ),
        0
    ) AS om_app_new_buyers,
    COALESCE(
        SUM(
            CASE
                WHEN channel IN ('ppc') THEN new_buyers
                ELSE 0
            END
        ),
        0
    ) AS ppc_new_buyers,
    COALESCE(
        SUM(
            CASE
                WHEN channel IN ('ppc') THEN app_new_buyers
                ELSE 0
            END
        ),
        0
    ) AS ppc_app_new_buyers,
    COALESCE(
        SUM(
            CASE
                WHEN channel IN ('affiliate') THEN new_buyers
                ELSE 0
            END
        ),
        0
    ) AS affiliate_new_buyers,
    COALESCE(
        SUM(
            CASE
                WHEN channel IN ('affiliate') THEN app_new_buyers
                ELSE 0
            END
        ),
        0
    ) AS affiliate_app_new_buyers,
    COALESCE(
        SUM(
            CASE
                WHEN channel IN ('rta') THEN new_buyers
                ELSE 0
            END
        ),
        0
    ) AS rta_new_buyers,
    COALESCE(
        SUM(
            CASE
                WHEN channel IN ('rta') THEN app_new_buyers
                ELSE 0
            END
        ),
        0
    ) AS rta_app_new_buyers --<< End
,
    COALESCE(
        SUM(
            CASE
                WHEN channel IN ('total') THEN reacquired_buyers
                ELSE 0
            END
        ),
        0
    ) AS total_reacquired_buyers --<< Start reacquired_buyers, app_reacquired_buyers
,
    COALESCE(
        SUM(
            CASE
                WHEN channel IN ('total') THEN app_reacquired_buyers
                ELSE 0
            END
        ),
        0
    ) AS total_app_reacquired_buyers,
    COALESCE(
        SUM(
            CASE
                WHEN channel IN ('organic') THEN reacquired_buyers
                ELSE 0
            END
        ),
        0
    ) AS organic_reacquired_buyers,
    COALESCE(
        SUM(
            CASE
                WHEN channel IN ('organic') THEN app_reacquired_buyers
                ELSE 0
            END
        ),
        0
    ) AS organic_app_reacquired_buyers,
    COALESCE(
        SUM(
            CASE
                WHEN channel IN ('mkt') THEN reacquired_buyers
                ELSE 0
            END
        ),
        0
    ) AS mkt_reacquired_buyers,
    COALESCE(
        SUM(
            CASE
                WHEN channel IN ('mkt') THEN app_reacquired_buyers
                ELSE 0
            END
        ),
        0
    ) AS mkt_app_reacquired_buyers,
    COALESCE(
        SUM(
            CASE
                WHEN channel IN ('other_mkt') THEN reacquired_buyers
                ELSE 0
            END
        ),
        0
    ) AS other_mkt_reacquired_buyers,
    COALESCE(
        SUM(
            CASE
                WHEN channel IN ('other_mkt') THEN app_reacquired_buyers
                ELSE 0
            END
        ),
        0
    ) AS other_mkt_app_reacquired_buyers,
    COALESCE(
        SUM(
            CASE
                WHEN channel IN ('crm') THEN reacquired_buyers
                ELSE 0
            END
        ),
        0
    ) AS crm_reacquired_buyers,
    COALESCE(
        SUM(
            CASE
                WHEN channel IN ('crm') THEN app_reacquired_buyers
                ELSE 0
            END
        ),
        0
    ) AS crm_app_reacquired_buyers,
    COALESCE(
        SUM(
            CASE
                WHEN channel IN ('om') THEN reacquired_buyers
                ELSE 0
            END
        ),
        0
    ) AS om_reacquired_buyers,
    COALESCE(
        SUM(
            CASE
                WHEN channel IN ('om') THEN app_reacquired_buyers
                ELSE 0
            END
        ),
        0
    ) AS om_app_reacquired_buyers,
    COALESCE(
        SUM(
            CASE
                WHEN channel IN ('ppc') THEN reacquired_buyers
                ELSE 0
            END
        ),
        0
    ) AS ppc_reacquired_buyers,
    COALESCE(
        SUM(
            CASE
                WHEN channel IN ('ppc') THEN app_reacquired_buyers
                ELSE 0
            END
        ),
        0
    ) AS ppc_app_reacquired_buyers,
    COALESCE(
        SUM(
            CASE
                WHEN channel IN ('affiliate') THEN reacquired_buyers
                ELSE 0
            END
        ),
        0
    ) AS affiliate_reacquired_buyers,
    COALESCE(
        SUM(
            CASE
                WHEN channel IN ('affiliate') THEN app_reacquired_buyers
                ELSE 0
            END
        ),
        0
    ) AS affiliate_app_reacquired_buyers,
    COALESCE(
        SUM(
            CASE
                WHEN channel IN ('rta') THEN reacquired_buyers
                ELSE 0
            END
        ),
        0
    ) AS rta_reacquired_buyers,
    COALESCE(
        SUM(
            CASE
                WHEN channel IN ('rta') THEN app_reacquired_buyers
                ELSE 0
            END
        ),
        0
    ) AS rta_app_reacquired_buyers --<< End
,
    COALESCE(
        SUM(
            CASE
                WHEN channel IN ('total') THEN sales_order_id_count
                ELSE 0
            END
        ),
        0
    ) AS total_sales_order_id_count --<< Start sales_order_id_count, app_sales_order_id_count
,
    COALESCE(
        SUM(
            CASE
                WHEN channel IN ('total') THEN app_sales_order_id_count
                ELSE 0
            END
        ),
        0
    ) AS total_app_sales_order_id_count,
    COALESCE(
        SUM(
            CASE
                WHEN channel IN ('organic') THEN sales_order_id_count
                ELSE 0
            END
        ),
        0
    ) AS organic_sales_order_id_count,
    COALESCE(
        SUM(
            CASE
                WHEN channel IN ('organic') THEN app_sales_order_id_count
                ELSE 0
            END
        ),
        0
    ) AS organic_app_sales_order_id_count,
    COALESCE(
        SUM(
            CASE
                WHEN channel IN ('mkt') THEN sales_order_id_count
                ELSE 0
            END
        ),
        0
    ) AS mkt_sales_order_id_count,
    COALESCE(
        SUM(
            CASE
                WHEN channel IN ('mkt') THEN app_sales_order_id_count
                ELSE 0
            END
        ),
        0
    ) AS mkt_app_sales_order_id_count,
    COALESCE(
        SUM(
            CASE
                WHEN channel IN ('other_mkt') THEN sales_order_id_count
                ELSE 0
            END
        ),
        0
    ) AS other_mkt_sales_order_id_count,
    COALESCE(
        SUM(
            CASE
                WHEN channel IN ('other_mkt') THEN app_sales_order_id_count
                ELSE 0
            END
        ),
        0
    ) AS other_mkt_app_sales_order_id_count,
    COALESCE(
        SUM(
            CASE
                WHEN channel IN ('crm') THEN sales_order_id_count
                ELSE 0
            END
        ),
        0
    ) AS crm_sales_order_id_count,
    COALESCE(
        SUM(
            CASE
                WHEN channel IN ('crm') THEN app_sales_order_id_count
                ELSE 0
            END
        ),
        0
    ) AS crm_app_sales_order_id_count,
    COALESCE(
        SUM(
            CASE
                WHEN channel IN ('om') THEN sales_order_id_count
                ELSE 0
            END
        ),
        0
    ) AS om_sales_order_id_count,
    COALESCE(
        SUM(
            CASE
                WHEN channel IN ('om') THEN app_sales_order_id_count
                ELSE 0
            END
        ),
        0
    ) AS om_app_sales_order_id_count,
    COALESCE(
        SUM(
            CASE
                WHEN channel IN ('ppc') THEN sales_order_id_count
                ELSE 0
            END
        ),
        0
    ) AS ppc_sales_order_id_count,
    COALESCE(
        SUM(
            CASE
                WHEN channel IN ('ppc') THEN app_sales_order_id_count
                ELSE 0
            END
        ),
        0
    ) AS ppc_app_sales_order_id_count,
    COALESCE(
        SUM(
            CASE
                WHEN channel IN ('affiliate') THEN sales_order_id_count
                ELSE 0
            END
        ),
        0
    ) AS affiliate_sales_order_id_count,
    COALESCE(
        SUM(
            CASE
                WHEN channel IN ('affiliate') THEN app_sales_order_id_count
                ELSE 0
            END
        ),
        0
    ) AS affiliate_app_sales_order_id_count,
    COALESCE(
        SUM(
            CASE
                WHEN channel IN ('rta') THEN sales_order_id_count
                ELSE 0
            END
        ),
        0
    ) AS rta_sales_order_id_count,
    COALESCE(
        SUM(
            CASE
                WHEN channel IN ('rta') THEN app_sales_order_id_count
                ELSE 0
            END
        ),
        0
    ) AS rta_app_sales_order_id_count --<< End
,
    COALESCE(
        SUM(
            CASE
                WHEN channel IN ('total') THEN order_id_count
                ELSE 0
            END
        ),
        0
    ) AS total_order_id_count --<< Start order_id_count, app_order_id_count
,
    COALESCE(
        SUM(
            CASE
                WHEN channel IN ('total') THEN app_order_id_count
                ELSE 0
            END
        ),
        0
    ) AS total_app_order_id_count,
    COALESCE(
        SUM(
            CASE
                WHEN channel IN ('organic') THEN order_id_count
                ELSE 0
            END
        ),
        0
    ) AS organic_order_id_count,
    COALESCE(
        SUM(
            CASE
                WHEN channel IN ('organic') THEN app_order_id_count
                ELSE 0
            END
        ),
        0
    ) AS organic_app_order_id_count,
    COALESCE(
        SUM(
            CASE
                WHEN channel IN ('mkt') THEN order_id_count
                ELSE 0
            END
        ),
        0
    ) AS mkt_order_id_count,
    COALESCE(
        SUM(
            CASE
                WHEN channel IN ('mkt') THEN app_order_id_count
                ELSE 0
            END
        ),
        0
    ) AS mkt_app_order_id_count,
    COALESCE(
        SUM(
            CASE
                WHEN channel IN ('other_mkt') THEN order_id_count
                ELSE 0
            END
        ),
        0
    ) AS other_mkt_order_id_count,
    COALESCE(
        SUM(
            CASE
                WHEN channel IN ('other_mkt') THEN app_order_id_count
                ELSE 0
            END
        ),
        0
    ) AS other_mkt_app_order_id_count,
    COALESCE(
        SUM(
            CASE
                WHEN channel IN ('crm') THEN order_id_count
                ELSE 0
            END
        ),
        0
    ) AS crm_order_id_count,
    COALESCE(
        SUM(
            CASE
                WHEN channel IN ('crm') THEN app_order_id_count
                ELSE 0
            END
        ),
        0
    ) AS crm_app_order_id_count,
    COALESCE(
        SUM(
            CASE
                WHEN channel IN ('om') THEN order_id_count
                ELSE 0
            END
        ),
        0
    ) AS om_order_id_count,
    COALESCE(
        SUM(
            CASE
                WHEN channel IN ('om') THEN app_order_id_count
                ELSE 0
            END
        ),
        0
    ) AS om_app_order_id_count,
    COALESCE(
        SUM(
            CASE
                WHEN channel IN ('ppc') THEN order_id_count
                ELSE 0
            END
        ),
        0
    ) AS ppc_order_id_count,
    COALESCE(
        SUM(
            CASE
                WHEN channel IN ('ppc') THEN app_order_id_count
                ELSE 0
            END
        ),
        0
    ) AS ppc_app_order_id_count,
    COALESCE(
        SUM(
            CASE
                WHEN channel IN ('affiliate') THEN order_id_count
                ELSE 0
            END
        ),
        0
    ) AS affiliate_order_id_count,
    COALESCE(
        SUM(
            CASE
                WHEN channel IN ('affiliate') THEN app_order_id_count
                ELSE 0
            END
        ),
        0
    ) AS affiliate_app_order_id_count,
    COALESCE(
        SUM(
            CASE
                WHEN channel IN ('rta') THEN order_id_count
                ELSE 0
            END
        ),
        0
    ) AS rta_order_id_count,
    COALESCE(
        SUM(
            CASE
                WHEN channel IN ('rta') THEN app_order_id_count
                ELSE 0
            END
        ),
        0
    ) AS rta_app_order_id_count --<< End
,
    COALESCE(
        SUM(
            CASE
                WHEN channel IN ('total') THEN gmv
                ELSE 0
            END
        ),
        0
    ) AS total_gmv --<< Start gmv, app_gmv
,
    COALESCE(
        SUM(
            CASE
                WHEN channel IN ('total') THEN app_gmv
                ELSE 0
            END
        ),
        0
    ) AS total_app_gmv,
    COALESCE(
        SUM(
            CASE
                WHEN channel IN ('organic') THEN gmv
                ELSE 0
            END
        ),
        0
    ) AS organic_gmv,
    COALESCE(
        SUM(
            CASE
                WHEN channel IN ('organic') THEN app_gmv
                ELSE 0
            END
        ),
        0
    ) AS organic_app_gmv,
    COALESCE(
        SUM(
            CASE
                WHEN channel IN ('mkt') THEN gmv
                ELSE 0
            END
        ),
        0
    ) AS mkt_gmv,
    COALESCE(
        SUM(
            CASE
                WHEN channel IN ('mkt') THEN app_gmv
                ELSE 0
            END
        ),
        0
    ) AS mkt_app_gmv,
    COALESCE(
        SUM(
            CASE
                WHEN channel IN ('other_mkt') THEN gmv
                ELSE 0
            END
        ),
        0
    ) AS other_mkt_gmv,
    COALESCE(
        SUM(
            CASE
                WHEN channel IN ('other_mkt') THEN app_gmv
                ELSE 0
            END
        ),
        0
    ) AS other_mkt_app_gmv,
    COALESCE(
        SUM(
            CASE
                WHEN channel IN ('crm') THEN gmv
                ELSE 0
            END
        ),
        0
    ) AS crm_gmv,
    COALESCE(
        SUM(
            CASE
                WHEN channel IN ('crm') THEN app_gmv
                ELSE 0
            END
        ),
        0
    ) AS crm_app_gmv,
    COALESCE(
        SUM(
            CASE
                WHEN channel IN ('om') THEN gmv
                ELSE 0
            END
        ),
        0
    ) AS om_gmv,
    COALESCE(
        SUM(
            CASE
                WHEN channel IN ('om') THEN app_gmv
                ELSE 0
            END
        ),
        0
    ) AS om_app_gmv,
    COALESCE(
        SUM(
            CASE
                WHEN channel IN ('ppc') THEN gmv
                ELSE 0
            END
        ),
        0
    ) AS ppc_gmv,
    COALESCE(
        SUM(
            CASE
                WHEN channel IN ('ppc') THEN app_gmv
                ELSE 0
            END
        ),
        0
    ) AS ppc_app_gmv,
    COALESCE(
        SUM(
            CASE
                WHEN channel IN ('affiliate') THEN gmv
                ELSE 0
            END
        ),
        0
    ) AS affiliate_gmv,
    COALESCE(
        SUM(
            CASE
                WHEN channel IN ('affiliate') THEN app_gmv
                ELSE 0
            END
        ),
        0
    ) AS affiliate_app_gmv,
    COALESCE(
        SUM(
            CASE
                WHEN channel IN ('rta') THEN gmv
                ELSE 0
            END
        ),
        0
    ) AS rta_gmv,
    COALESCE(
        SUM(
            CASE
                WHEN channel IN ('rta') THEN app_gmv
                ELSE 0
            END
        ),
        0
    ) AS rta_app_gmv --<< End
FROM (
        SELECT ds,
            hh,
            venture,
            COALESCE(channel, "total") AS channel,
            SUM(
                SUM(
                    CASE
                        WHEN platform IN ('App', 'Pc', 'Wap', 'Unknown')
                        AND attr_model = 'lt_1d' THEN installs
                        ELSE 0
                    END
                )
            ) OVER (
                PARTITION BY venture,
                ds
                ORDER BY hh ASC
            ) AS installs,
            SUM(
                SUM(
                    CASE
                        WHEN platform IN ('App')
                        AND attr_model = 'lt_1d' THEN installs
                        ELSE 0
                    END
                )
            ) OVER (
                PARTITION BY venture,
                ds
                ORDER BY hh ASC
            ) AS app_installs,
            SUM(
                SUM(
                    CASE
                        WHEN platform IN ('App', 'Pc', 'Wap', 'Unknown')
                        AND attr_model = 'lt_1d' THEN reinstalls
                        ELSE 0
                    END
                )
            ) OVER (
                PARTITION BY venture,
                ds
                ORDER BY hh ASC
            ) AS reinstalls,
            SUM(
                SUM(
                    CASE
                        WHEN platform IN ('App')
                        AND attr_model = 'lt_1d' THEN reinstalls
                        ELSE 0
                    END
                )
            ) OVER (
                PARTITION BY venture,
                ds
                ORDER BY hh ASC
            ) AS app_reinstalls,
            SUM(
                SUM(
                    CASE
                        WHEN platform IN ('App', 'Pc', 'Wap', 'Unknown')
                        AND attr_model = 'ft_1d_np' THEN visits
                        ELSE 0
                    END
                )
            ) OVER (
                PARTITION BY venture,
                ds
                ORDER BY hh ASC
            ) AS visits,
            SUM(
                SUM(
                    CASE
                        WHEN platform IN ('App')
                        AND attr_model = 'ft_1d_np' THEN visits
                        ELSE 0
                    END
                )
            ) OVER (
                PARTITION BY venture,
                ds
                ORDER BY hh ASC
            ) AS app_visits,
            SUM(
                SUM(
                    CASE
                        WHEN platform IN ('App', 'Pc', 'Wap', 'Unknown')
                        AND attr_model = 'ft_1d_np' THEN dau
                        ELSE 0
                    END
                )
            ) OVER (
                PARTITION BY venture,
                ds
                ORDER BY hh ASC
            ) AS dau,
            SUM(
                SUM(
                    CASE
                        WHEN platform IN ('App')
                        AND attr_model = 'ft_1d_np' THEN dau
                        ELSE 0
                    END
                )
            ) OVER (
                PARTITION BY venture,
                ds
                ORDER BY hh ASC
            ) AS app_dau,
            SUM(
                SUM(
                    CASE
                        WHEN platform IN ('App', 'Pc', 'Wap', 'Unknown')
                        AND attr_model = 'lt_1d' THEN pdp_pv
                        ELSE 0
                    END
                )
            ) OVER (
                PARTITION BY venture,
                ds
                ORDER BY hh ASC
            ) AS pdp_pv,
            SUM(
                SUM(
                    CASE
                        WHEN platform IN ('App')
                        AND attr_model = 'lt_1d' THEN pdp_pv
                        ELSE 0
                    END
                )
            ) OVER (
                PARTITION BY venture,
                ds
                ORDER BY hh ASC
            ) AS app_pdp_pv,
            SUM(
                SUM(
                    CASE
                        WHEN platform IN ('App', 'Pc', 'Wap', 'Unknown')
                        AND attr_model = 'lt_1d' THEN pdp_uv
                        ELSE 0
                    END
                )
            ) OVER (
                PARTITION BY venture,
                ds
                ORDER BY hh ASC
            ) AS pdp_uv,
            SUM(
                SUM(
                    CASE
                        WHEN platform IN ('App')
                        AND attr_model = 'lt_1d' THEN pdp_uv
                        ELSE 0
                    END
                )
            ) OVER (
                PARTITION BY venture,
                ds
                ORDER BY hh ASC
            ) AS app_pdp_uv,
            SUM(
                SUM(
                    CASE
                        WHEN platform IN ('App', 'Pc', 'Wap', 'Unknown')
                        AND attr_model = 'lt_1d_p' THEN a2c
                        ELSE 0
                    END
                )
            ) OVER (
                PARTITION BY venture,
                ds
                ORDER BY hh ASC
            ) AS a2c,
            SUM(
                SUM(
                    CASE
                        WHEN platform IN ('App')
                        AND attr_model = 'lt_1d_p' THEN a2c
                        ELSE 0
                    END
                )
            ) OVER (
                PARTITION BY venture,
                ds
                ORDER BY hh ASC
            ) AS app_a2c,
            SUM(
                SUM(
                    CASE
                        WHEN platform IN ('App', 'Pc', 'Wap', 'Unknown')
                        AND attr_model = 'lt_1d_p' THEN a2c_uv
                        ELSE 0
                    END
                )
            ) OVER (
                PARTITION BY venture,
                ds
                ORDER BY hh ASC
            ) AS a2c_uv,
            SUM(
                SUM(
                    CASE
                        WHEN platform IN ('App')
                        AND attr_model = 'lt_1d_p' THEN a2c_uv
                        ELSE 0
                    END
                )
            ) OVER (
                PARTITION BY venture,
                ds
                ORDER BY hh ASC
            ) AS app_a2c_uv,
            SUM(
                SUM(
                    CASE
                        WHEN platform IN ('App', 'Pc', 'Wap', 'Unknown')
                        AND attr_model = 'lt_1d_p' THEN buyers
                        ELSE 0
                    END
                )
            ) OVER (
                PARTITION BY venture,
                ds
                ORDER BY hh ASC
            ) AS buyers,
            SUM(
                SUM(
                    CASE
                        WHEN platform IN ('App')
                        AND attr_model = 'lt_1d_p' THEN buyers
                        ELSE 0
                    END
                )
            ) OVER (
                PARTITION BY venture,
                ds
                ORDER BY hh ASC
            ) AS app_buyers,
            SUM(
                SUM(
                    CASE
                        WHEN platform IN ('App', 'Pc', 'Wap', 'Unknown')
                        AND attr_model = 'lt_1d_p' THEN new_buyers
                        ELSE 0
                    END
                )
            ) OVER (
                PARTITION BY venture,
                ds
                ORDER BY hh ASC
            ) AS new_buyers,
            SUM(
                SUM(
                    CASE
                        WHEN platform IN ('App')
                        AND attr_model = 'lt_1d_p' THEN new_buyers
                        ELSE 0
                    END
                )
            ) OVER (
                PARTITION BY venture,
                ds
                ORDER BY hh ASC
            ) AS app_new_buyers,
            SUM(
                SUM(
                    CASE
                        WHEN platform IN ('App', 'Pc', 'Wap', 'Unknown')
                        AND attr_model = 'lt_1d_p' THEN reacquired_buyers
                        ELSE 0
                    END
                )
            ) OVER (
                PARTITION BY venture,
                ds
                ORDER BY hh ASC
            ) AS reacquired_buyers,
            SUM(
                SUM(
                    CASE
                        WHEN platform IN ('App')
                        AND attr_model = 'lt_1d_p' THEN reacquired_buyers
                        ELSE 0
                    END
                )
            ) OVER (
                PARTITION BY venture,
                ds
                ORDER BY hh ASC
            ) AS app_reacquired_buyers,
            SUM(
                SUM(
                    CASE
                        WHEN platform IN ('App', 'Pc', 'Wap', 'Unknown')
                        AND attr_model = 'lt_1d_p' THEN sales_order_id_count
                        ELSE 0
                    END
                )
            ) OVER (
                PARTITION BY venture,
                ds
                ORDER BY hh ASC
            ) AS sales_order_id_count,
            SUM(
                SUM(
                    CASE
                        WHEN platform IN ('App')
                        AND attr_model = 'lt_1d_p' THEN sales_order_id_count
                        ELSE 0
                    END
                )
            ) OVER (
                PARTITION BY venture,
                ds
                ORDER BY hh ASC
            ) AS app_sales_order_id_count,
            SUM(
                SUM(
                    CASE
                        WHEN platform IN ('App', 'Pc', 'Wap', 'Unknown')
                        AND attr_model = 'lt_1d_p' THEN order_id_count
                        ELSE 0
                    END
                )
            ) OVER (
                PARTITION BY venture,
                ds
                ORDER BY hh ASC
            ) AS order_id_count,
            SUM(
                SUM(
                    CASE
                        WHEN platform IN ('App')
                        AND attr_model = 'lt_1d_p' THEN order_id_count
                        ELSE 0
                    END
                )
            ) OVER (
                PARTITION BY venture,
                ds
                ORDER BY hh ASC
            ) AS app_order_id_count,
            SUM(
                SUM(
                    CASE
                        WHEN platform IN ('App', 'Pc', 'Wap', 'Unknown')
                        AND attr_model = 'lt_1d_p' THEN gmv
                        ELSE 0
                    END
                )
            ) OVER (
                PARTITION BY venture,
                ds
                ORDER BY hh ASC
            ) AS gmv,
            SUM(
                SUM(
                    CASE
                        WHEN platform IN ('App')
                        AND attr_model = 'lt_1d_p' THEN gmv
                        ELSE 0
                    END
                )
            ) OVER (
                PARTITION BY venture,
                ds
                ORDER BY hh ASC
            ) AS app_gmv
        FROM lazada_ads.ads_lzd_mkt_rt_uam_campaign_hi
        WHERE 1 = 1
            AND attr_model IN ('ft_1d_np', 'lt_1d', 'lt_1d_p')
        GROUP BY ds,
            hh,
            venture,
            channel GROUPING SETS ((ds, hh, venture))
        UNION ALL
        SELECT ds,
            hh,
            venture,
            COALESCE(channel, "organic") AS channel,
            SUM(
                SUM(
                    CASE
                        WHEN platform IN ('App', 'Pc', 'Wap', 'Unknown')
                        AND attr_model = 'lt_1d' THEN installs
                        ELSE 0
                    END
                )
            ) OVER (
                PARTITION BY venture,
                ds
                ORDER BY hh ASC
            ) AS installs,
            SUM(
                SUM(
                    CASE
                        WHEN platform IN ('App')
                        AND attr_model = 'lt_1d' THEN installs
                        ELSE 0
                    END
                )
            ) OVER (
                PARTITION BY venture,
                ds
                ORDER BY hh ASC
            ) AS app_installs,
            SUM(
                SUM(
                    CASE
                        WHEN platform IN ('App', 'Pc', 'Wap', 'Unknown')
                        AND attr_model = 'lt_1d' THEN reinstalls
                        ELSE 0
                    END
                )
            ) OVER (
                PARTITION BY venture,
                ds
                ORDER BY hh ASC
            ) AS reinstalls,
            SUM(
                SUM(
                    CASE
                        WHEN platform IN ('App')
                        AND attr_model = 'lt_1d' THEN reinstalls
                        ELSE 0
                    END
                )
            ) OVER (
                PARTITION BY venture,
                ds
                ORDER BY hh ASC
            ) AS app_reinstalls,
            SUM(
                SUM(
                    CASE
                        WHEN platform IN ('App', 'Pc', 'Wap', 'Unknown')
                        AND attr_model = 'ft_1d_np' THEN visits
                        ELSE 0
                    END
                )
            ) OVER (
                PARTITION BY venture,
                ds
                ORDER BY hh ASC
            ) AS visits,
            SUM(
                SUM(
                    CASE
                        WHEN platform IN ('App')
                        AND attr_model = 'ft_1d_np' THEN visits
                        ELSE 0
                    END
                )
            ) OVER (
                PARTITION BY venture,
                ds
                ORDER BY hh ASC
            ) AS app_visits,
            SUM(
                SUM(
                    CASE
                        WHEN platform IN ('App', 'Pc', 'Wap', 'Unknown')
                        AND attr_model = 'ft_1d_np' THEN dau
                        ELSE 0
                    END
                )
            ) OVER (
                PARTITION BY venture,
                ds
                ORDER BY hh ASC
            ) AS dau,
            SUM(
                SUM(
                    CASE
                        WHEN platform IN ('App')
                        AND attr_model = 'ft_1d_np' THEN dau
                        ELSE 0
                    END
                )
            ) OVER (
                PARTITION BY venture,
                ds
                ORDER BY hh ASC
            ) AS app_dau,
            SUM(
                SUM(
                    CASE
                        WHEN platform IN ('App', 'Pc', 'Wap', 'Unknown')
                        AND attr_model = 'lt_1d' THEN pdp_pv
                        ELSE 0
                    END
                )
            ) OVER (
                PARTITION BY venture,
                ds
                ORDER BY hh ASC
            ) AS pdp_pv,
            SUM(
                SUM(
                    CASE
                        WHEN platform IN ('App')
                        AND attr_model = 'lt_1d' THEN pdp_pv
                        ELSE 0
                    END
                )
            ) OVER (
                PARTITION BY venture,
                ds
                ORDER BY hh ASC
            ) AS app_pdp_pv,
            SUM(
                SUM(
                    CASE
                        WHEN platform IN ('App', 'Pc', 'Wap', 'Unknown')
                        AND attr_model = 'lt_1d' THEN pdp_uv
                        ELSE 0
                    END
                )
            ) OVER (
                PARTITION BY venture,
                ds
                ORDER BY hh ASC
            ) AS pdp_uv,
            SUM(
                SUM(
                    CASE
                        WHEN platform IN ('App')
                        AND attr_model = 'lt_1d' THEN pdp_uv
                        ELSE 0
                    END
                )
            ) OVER (
                PARTITION BY venture,
                ds
                ORDER BY hh ASC
            ) AS app_pdp_uv,
            SUM(
                SUM(
                    CASE
                        WHEN platform IN ('App', 'Pc', 'Wap', 'Unknown')
                        AND attr_model = 'lt_1d_p' THEN a2c
                        ELSE 0
                    END
                )
            ) OVER (
                PARTITION BY venture,
                ds
                ORDER BY hh ASC
            ) AS a2c,
            SUM(
                SUM(
                    CASE
                        WHEN platform IN ('App')
                        AND attr_model = 'lt_1d_p' THEN a2c
                        ELSE 0
                    END
                )
            ) OVER (
                PARTITION BY venture,
                ds
                ORDER BY hh ASC
            ) AS app_a2c,
            SUM(
                SUM(
                    CASE
                        WHEN platform IN ('App', 'Pc', 'Wap', 'Unknown')
                        AND attr_model = 'lt_1d_p' THEN a2c_uv
                        ELSE 0
                    END
                )
            ) OVER (
                PARTITION BY venture,
                ds
                ORDER BY hh ASC
            ) AS a2c_uv,
            SUM(
                SUM(
                    CASE
                        WHEN platform IN ('App')
                        AND attr_model = 'lt_1d_p' THEN a2c_uv
                        ELSE 0
                    END
                )
            ) OVER (
                PARTITION BY venture,
                ds
                ORDER BY hh ASC
            ) AS app_a2c_uv,
            SUM(
                SUM(
                    CASE
                        WHEN platform IN ('App', 'Pc', 'Wap', 'Unknown')
                        AND attr_model = 'lt_1d_p' THEN buyers
                        ELSE 0
                    END
                )
            ) OVER (
                PARTITION BY venture,
                ds
                ORDER BY hh ASC
            ) AS buyers,
            SUM(
                SUM(
                    CASE
                        WHEN platform IN ('App')
                        AND attr_model = 'lt_1d_p' THEN buyers
                        ELSE 0
                    END
                )
            ) OVER (
                PARTITION BY venture,
                ds
                ORDER BY hh ASC
            ) AS app_buyers,
            SUM(
                SUM(
                    CASE
                        WHEN platform IN ('App', 'Pc', 'Wap', 'Unknown')
                        AND attr_model = 'lt_1d_p' THEN new_buyers
                        ELSE 0
                    END
                )
            ) OVER (
                PARTITION BY venture,
                ds
                ORDER BY hh ASC
            ) AS new_buyers,
            SUM(
                SUM(
                    CASE
                        WHEN platform IN ('App')
                        AND attr_model = 'lt_1d_p' THEN new_buyers
                        ELSE 0
                    END
                )
            ) OVER (
                PARTITION BY venture,
                ds
                ORDER BY hh ASC
            ) AS app_new_buyers,
            SUM(
                SUM(
                    CASE
                        WHEN platform IN ('App', 'Pc', 'Wap', 'Unknown')
                        AND attr_model = 'lt_1d_p' THEN reacquired_buyers
                        ELSE 0
                    END
                )
            ) OVER (
                PARTITION BY venture,
                ds
                ORDER BY hh ASC
            ) AS reacquired_buyers,
            SUM(
                SUM(
                    CASE
                        WHEN platform IN ('App')
                        AND attr_model = 'lt_1d_p' THEN reacquired_buyers
                        ELSE 0
                    END
                )
            ) OVER (
                PARTITION BY venture,
                ds
                ORDER BY hh ASC
            ) AS app_reacquired_buyers,
            SUM(
                SUM(
                    CASE
                        WHEN platform IN ('App', 'Pc', 'Wap', 'Unknown')
                        AND attr_model = 'lt_1d_p' THEN sales_order_id_count
                        ELSE 0
                    END
                )
            ) OVER (
                PARTITION BY venture,
                ds
                ORDER BY hh ASC
            ) AS sales_order_id_count,
            SUM(
                SUM(
                    CASE
                        WHEN platform IN ('App')
                        AND attr_model = 'lt_1d_p' THEN sales_order_id_count
                        ELSE 0
                    END
                )
            ) OVER (
                PARTITION BY venture,
                ds
                ORDER BY hh ASC
            ) AS app_sales_order_id_count,
            SUM(
                SUM(
                    CASE
                        WHEN platform IN ('App', 'Pc', 'Wap', 'Unknown')
                        AND attr_model = 'lt_1d_p' THEN order_id_count
                        ELSE 0
                    END
                )
            ) OVER (
                PARTITION BY venture,
                ds
                ORDER BY hh ASC
            ) AS order_id_count,
            SUM(
                SUM(
                    CASE
                        WHEN platform IN ('App')
                        AND attr_model = 'lt_1d_p' THEN order_id_count
                        ELSE 0
                    END
                )
            ) OVER (
                PARTITION BY venture,
                ds
                ORDER BY hh ASC
            ) AS app_order_id_count,
            SUM(
                SUM(
                    CASE
                        WHEN platform IN ('App', 'Pc', 'Wap', 'Unknown')
                        AND attr_model = 'lt_1d_p' THEN gmv
                        ELSE 0
                    END
                )
            ) OVER (
                PARTITION BY venture,
                ds
                ORDER BY hh ASC
            ) AS gmv,
            SUM(
                SUM(
                    CASE
                        WHEN platform IN ('App')
                        AND attr_model = 'lt_1d_p' THEN gmv
                        ELSE 0
                    END
                )
            ) OVER (
                PARTITION BY venture,
                ds
                ORDER BY hh ASC
            ) AS app_gmv
        FROM lazada_ads.ads_lzd_mkt_rt_uam_campaign_hi
        WHERE 1 = 1
            AND funding_bucket IN ('Unknown')
            AND channel IN ('Direct')
            AND attr_model IN ('ft_1d_np', 'lt_1d', 'lt_1d_p')
        GROUP BY ds,
            hh,
            venture,
            channel GROUPING SETS ((ds, hh, venture))
        UNION ALL
        SELECT ds,
            hh,
            venture,
            COALESCE(channel, "mkt") AS channel,
            SUM(
                SUM(
                    CASE
                        WHEN platform IN ('App', 'Pc', 'Wap', 'Unknown')
                        AND attr_model = 'lt_1d' THEN installs
                        ELSE 0
                    END
                )
            ) OVER (
                PARTITION BY venture,
                ds
                ORDER BY hh ASC
            ) AS installs,
            SUM(
                SUM(
                    CASE
                        WHEN platform IN ('App')
                        AND attr_model = 'lt_1d' THEN installs
                        ELSE 0
                    END
                )
            ) OVER (
                PARTITION BY venture,
                ds
                ORDER BY hh ASC
            ) AS app_installs,
            SUM(
                SUM(
                    CASE
                        WHEN platform IN ('App', 'Pc', 'Wap', 'Unknown')
                        AND attr_model = 'lt_1d' THEN reinstalls
                        ELSE 0
                    END
                )
            ) OVER (
                PARTITION BY venture,
                ds
                ORDER BY hh ASC
            ) AS reinstalls,
            SUM(
                SUM(
                    CASE
                        WHEN platform IN ('App')
                        AND attr_model = 'lt_1d' THEN reinstalls
                        ELSE 0
                    END
                )
            ) OVER (
                PARTITION BY venture,
                ds
                ORDER BY hh ASC
            ) AS app_reinstalls,
            SUM(
                SUM(
                    CASE
                        WHEN platform IN ('App', 'Pc', 'Wap', 'Unknown')
                        AND attr_model = 'ft_1d_np' THEN visits
                        ELSE 0
                    END
                )
            ) OVER (
                PARTITION BY venture,
                ds
                ORDER BY hh ASC
            ) AS visits,
            SUM(
                SUM(
                    CASE
                        WHEN platform IN ('App')
                        AND attr_model = 'ft_1d_np' THEN visits
                        ELSE 0
                    END
                )
            ) OVER (
                PARTITION BY venture,
                ds
                ORDER BY hh ASC
            ) AS app_visits,
            SUM(
                SUM(
                    CASE
                        WHEN platform IN ('App', 'Pc', 'Wap', 'Unknown')
                        AND attr_model = 'ft_1d_np' THEN dau
                        ELSE 0
                    END
                )
            ) OVER (
                PARTITION BY venture,
                ds
                ORDER BY hh ASC
            ) AS dau,
            SUM(
                SUM(
                    CASE
                        WHEN platform IN ('App')
                        AND attr_model = 'ft_1d_np' THEN dau
                        ELSE 0
                    END
                )
            ) OVER (
                PARTITION BY venture,
                ds
                ORDER BY hh ASC
            ) AS app_dau,
            SUM(
                SUM(
                    CASE
                        WHEN platform IN ('App', 'Pc', 'Wap', 'Unknown')
                        AND attr_model = 'lt_1d' THEN pdp_pv
                        ELSE 0
                    END
                )
            ) OVER (
                PARTITION BY venture,
                ds
                ORDER BY hh ASC
            ) AS pdp_pv,
            SUM(
                SUM(
                    CASE
                        WHEN platform IN ('App')
                        AND attr_model = 'lt_1d' THEN pdp_pv
                        ELSE 0
                    END
                )
            ) OVER (
                PARTITION BY venture,
                ds
                ORDER BY hh ASC
            ) AS app_pdp_pv,
            SUM(
                SUM(
                    CASE
                        WHEN platform IN ('App', 'Pc', 'Wap', 'Unknown')
                        AND attr_model = 'lt_1d' THEN pdp_uv
                        ELSE 0
                    END
                )
            ) OVER (
                PARTITION BY venture,
                ds
                ORDER BY hh ASC
            ) AS pdp_uv,
            SUM(
                SUM(
                    CASE
                        WHEN platform IN ('App')
                        AND attr_model = 'lt_1d' THEN pdp_uv
                        ELSE 0
                    END
                )
            ) OVER (
                PARTITION BY venture,
                ds
                ORDER BY hh ASC
            ) AS app_pdp_uv,
            SUM(
                SUM(
                    CASE
                        WHEN platform IN ('App', 'Pc', 'Wap', 'Unknown')
                        AND attr_model = 'lt_1d_p' THEN a2c
                        ELSE 0
                    END
                )
            ) OVER (
                PARTITION BY venture,
                ds
                ORDER BY hh ASC
            ) AS a2c,
            SUM(
                SUM(
                    CASE
                        WHEN platform IN ('App')
                        AND attr_model = 'lt_1d_p' THEN a2c
                        ELSE 0
                    END
                )
            ) OVER (
                PARTITION BY venture,
                ds
                ORDER BY hh ASC
            ) AS app_a2c,
            SUM(
                SUM(
                    CASE
                        WHEN platform IN ('App', 'Pc', 'Wap', 'Unknown')
                        AND attr_model = 'lt_1d_p' THEN a2c_uv
                        ELSE 0
                    END
                )
            ) OVER (
                PARTITION BY venture,
                ds
                ORDER BY hh ASC
            ) AS a2c_uv,
            SUM(
                SUM(
                    CASE
                        WHEN platform IN ('App')
                        AND attr_model = 'lt_1d_p' THEN a2c_uv
                        ELSE 0
                    END
                )
            ) OVER (
                PARTITION BY venture,
                ds
                ORDER BY hh ASC
            ) AS app_a2c_uv,
            SUM(
                SUM(
                    CASE
                        WHEN platform IN ('App', 'Pc', 'Wap', 'Unknown')
                        AND attr_model = 'lt_1d_p' THEN buyers
                        ELSE 0
                    END
                )
            ) OVER (
                PARTITION BY venture,
                ds
                ORDER BY hh ASC
            ) AS buyers,
            SUM(
                SUM(
                    CASE
                        WHEN platform IN ('App')
                        AND attr_model = 'lt_1d_p' THEN buyers
                        ELSE 0
                    END
                )
            ) OVER (
                PARTITION BY venture,
                ds
                ORDER BY hh ASC
            ) AS app_buyers,
            SUM(
                SUM(
                    CASE
                        WHEN platform IN ('App', 'Pc', 'Wap', 'Unknown')
                        AND attr_model = 'lt_1d_p' THEN new_buyers
                        ELSE 0
                    END
                )
            ) OVER (
                PARTITION BY venture,
                ds
                ORDER BY hh ASC
            ) AS new_buyers,
            SUM(
                SUM(
                    CASE
                        WHEN platform IN ('App')
                        AND attr_model = 'lt_1d_p' THEN new_buyers
                        ELSE 0
                    END
                )
            ) OVER (
                PARTITION BY venture,
                ds
                ORDER BY hh ASC
            ) AS app_new_buyers,
            SUM(
                SUM(
                    CASE
                        WHEN platform IN ('App', 'Pc', 'Wap', 'Unknown')
                        AND attr_model = 'lt_1d_p' THEN reacquired_buyers
                        ELSE 0
                    END
                )
            ) OVER (
                PARTITION BY venture,
                ds
                ORDER BY hh ASC
            ) AS reacquired_buyers,
            SUM(
                SUM(
                    CASE
                        WHEN platform IN ('App')
                        AND attr_model = 'lt_1d_p' THEN reacquired_buyers
                        ELSE 0
                    END
                )
            ) OVER (
                PARTITION BY venture,
                ds
                ORDER BY hh ASC
            ) AS app_reacquired_buyers,
            SUM(
                SUM(
                    CASE
                        WHEN platform IN ('App', 'Pc', 'Wap', 'Unknown')
                        AND attr_model = 'lt_1d_p' THEN sales_order_id_count
                        ELSE 0
                    END
                )
            ) OVER (
                PARTITION BY venture,
                ds
                ORDER BY hh ASC
            ) AS sales_order_id_count,
            SUM(
                SUM(
                    CASE
                        WHEN platform IN ('App')
                        AND attr_model = 'lt_1d_p' THEN sales_order_id_count
                        ELSE 0
                    END
                )
            ) OVER (
                PARTITION BY venture,
                ds
                ORDER BY hh ASC
            ) AS app_sales_order_id_count,
            SUM(
                SUM(
                    CASE
                        WHEN platform IN ('App', 'Pc', 'Wap', 'Unknown')
                        AND attr_model = 'lt_1d_p' THEN order_id_count
                        ELSE 0
                    END
                )
            ) OVER (
                PARTITION BY venture,
                ds
                ORDER BY hh ASC
            ) AS order_id_count,
            SUM(
                SUM(
                    CASE
                        WHEN platform IN ('App')
                        AND attr_model = 'lt_1d_p' THEN order_id_count
                        ELSE 0
                    END
                )
            ) OVER (
                PARTITION BY venture,
                ds
                ORDER BY hh ASC
            ) AS app_order_id_count,
            SUM(
                SUM(
                    CASE
                        WHEN platform IN ('App', 'Pc', 'Wap', 'Unknown')
                        AND attr_model = 'lt_1d_p' THEN gmv
                        ELSE 0
                    END
                )
            ) OVER (
                PARTITION BY venture,
                ds
                ORDER BY hh ASC
            ) AS gmv,
            SUM(
                SUM(
                    CASE
                        WHEN platform IN ('App')
                        AND attr_model = 'lt_1d_p' THEN gmv
                        ELSE 0
                    END
                )
            ) OVER (
                PARTITION BY venture,
                ds
                ORDER BY hh ASC
            ) AS app_gmv
        FROM lazada_ads.ads_lzd_mkt_rt_uam_campaign_hi
        WHERE 1 = 1
            AND funding_bucket NOT IN ('Unknown')
            AND channel NOT IN ('Direct')
            AND attr_model IN ('ft_1d_np', 'lt_1d', 'lt_1d_p')
        GROUP BY ds,
            hh,
            venture,
            channel GROUPING SETS ((ds, hh, venture))
        UNION ALL
        SELECT ds,
            hh,
            venture,
            COALESCE(channel, "other_mkt") AS channel,
            SUM(
                SUM(
                    CASE
                        WHEN platform IN ('App', 'Pc', 'Wap', 'Unknown')
                        AND attr_model = 'lt_1d' THEN installs
                        ELSE 0
                    END
                )
            ) OVER (
                PARTITION BY venture,
                ds
                ORDER BY hh ASC
            ) AS installs,
            SUM(
                SUM(
                    CASE
                        WHEN platform IN ('App')
                        AND attr_model = 'lt_1d' THEN installs
                        ELSE 0
                    END
                )
            ) OVER (
                PARTITION BY venture,
                ds
                ORDER BY hh ASC
            ) AS app_installs,
            SUM(
                SUM(
                    CASE
                        WHEN platform IN ('App', 'Pc', 'Wap', 'Unknown')
                        AND attr_model = 'lt_1d' THEN reinstalls
                        ELSE 0
                    END
                )
            ) OVER (
                PARTITION BY venture,
                ds
                ORDER BY hh ASC
            ) AS reinstalls,
            SUM(
                SUM(
                    CASE
                        WHEN platform IN ('App')
                        AND attr_model = 'lt_1d' THEN reinstalls
                        ELSE 0
                    END
                )
            ) OVER (
                PARTITION BY venture,
                ds
                ORDER BY hh ASC
            ) AS app_reinstalls,
            SUM(
                SUM(
                    CASE
                        WHEN platform IN ('App', 'Pc', 'Wap', 'Unknown')
                        AND attr_model = 'ft_1d_np' THEN visits
                        ELSE 0
                    END
                )
            ) OVER (
                PARTITION BY venture,
                ds
                ORDER BY hh ASC
            ) AS visits,
            SUM(
                SUM(
                    CASE
                        WHEN platform IN ('App')
                        AND attr_model = 'ft_1d_np' THEN visits
                        ELSE 0
                    END
                )
            ) OVER (
                PARTITION BY venture,
                ds
                ORDER BY hh ASC
            ) AS app_visits,
            SUM(
                SUM(
                    CASE
                        WHEN platform IN ('App', 'Pc', 'Wap', 'Unknown')
                        AND attr_model = 'ft_1d_np' THEN dau
                        ELSE 0
                    END
                )
            ) OVER (
                PARTITION BY venture,
                ds
                ORDER BY hh ASC
            ) AS dau,
            SUM(
                SUM(
                    CASE
                        WHEN platform IN ('App')
                        AND attr_model = 'ft_1d_np' THEN dau
                        ELSE 0
                    END
                )
            ) OVER (
                PARTITION BY venture,
                ds
                ORDER BY hh ASC
            ) AS app_dau,
            SUM(
                SUM(
                    CASE
                        WHEN platform IN ('App', 'Pc', 'Wap', 'Unknown')
                        AND attr_model = 'lt_1d' THEN pdp_pv
                        ELSE 0
                    END
                )
            ) OVER (
                PARTITION BY venture,
                ds
                ORDER BY hh ASC
            ) AS pdp_pv,
            SUM(
                SUM(
                    CASE
                        WHEN platform IN ('App')
                        AND attr_model = 'lt_1d' THEN pdp_pv
                        ELSE 0
                    END
                )
            ) OVER (
                PARTITION BY venture,
                ds
                ORDER BY hh ASC
            ) AS app_pdp_pv,
            SUM(
                SUM(
                    CASE
                        WHEN platform IN ('App', 'Pc', 'Wap', 'Unknown')
                        AND attr_model = 'lt_1d' THEN pdp_uv
                        ELSE 0
                    END
                )
            ) OVER (
                PARTITION BY venture,
                ds
                ORDER BY hh ASC
            ) AS pdp_uv,
            SUM(
                SUM(
                    CASE
                        WHEN platform IN ('App')
                        AND attr_model = 'lt_1d' THEN pdp_uv
                        ELSE 0
                    END
                )
            ) OVER (
                PARTITION BY venture,
                ds
                ORDER BY hh ASC
            ) AS app_pdp_uv,
            SUM(
                SUM(
                    CASE
                        WHEN platform IN ('App', 'Pc', 'Wap', 'Unknown')
                        AND attr_model = 'lt_1d_p' THEN a2c
                        ELSE 0
                    END
                )
            ) OVER (
                PARTITION BY venture,
                ds
                ORDER BY hh ASC
            ) AS a2c,
            SUM(
                SUM(
                    CASE
                        WHEN platform IN ('App')
                        AND attr_model = 'lt_1d_p' THEN a2c
                        ELSE 0
                    END
                )
            ) OVER (
                PARTITION BY venture,
                ds
                ORDER BY hh ASC
            ) AS app_a2c,
            SUM(
                SUM(
                    CASE
                        WHEN platform IN ('App', 'Pc', 'Wap', 'Unknown')
                        AND attr_model = 'lt_1d_p' THEN a2c_uv
                        ELSE 0
                    END
                )
            ) OVER (
                PARTITION BY venture,
                ds
                ORDER BY hh ASC
            ) AS a2c_uv,
            SUM(
                SUM(
                    CASE
                        WHEN platform IN ('App')
                        AND attr_model = 'lt_1d_p' THEN a2c_uv
                        ELSE 0
                    END
                )
            ) OVER (
                PARTITION BY venture,
                ds
                ORDER BY hh ASC
            ) AS app_a2c_uv,
            SUM(
                SUM(
                    CASE
                        WHEN platform IN ('App', 'Pc', 'Wap', 'Unknown')
                        AND attr_model = 'lt_1d_p' THEN buyers
                        ELSE 0
                    END
                )
            ) OVER (
                PARTITION BY venture,
                ds
                ORDER BY hh ASC
            ) AS buyers,
            SUM(
                SUM(
                    CASE
                        WHEN platform IN ('App')
                        AND attr_model = 'lt_1d_p' THEN buyers
                        ELSE 0
                    END
                )
            ) OVER (
                PARTITION BY venture,
                ds
                ORDER BY hh ASC
            ) AS app_buyers,
            SUM(
                SUM(
                    CASE
                        WHEN platform IN ('App', 'Pc', 'Wap', 'Unknown')
                        AND attr_model = 'lt_1d_p' THEN new_buyers
                        ELSE 0
                    END
                )
            ) OVER (
                PARTITION BY venture,
                ds
                ORDER BY hh ASC
            ) AS new_buyers,
            SUM(
                SUM(
                    CASE
                        WHEN platform IN ('App')
                        AND attr_model = 'lt_1d_p' THEN new_buyers
                        ELSE 0
                    END
                )
            ) OVER (
                PARTITION BY venture,
                ds
                ORDER BY hh ASC
            ) AS app_new_buyers,
            SUM(
                SUM(
                    CASE
                        WHEN platform IN ('App', 'Pc', 'Wap', 'Unknown')
                        AND attr_model = 'lt_1d_p' THEN reacquired_buyers
                        ELSE 0
                    END
                )
            ) OVER (
                PARTITION BY venture,
                ds
                ORDER BY hh ASC
            ) AS reacquired_buyers,
            SUM(
                SUM(
                    CASE
                        WHEN platform IN ('App')
                        AND attr_model = 'lt_1d_p' THEN reacquired_buyers
                        ELSE 0
                    END
                )
            ) OVER (
                PARTITION BY venture,
                ds
                ORDER BY hh ASC
            ) AS app_reacquired_buyers,
            SUM(
                SUM(
                    CASE
                        WHEN platform IN ('App', 'Pc', 'Wap', 'Unknown')
                        AND attr_model = 'lt_1d_p' THEN sales_order_id_count
                        ELSE 0
                    END
                )
            ) OVER (
                PARTITION BY venture,
                ds
                ORDER BY hh ASC
            ) AS sales_order_id_count,
            SUM(
                SUM(
                    CASE
                        WHEN platform IN ('App')
                        AND attr_model = 'lt_1d_p' THEN sales_order_id_count
                        ELSE 0
                    END
                )
            ) OVER (
                PARTITION BY venture,
                ds
                ORDER BY hh ASC
            ) AS app_sales_order_id_count,
            SUM(
                SUM(
                    CASE
                        WHEN platform IN ('App', 'Pc', 'Wap', 'Unknown')
                        AND attr_model = 'lt_1d_p' THEN order_id_count
                        ELSE 0
                    END
                )
            ) OVER (
                PARTITION BY venture,
                ds
                ORDER BY hh ASC
            ) AS order_id_count,
            SUM(
                SUM(
                    CASE
                        WHEN platform IN ('App')
                        AND attr_model = 'lt_1d_p' THEN order_id_count
                        ELSE 0
                    END
                )
            ) OVER (
                PARTITION BY venture,
                ds
                ORDER BY hh ASC
            ) AS app_order_id_count,
            SUM(
                SUM(
                    CASE
                        WHEN platform IN ('App', 'Pc', 'Wap', 'Unknown')
                        AND attr_model = 'lt_1d_p' THEN gmv
                        ELSE 0
                    END
                )
            ) OVER (
                PARTITION BY venture,
                ds
                ORDER BY hh ASC
            ) AS gmv,
            SUM(
                SUM(
                    CASE
                        WHEN platform IN ('App')
                        AND attr_model = 'lt_1d_p' THEN gmv
                        ELSE 0
                    END
                )
            ) OVER (
                PARTITION BY venture,
                ds
                ORDER BY hh ASC
            ) AS app_gmv
        FROM lazada_ads.ads_lzd_mkt_rt_uam_campaign_hi
        WHERE 1 = 1
            AND funding_bucket NOT IN ('Lazada OM', 'CRM', 'Unknown')
            AND attr_model IN ('ft_1d_np', 'lt_1d', 'lt_1d_p')
        GROUP BY ds,
            hh,
            venture,
            channel GROUPING SETS ((ds, hh, venture))
        UNION ALL
        SELECT ds,
            hh,
            venture,
            COALESCE(channel, "crm") AS channel,
            SUM(
                SUM(
                    CASE
                        WHEN platform IN ('App', 'Pc', 'Wap', 'Unknown')
                        AND attr_model = 'lt_1d' THEN installs
                        ELSE 0
                    END
                )
            ) OVER (
                PARTITION BY venture,
                ds
                ORDER BY hh ASC
            ) AS installs,
            SUM(
                SUM(
                    CASE
                        WHEN platform IN ('App')
                        AND attr_model = 'lt_1d' THEN installs
                        ELSE 0
                    END
                )
            ) OVER (
                PARTITION BY venture,
                ds
                ORDER BY hh ASC
            ) AS app_installs,
            SUM(
                SUM(
                    CASE
                        WHEN platform IN ('App', 'Pc', 'Wap', 'Unknown')
                        AND attr_model = 'lt_1d' THEN reinstalls
                        ELSE 0
                    END
                )
            ) OVER (
                PARTITION BY venture,
                ds
                ORDER BY hh ASC
            ) AS reinstalls,
            SUM(
                SUM(
                    CASE
                        WHEN platform IN ('App')
                        AND attr_model = 'lt_1d' THEN reinstalls
                        ELSE 0
                    END
                )
            ) OVER (
                PARTITION BY venture,
                ds
                ORDER BY hh ASC
            ) AS app_reinstalls,
            SUM(
                SUM(
                    CASE
                        WHEN platform IN ('App', 'Pc', 'Wap', 'Unknown')
                        AND attr_model = 'ft_1d_np' THEN visits
                        ELSE 0
                    END
                )
            ) OVER (
                PARTITION BY venture,
                ds
                ORDER BY hh ASC
            ) AS visits,
            SUM(
                SUM(
                    CASE
                        WHEN platform IN ('App')
                        AND attr_model = 'ft_1d_np' THEN visits
                        ELSE 0
                    END
                )
            ) OVER (
                PARTITION BY venture,
                ds
                ORDER BY hh ASC
            ) AS app_visits,
            SUM(
                SUM(
                    CASE
                        WHEN platform IN ('App', 'Pc', 'Wap', 'Unknown')
                        AND attr_model = 'ft_1d_np' THEN dau
                        ELSE 0
                    END
                )
            ) OVER (
                PARTITION BY venture,
                ds
                ORDER BY hh ASC
            ) AS dau,
            SUM(
                SUM(
                    CASE
                        WHEN platform IN ('App')
                        AND attr_model = 'ft_1d_np' THEN dau
                        ELSE 0
                    END
                )
            ) OVER (
                PARTITION BY venture,
                ds
                ORDER BY hh ASC
            ) AS app_dau,
            SUM(
                SUM(
                    CASE
                        WHEN platform IN ('App', 'Pc', 'Wap', 'Unknown')
                        AND attr_model = 'lt_1d' THEN pdp_pv
                        ELSE 0
                    END
                )
            ) OVER (
                PARTITION BY venture,
                ds
                ORDER BY hh ASC
            ) AS pdp_pv,
            SUM(
                SUM(
                    CASE
                        WHEN platform IN ('App')
                        AND attr_model = 'lt_1d' THEN pdp_pv
                        ELSE 0
                    END
                )
            ) OVER (
                PARTITION BY venture,
                ds
                ORDER BY hh ASC
            ) AS app_pdp_pv,
            SUM(
                SUM(
                    CASE
                        WHEN platform IN ('App', 'Pc', 'Wap', 'Unknown')
                        AND attr_model = 'lt_1d' THEN pdp_uv
                        ELSE 0
                    END
                )
            ) OVER (
                PARTITION BY venture,
                ds
                ORDER BY hh ASC
            ) AS pdp_uv,
            SUM(
                SUM(
                    CASE
                        WHEN platform IN ('App')
                        AND attr_model = 'lt_1d' THEN pdp_uv
                        ELSE 0
                    END
                )
            ) OVER (
                PARTITION BY venture,
                ds
                ORDER BY hh ASC
            ) AS app_pdp_uv,
            SUM(
                SUM(
                    CASE
                        WHEN platform IN ('App', 'Pc', 'Wap', 'Unknown')
                        AND attr_model = 'lt_1d_p' THEN a2c
                        ELSE 0
                    END
                )
            ) OVER (
                PARTITION BY venture,
                ds
                ORDER BY hh ASC
            ) AS a2c,
            SUM(
                SUM(
                    CASE
                        WHEN platform IN ('App')
                        AND attr_model = 'lt_1d_p' THEN a2c
                        ELSE 0
                    END
                )
            ) OVER (
                PARTITION BY venture,
                ds
                ORDER BY hh ASC
            ) AS app_a2c,
            SUM(
                SUM(
                    CASE
                        WHEN platform IN ('App', 'Pc', 'Wap', 'Unknown')
                        AND attr_model = 'lt_1d_p' THEN a2c_uv
                        ELSE 0
                    END
                )
            ) OVER (
                PARTITION BY venture,
                ds
                ORDER BY hh ASC
            ) AS a2c_uv,
            SUM(
                SUM(
                    CASE
                        WHEN platform IN ('App')
                        AND attr_model = 'lt_1d_p' THEN a2c_uv
                        ELSE 0
                    END
                )
            ) OVER (
                PARTITION BY venture,
                ds
                ORDER BY hh ASC
            ) AS app_a2c_uv,
            SUM(
                SUM(
                    CASE
                        WHEN platform IN ('App', 'Pc', 'Wap', 'Unknown')
                        AND attr_model = 'lt_1d_p' THEN buyers
                        ELSE 0
                    END
                )
            ) OVER (
                PARTITION BY venture,
                ds
                ORDER BY hh ASC
            ) AS buyers,
            SUM(
                SUM(
                    CASE
                        WHEN platform IN ('App')
                        AND attr_model = 'lt_1d_p' THEN buyers
                        ELSE 0
                    END
                )
            ) OVER (
                PARTITION BY venture,
                ds
                ORDER BY hh ASC
            ) AS app_buyers,
            SUM(
                SUM(
                    CASE
                        WHEN platform IN ('App', 'Pc', 'Wap', 'Unknown')
                        AND attr_model = 'lt_1d_p' THEN new_buyers
                        ELSE 0
                    END
                )
            ) OVER (
                PARTITION BY venture,
                ds
                ORDER BY hh ASC
            ) AS new_buyers,
            SUM(
                SUM(
                    CASE
                        WHEN platform IN ('App')
                        AND attr_model = 'lt_1d_p' THEN new_buyers
                        ELSE 0
                    END
                )
            ) OVER (
                PARTITION BY venture,
                ds
                ORDER BY hh ASC
            ) AS app_new_buyers,
            SUM(
                SUM(
                    CASE
                        WHEN platform IN ('App', 'Pc', 'Wap', 'Unknown')
                        AND attr_model = 'lt_1d_p' THEN reacquired_buyers
                        ELSE 0
                    END
                )
            ) OVER (
                PARTITION BY venture,
                ds
                ORDER BY hh ASC
            ) AS reacquired_buyers,
            SUM(
                SUM(
                    CASE
                        WHEN platform IN ('App')
                        AND attr_model = 'lt_1d_p' THEN reacquired_buyers
                        ELSE 0
                    END
                )
            ) OVER (
                PARTITION BY venture,
                ds
                ORDER BY hh ASC
            ) AS app_reacquired_buyers,
            SUM(
                SUM(
                    CASE
                        WHEN platform IN ('App', 'Pc', 'Wap', 'Unknown')
                        AND attr_model = 'lt_1d_p' THEN sales_order_id_count
                        ELSE 0
                    END
                )
            ) OVER (
                PARTITION BY venture,
                ds
                ORDER BY hh ASC
            ) AS sales_order_id_count,
            SUM(
                SUM(
                    CASE
                        WHEN platform IN ('App')
                        AND attr_model = 'lt_1d_p' THEN sales_order_id_count
                        ELSE 0
                    END
                )
            ) OVER (
                PARTITION BY venture,
                ds
                ORDER BY hh ASC
            ) AS app_sales_order_id_count,
            SUM(
                SUM(
                    CASE
                        WHEN platform IN ('App', 'Pc', 'Wap', 'Unknown')
                        AND attr_model = 'lt_1d_p' THEN order_id_count
                        ELSE 0
                    END
                )
            ) OVER (
                PARTITION BY venture,
                ds
                ORDER BY hh ASC
            ) AS order_id_count,
            SUM(
                SUM(
                    CASE
                        WHEN platform IN ('App')
                        AND attr_model = 'lt_1d_p' THEN order_id_count
                        ELSE 0
                    END
                )
            ) OVER (
                PARTITION BY venture,
                ds
                ORDER BY hh ASC
            ) AS app_order_id_count,
            SUM(
                SUM(
                    CASE
                        WHEN platform IN ('App', 'Pc', 'Wap', 'Unknown')
                        AND attr_model = 'lt_1d_p' THEN gmv
                        ELSE 0
                    END
                )
            ) OVER (
                PARTITION BY venture,
                ds
                ORDER BY hh ASC
            ) AS gmv,
            SUM(
                SUM(
                    CASE
                        WHEN platform IN ('App')
                        AND attr_model = 'lt_1d_p' THEN gmv
                        ELSE 0
                    END
                )
            ) OVER (
                PARTITION BY venture,
                ds
                ORDER BY hh ASC
            ) AS app_gmv
        FROM lazada_ads.ads_lzd_mkt_rt_uam_campaign_hi
        WHERE 1 = 1
            AND funding_bucket IN ('CRM')
            AND attr_model IN ('ft_1d_np', 'lt_1d', 'lt_1d_p')
        GROUP BY ds,
            hh,
            venture,
            channel GROUPING SETS ((ds, hh, venture))
        UNION ALL
        SELECT ds,
            hh,
            venture,
            COALESCE(channel, "om") AS channel,
            SUM(
                SUM(
                    CASE
                        WHEN platform IN ('App', 'Pc', 'Wap', 'Unknown')
                        AND attr_model = 'lt_1d' THEN installs
                        ELSE 0
                    END
                )
            ) OVER (
                PARTITION BY venture,
                ds
                ORDER BY hh ASC
            ) AS installs,
            SUM(
                SUM(
                    CASE
                        WHEN platform IN ('App')
                        AND attr_model = 'lt_1d' THEN installs
                        ELSE 0
                    END
                )
            ) OVER (
                PARTITION BY venture,
                ds
                ORDER BY hh ASC
            ) AS app_installs,
            SUM(
                SUM(
                    CASE
                        WHEN platform IN ('App', 'Pc', 'Wap', 'Unknown')
                        AND attr_model = 'lt_1d' THEN reinstalls
                        ELSE 0
                    END
                )
            ) OVER (
                PARTITION BY venture,
                ds
                ORDER BY hh ASC
            ) AS reinstalls,
            SUM(
                SUM(
                    CASE
                        WHEN platform IN ('App')
                        AND attr_model = 'lt_1d' THEN reinstalls
                        ELSE 0
                    END
                )
            ) OVER (
                PARTITION BY venture,
                ds
                ORDER BY hh ASC
            ) AS app_reinstalls,
            SUM(
                SUM(
                    CASE
                        WHEN platform IN ('App', 'Pc', 'Wap', 'Unknown')
                        AND attr_model = 'ft_1d_np' THEN visits
                        ELSE 0
                    END
                )
            ) OVER (
                PARTITION BY venture,
                ds
                ORDER BY hh ASC
            ) AS visits,
            SUM(
                SUM(
                    CASE
                        WHEN platform IN ('App')
                        AND attr_model = 'ft_1d_np' THEN visits
                        ELSE 0
                    END
                )
            ) OVER (
                PARTITION BY venture,
                ds
                ORDER BY hh ASC
            ) AS app_visits,
            SUM(
                SUM(
                    CASE
                        WHEN platform IN ('App', 'Pc', 'Wap', 'Unknown')
                        AND attr_model = 'ft_1d_np' THEN dau
                        ELSE 0
                    END
                )
            ) OVER (
                PARTITION BY venture,
                ds
                ORDER BY hh ASC
            ) AS dau,
            SUM(
                SUM(
                    CASE
                        WHEN platform IN ('App')
                        AND attr_model = 'ft_1d_np' THEN dau
                        ELSE 0
                    END
                )
            ) OVER (
                PARTITION BY venture,
                ds
                ORDER BY hh ASC
            ) AS app_dau,
            SUM(
                SUM(
                    CASE
                        WHEN platform IN ('App', 'Pc', 'Wap', 'Unknown')
                        AND attr_model = 'lt_1d' THEN pdp_pv
                        ELSE 0
                    END
                )
            ) OVER (
                PARTITION BY venture,
                ds
                ORDER BY hh ASC
            ) AS pdp_pv,
            SUM(
                SUM(
                    CASE
                        WHEN platform IN ('App')
                        AND attr_model = 'lt_1d' THEN pdp_pv
                        ELSE 0
                    END
                )
            ) OVER (
                PARTITION BY venture,
                ds
                ORDER BY hh ASC
            ) AS app_pdp_pv,
            SUM(
                SUM(
                    CASE
                        WHEN platform IN ('App', 'Pc', 'Wap', 'Unknown')
                        AND attr_model = 'lt_1d' THEN pdp_uv
                        ELSE 0
                    END
                )
            ) OVER (
                PARTITION BY venture,
                ds
                ORDER BY hh ASC
            ) AS pdp_uv,
            SUM(
                SUM(
                    CASE
                        WHEN platform IN ('App')
                        AND attr_model = 'lt_1d' THEN pdp_uv
                        ELSE 0
                    END
                )
            ) OVER (
                PARTITION BY venture,
                ds
                ORDER BY hh ASC
            ) AS app_pdp_uv,
            SUM(
                SUM(
                    CASE
                        WHEN platform IN ('App', 'Pc', 'Wap', 'Unknown')
                        AND attr_model = 'lt_1d_p' THEN a2c
                        ELSE 0
                    END
                )
            ) OVER (
                PARTITION BY venture,
                ds
                ORDER BY hh ASC
            ) AS a2c,
            SUM(
                SUM(
                    CASE
                        WHEN platform IN ('App')
                        AND attr_model = 'lt_1d_p' THEN a2c
                        ELSE 0
                    END
                )
            ) OVER (
                PARTITION BY venture,
                ds
                ORDER BY hh ASC
            ) AS app_a2c,
            SUM(
                SUM(
                    CASE
                        WHEN platform IN ('App', 'Pc', 'Wap', 'Unknown')
                        AND attr_model = 'lt_1d_p' THEN a2c_uv
                        ELSE 0
                    END
                )
            ) OVER (
                PARTITION BY venture,
                ds
                ORDER BY hh ASC
            ) AS a2c_uv,
            SUM(
                SUM(
                    CASE
                        WHEN platform IN ('App')
                        AND attr_model = 'lt_1d_p' THEN a2c_uv
                        ELSE 0
                    END
                )
            ) OVER (
                PARTITION BY venture,
                ds
                ORDER BY hh ASC
            ) AS app_a2c_uv,
            SUM(
                SUM(
                    CASE
                        WHEN platform IN ('App', 'Pc', 'Wap', 'Unknown')
                        AND attr_model = 'lt_1d_p' THEN buyers
                        ELSE 0
                    END
                )
            ) OVER (
                PARTITION BY venture,
                ds
                ORDER BY hh ASC
            ) AS buyers,
            SUM(
                SUM(
                    CASE
                        WHEN platform IN ('App')
                        AND attr_model = 'lt_1d_p' THEN buyers
                        ELSE 0
                    END
                )
            ) OVER (
                PARTITION BY venture,
                ds
                ORDER BY hh ASC
            ) AS app_buyers,
            SUM(
                SUM(
                    CASE
                        WHEN platform IN ('App', 'Pc', 'Wap', 'Unknown')
                        AND attr_model = 'lt_1d_p' THEN new_buyers
                        ELSE 0
                    END
                )
            ) OVER (
                PARTITION BY venture,
                ds
                ORDER BY hh ASC
            ) AS new_buyers,
            SUM(
                SUM(
                    CASE
                        WHEN platform IN ('App')
                        AND attr_model = 'lt_1d_p' THEN new_buyers
                        ELSE 0
                    END
                )
            ) OVER (
                PARTITION BY venture,
                ds
                ORDER BY hh ASC
            ) AS app_new_buyers,
            SUM(
                SUM(
                    CASE
                        WHEN platform IN ('App', 'Pc', 'Wap', 'Unknown')
                        AND attr_model = 'lt_1d_p' THEN reacquired_buyers
                        ELSE 0
                    END
                )
            ) OVER (
                PARTITION BY venture,
                ds
                ORDER BY hh ASC
            ) AS reacquired_buyers,
            SUM(
                SUM(
                    CASE
                        WHEN platform IN ('App')
                        AND attr_model = 'lt_1d_p' THEN reacquired_buyers
                        ELSE 0
                    END
                )
            ) OVER (
                PARTITION BY venture,
                ds
                ORDER BY hh ASC
            ) AS app_reacquired_buyers,
            SUM(
                SUM(
                    CASE
                        WHEN platform IN ('App', 'Pc', 'Wap', 'Unknown')
                        AND attr_model = 'lt_1d_p' THEN sales_order_id_count
                        ELSE 0
                    END
                )
            ) OVER (
                PARTITION BY venture,
                ds
                ORDER BY hh ASC
            ) AS sales_order_id_count,
            SUM(
                SUM(
                    CASE
                        WHEN platform IN ('App')
                        AND attr_model = 'lt_1d_p' THEN sales_order_id_count
                        ELSE 0
                    END
                )
            ) OVER (
                PARTITION BY venture,
                ds
                ORDER BY hh ASC
            ) AS app_sales_order_id_count,
            SUM(
                SUM(
                    CASE
                        WHEN platform IN ('App', 'Pc', 'Wap', 'Unknown')
                        AND attr_model = 'lt_1d_p' THEN order_id_count
                        ELSE 0
                    END
                )
            ) OVER (
                PARTITION BY venture,
                ds
                ORDER BY hh ASC
            ) AS order_id_count,
            SUM(
                SUM(
                    CASE
                        WHEN platform IN ('App')
                        AND attr_model = 'lt_1d_p' THEN order_id_count
                        ELSE 0
                    END
                )
            ) OVER (
                PARTITION BY venture,
                ds
                ORDER BY hh ASC
            ) AS app_order_id_count,
            SUM(
                SUM(
                    CASE
                        WHEN platform IN ('App', 'Pc', 'Wap', 'Unknown')
                        AND attr_model = 'lt_1d_p' THEN gmv
                        ELSE 0
                    END
                )
            ) OVER (
                PARTITION BY venture,
                ds
                ORDER BY hh ASC
            ) AS gmv,
            SUM(
                SUM(
                    CASE
                        WHEN platform IN ('App')
                        AND attr_model = 'lt_1d_p' THEN gmv
                        ELSE 0
                    END
                )
            ) OVER (
                PARTITION BY venture,
                ds
                ORDER BY hh ASC
            ) AS app_gmv
        FROM lazada_ads.ads_lzd_mkt_rt_uam_campaign_hi
        WHERE 1 = 1
            AND funding_bucket IN ('Lazada OM')
            AND attr_model IN ('ft_1d_np', 'lt_1d', 'lt_1d_p')
        GROUP BY ds,
            hh,
            venture,
            channel GROUPING SETS ((ds, hh, venture))
        UNION ALL
        SELECT ds,
            hh,
            venture,
            COALESCE(channel, "ppc") AS channel,
            SUM(
                SUM(
                    CASE
                        WHEN platform IN ('App', 'Pc', 'Wap', 'Unknown')
                        AND attr_model = 'lt_1d' THEN installs
                        ELSE 0
                    END
                )
            ) OVER (
                PARTITION BY venture,
                ds
                ORDER BY hh ASC
            ) AS installs,
            SUM(
                SUM(
                    CASE
                        WHEN platform IN ('App')
                        AND attr_model = 'lt_1d' THEN installs
                        ELSE 0
                    END
                )
            ) OVER (
                PARTITION BY venture,
                ds
                ORDER BY hh ASC
            ) AS app_installs,
            SUM(
                SUM(
                    CASE
                        WHEN platform IN ('App', 'Pc', 'Wap', 'Unknown')
                        AND attr_model = 'lt_1d' THEN reinstalls
                        ELSE 0
                    END
                )
            ) OVER (
                PARTITION BY venture,
                ds
                ORDER BY hh ASC
            ) AS reinstalls,
            SUM(
                SUM(
                    CASE
                        WHEN platform IN ('App')
                        AND attr_model = 'lt_1d' THEN reinstalls
                        ELSE 0
                    END
                )
            ) OVER (
                PARTITION BY venture,
                ds
                ORDER BY hh ASC
            ) AS app_reinstalls,
            SUM(
                SUM(
                    CASE
                        WHEN platform IN ('App', 'Pc', 'Wap', 'Unknown')
                        AND attr_model = 'ft_1d_np' THEN visits
                        ELSE 0
                    END
                )
            ) OVER (
                PARTITION BY venture,
                ds
                ORDER BY hh ASC
            ) AS visits,
            SUM(
                SUM(
                    CASE
                        WHEN platform IN ('App')
                        AND attr_model = 'ft_1d_np' THEN visits
                        ELSE 0
                    END
                )
            ) OVER (
                PARTITION BY venture,
                ds
                ORDER BY hh ASC
            ) AS app_visits,
            SUM(
                SUM(
                    CASE
                        WHEN platform IN ('App', 'Pc', 'Wap', 'Unknown')
                        AND attr_model = 'ft_1d_np' THEN dau
                        ELSE 0
                    END
                )
            ) OVER (
                PARTITION BY venture,
                ds
                ORDER BY hh ASC
            ) AS dau,
            SUM(
                SUM(
                    CASE
                        WHEN platform IN ('App')
                        AND attr_model = 'ft_1d_np' THEN dau
                        ELSE 0
                    END
                )
            ) OVER (
                PARTITION BY venture,
                ds
                ORDER BY hh ASC
            ) AS app_dau,
            SUM(
                SUM(
                    CASE
                        WHEN platform IN ('App', 'Pc', 'Wap', 'Unknown')
                        AND attr_model = 'lt_1d' THEN pdp_pv
                        ELSE 0
                    END
                )
            ) OVER (
                PARTITION BY venture,
                ds
                ORDER BY hh ASC
            ) AS pdp_pv,
            SUM(
                SUM(
                    CASE
                        WHEN platform IN ('App')
                        AND attr_model = 'lt_1d' THEN pdp_pv
                        ELSE 0
                    END
                )
            ) OVER (
                PARTITION BY venture,
                ds
                ORDER BY hh ASC
            ) AS app_pdp_pv,
            SUM(
                SUM(
                    CASE
                        WHEN platform IN ('App', 'Pc', 'Wap', 'Unknown')
                        AND attr_model = 'lt_1d' THEN pdp_uv
                        ELSE 0
                    END
                )
            ) OVER (
                PARTITION BY venture,
                ds
                ORDER BY hh ASC
            ) AS pdp_uv,
            SUM(
                SUM(
                    CASE
                        WHEN platform IN ('App')
                        AND attr_model = 'lt_1d' THEN pdp_uv
                        ELSE 0
                    END
                )
            ) OVER (
                PARTITION BY venture,
                ds
                ORDER BY hh ASC
            ) AS app_pdp_uv,
            SUM(
                SUM(
                    CASE
                        WHEN platform IN ('App', 'Pc', 'Wap', 'Unknown')
                        AND attr_model = 'lt_1d_p' THEN a2c
                        ELSE 0
                    END
                )
            ) OVER (
                PARTITION BY venture,
                ds
                ORDER BY hh ASC
            ) AS a2c,
            SUM(
                SUM(
                    CASE
                        WHEN platform IN ('App')
                        AND attr_model = 'lt_1d_p' THEN a2c
                        ELSE 0
                    END
                )
            ) OVER (
                PARTITION BY venture,
                ds
                ORDER BY hh ASC
            ) AS app_a2c,
            SUM(
                SUM(
                    CASE
                        WHEN platform IN ('App', 'Pc', 'Wap', 'Unknown')
                        AND attr_model = 'lt_1d_p' THEN a2c_uv
                        ELSE 0
                    END
                )
            ) OVER (
                PARTITION BY venture,
                ds
                ORDER BY hh ASC
            ) AS a2c_uv,
            SUM(
                SUM(
                    CASE
                        WHEN platform IN ('App')
                        AND attr_model = 'lt_1d_p' THEN a2c_uv
                        ELSE 0
                    END
                )
            ) OVER (
                PARTITION BY venture,
                ds
                ORDER BY hh ASC
            ) AS app_a2c_uv,
            SUM(
                SUM(
                    CASE
                        WHEN platform IN ('App', 'Pc', 'Wap', 'Unknown')
                        AND attr_model = 'lt_1d_p' THEN buyers
                        ELSE 0
                    END
                )
            ) OVER (
                PARTITION BY venture,
                ds
                ORDER BY hh ASC
            ) AS buyers,
            SUM(
                SUM(
                    CASE
                        WHEN platform IN ('App')
                        AND attr_model = 'lt_1d_p' THEN buyers
                        ELSE 0
                    END
                )
            ) OVER (
                PARTITION BY venture,
                ds
                ORDER BY hh ASC
            ) AS app_buyers,
            SUM(
                SUM(
                    CASE
                        WHEN platform IN ('App', 'Pc', 'Wap', 'Unknown')
                        AND attr_model = 'lt_1d_p' THEN new_buyers
                        ELSE 0
                    END
                )
            ) OVER (
                PARTITION BY venture,
                ds
                ORDER BY hh ASC
            ) AS new_buyers,
            SUM(
                SUM(
                    CASE
                        WHEN platform IN ('App')
                        AND attr_model = 'lt_1d_p' THEN new_buyers
                        ELSE 0
                    END
                )
            ) OVER (
                PARTITION BY venture,
                ds
                ORDER BY hh ASC
            ) AS app_new_buyers,
            SUM(
                SUM(
                    CASE
                        WHEN platform IN ('App', 'Pc', 'Wap', 'Unknown')
                        AND attr_model = 'lt_1d_p' THEN reacquired_buyers
                        ELSE 0
                    END
                )
            ) OVER (
                PARTITION BY venture,
                ds
                ORDER BY hh ASC
            ) AS reacquired_buyers,
            SUM(
                SUM(
                    CASE
                        WHEN platform IN ('App')
                        AND attr_model = 'lt_1d_p' THEN reacquired_buyers
                        ELSE 0
                    END
                )
            ) OVER (
                PARTITION BY venture,
                ds
                ORDER BY hh ASC
            ) AS app_reacquired_buyers,
            SUM(
                SUM(
                    CASE
                        WHEN platform IN ('App', 'Pc', 'Wap', 'Unknown')
                        AND attr_model = 'lt_1d_p' THEN sales_order_id_count
                        ELSE 0
                    END
                )
            ) OVER (
                PARTITION BY venture,
                ds
                ORDER BY hh ASC
            ) AS sales_order_id_count,
            SUM(
                SUM(
                    CASE
                        WHEN platform IN ('App')
                        AND attr_model = 'lt_1d_p' THEN sales_order_id_count
                        ELSE 0
                    END
                )
            ) OVER (
                PARTITION BY venture,
                ds
                ORDER BY hh ASC
            ) AS app_sales_order_id_count,
            SUM(
                SUM(
                    CASE
                        WHEN platform IN ('App', 'Pc', 'Wap', 'Unknown')
                        AND attr_model = 'lt_1d_p' THEN order_id_count
                        ELSE 0
                    END
                )
            ) OVER (
                PARTITION BY venture,
                ds
                ORDER BY hh ASC
            ) AS order_id_count,
            SUM(
                SUM(
                    CASE
                        WHEN platform IN ('App')
                        AND attr_model = 'lt_1d_p' THEN order_id_count
                        ELSE 0
                    END
                )
            ) OVER (
                PARTITION BY venture,
                ds
                ORDER BY hh ASC
            ) AS app_order_id_count,
            SUM(
                SUM(
                    CASE
                        WHEN platform IN ('App', 'Pc', 'Wap', 'Unknown')
                        AND attr_model = 'lt_1d_p' THEN gmv
                        ELSE 0
                    END
                )
            ) OVER (
                PARTITION BY venture,
                ds
                ORDER BY hh ASC
            ) AS gmv,
            SUM(
                SUM(
                    CASE
                        WHEN platform IN ('App')
                        AND attr_model = 'lt_1d_p' THEN gmv
                        ELSE 0
                    END
                )
            ) OVER (
                PARTITION BY venture,
                ds
                ORDER BY hh ASC
            ) AS app_gmv
        FROM lazada_ads.ads_lzd_mkt_rt_uam_campaign_hi
        WHERE 1 = 1
            AND funding_bucket IN ('Lazada OM')
            AND channel IN ('Apple Search', 'Display', 'SEM', 'Social', 'TikTok')
            AND attr_model IN ('ft_1d_np', 'lt_1d', 'lt_1d_p')
        GROUP BY ds,
            hh,
            venture,
            channel GROUPING SETS ((ds, hh, venture))
        UNION ALL
        SELECT ds,
            hh,
            venture,
            COALESCE(channel, "affiliate") AS channel,
            SUM(
                SUM(
                    CASE
                        WHEN platform IN ('App', 'Pc', 'Wap', 'Unknown')
                        AND attr_model = 'lt_1d' THEN installs
                        ELSE 0
                    END
                )
            ) OVER (
                PARTITION BY venture,
                ds
                ORDER BY hh ASC
            ) AS installs,
            SUM(
                SUM(
                    CASE
                        WHEN platform IN ('App')
                        AND attr_model = 'lt_1d' THEN installs
                        ELSE 0
                    END
                )
            ) OVER (
                PARTITION BY venture,
                ds
                ORDER BY hh ASC
            ) AS app_installs,
            SUM(
                SUM(
                    CASE
                        WHEN platform IN ('App', 'Pc', 'Wap', 'Unknown')
                        AND attr_model = 'lt_1d' THEN reinstalls
                        ELSE 0
                    END
                )
            ) OVER (
                PARTITION BY venture,
                ds
                ORDER BY hh ASC
            ) AS reinstalls,
            SUM(
                SUM(
                    CASE
                        WHEN platform IN ('App')
                        AND attr_model = 'lt_1d' THEN reinstalls
                        ELSE 0
                    END
                )
            ) OVER (
                PARTITION BY venture,
                ds
                ORDER BY hh ASC
            ) AS app_reinstalls,
            SUM(
                SUM(
                    CASE
                        WHEN platform IN ('App', 'Pc', 'Wap', 'Unknown')
                        AND attr_model = 'ft_1d_np' THEN visits
                        ELSE 0
                    END
                )
            ) OVER (
                PARTITION BY venture,
                ds
                ORDER BY hh ASC
            ) AS visits,
            SUM(
                SUM(
                    CASE
                        WHEN platform IN ('App')
                        AND attr_model = 'ft_1d_np' THEN visits
                        ELSE 0
                    END
                )
            ) OVER (
                PARTITION BY venture,
                ds
                ORDER BY hh ASC
            ) AS app_visits,
            SUM(
                SUM(
                    CASE
                        WHEN platform IN ('App', 'Pc', 'Wap', 'Unknown')
                        AND attr_model = 'ft_1d_np' THEN dau
                        ELSE 0
                    END
                )
            ) OVER (
                PARTITION BY venture,
                ds
                ORDER BY hh ASC
            ) AS dau,
            SUM(
                SUM(
                    CASE
                        WHEN platform IN ('App')
                        AND attr_model = 'ft_1d_np' THEN dau
                        ELSE 0
                    END
                )
            ) OVER (
                PARTITION BY venture,
                ds
                ORDER BY hh ASC
            ) AS app_dau,
            SUM(
                SUM(
                    CASE
                        WHEN platform IN ('App', 'Pc', 'Wap', 'Unknown')
                        AND attr_model = 'lt_1d' THEN pdp_pv
                        ELSE 0
                    END
                )
            ) OVER (
                PARTITION BY venture,
                ds
                ORDER BY hh ASC
            ) AS pdp_pv,
            SUM(
                SUM(
                    CASE
                        WHEN platform IN ('App')
                        AND attr_model = 'lt_1d' THEN pdp_pv
                        ELSE 0
                    END
                )
            ) OVER (
                PARTITION BY venture,
                ds
                ORDER BY hh ASC
            ) AS app_pdp_pv,
            SUM(
                SUM(
                    CASE
                        WHEN platform IN ('App', 'Pc', 'Wap', 'Unknown')
                        AND attr_model = 'lt_1d' THEN pdp_uv
                        ELSE 0
                    END
                )
            ) OVER (
                PARTITION BY venture,
                ds
                ORDER BY hh ASC
            ) AS pdp_uv,
            SUM(
                SUM(
                    CASE
                        WHEN platform IN ('App')
                        AND attr_model = 'lt_1d' THEN pdp_uv
                        ELSE 0
                    END
                )
            ) OVER (
                PARTITION BY venture,
                ds
                ORDER BY hh ASC
            ) AS app_pdp_uv,
            SUM(
                SUM(
                    CASE
                        WHEN platform IN ('App', 'Pc', 'Wap', 'Unknown')
                        AND attr_model = 'lt_1d_p' THEN a2c
                        ELSE 0
                    END
                )
            ) OVER (
                PARTITION BY venture,
                ds
                ORDER BY hh ASC
            ) AS a2c,
            SUM(
                SUM(
                    CASE
                        WHEN platform IN ('App')
                        AND attr_model = 'lt_1d_p' THEN a2c
                        ELSE 0
                    END
                )
            ) OVER (
                PARTITION BY venture,
                ds
                ORDER BY hh ASC
            ) AS app_a2c,
            SUM(
                SUM(
                    CASE
                        WHEN platform IN ('App', 'Pc', 'Wap', 'Unknown')
                        AND attr_model = 'lt_1d_p' THEN a2c_uv
                        ELSE 0
                    END
                )
            ) OVER (
                PARTITION BY venture,
                ds
                ORDER BY hh ASC
            ) AS a2c_uv,
            SUM(
                SUM(
                    CASE
                        WHEN platform IN ('App')
                        AND attr_model = 'lt_1d_p' THEN a2c_uv
                        ELSE 0
                    END
                )
            ) OVER (
                PARTITION BY venture,
                ds
                ORDER BY hh ASC
            ) AS app_a2c_uv,
            SUM(
                SUM(
                    CASE
                        WHEN platform IN ('App', 'Pc', 'Wap', 'Unknown')
                        AND attr_model = 'lt_1d_p' THEN buyers
                        ELSE 0
                    END
                )
            ) OVER (
                PARTITION BY venture,
                ds
                ORDER BY hh ASC
            ) AS buyers,
            SUM(
                SUM(
                    CASE
                        WHEN platform IN ('App')
                        AND attr_model = 'lt_1d_p' THEN buyers
                        ELSE 0
                    END
                )
            ) OVER (
                PARTITION BY venture,
                ds
                ORDER BY hh ASC
            ) AS app_buyers,
            SUM(
                SUM(
                    CASE
                        WHEN platform IN ('App', 'Pc', 'Wap', 'Unknown')
                        AND attr_model = 'lt_1d_p' THEN new_buyers
                        ELSE 0
                    END
                )
            ) OVER (
                PARTITION BY venture,
                ds
                ORDER BY hh ASC
            ) AS new_buyers,
            SUM(
                SUM(
                    CASE
                        WHEN platform IN ('App')
                        AND attr_model = 'lt_1d_p' THEN new_buyers
                        ELSE 0
                    END
                )
            ) OVER (
                PARTITION BY venture,
                ds
                ORDER BY hh ASC
            ) AS app_new_buyers,
            SUM(
                SUM(
                    CASE
                        WHEN platform IN ('App', 'Pc', 'Wap', 'Unknown')
                        AND attr_model = 'lt_1d_p' THEN reacquired_buyers
                        ELSE 0
                    END
                )
            ) OVER (
                PARTITION BY venture,
                ds
                ORDER BY hh ASC
            ) AS reacquired_buyers,
            SUM(
                SUM(
                    CASE
                        WHEN platform IN ('App')
                        AND attr_model = 'lt_1d_p' THEN reacquired_buyers
                        ELSE 0
                    END
                )
            ) OVER (
                PARTITION BY venture,
                ds
                ORDER BY hh ASC
            ) AS app_reacquired_buyers,
            SUM(
                SUM(
                    CASE
                        WHEN platform IN ('App', 'Pc', 'Wap', 'Unknown')
                        AND attr_model = 'lt_1d_p' THEN sales_order_id_count
                        ELSE 0
                    END
                )
            ) OVER (
                PARTITION BY venture,
                ds
                ORDER BY hh ASC
            ) AS sales_order_id_count,
            SUM(
                SUM(
                    CASE
                        WHEN platform IN ('App')
                        AND attr_model = 'lt_1d_p' THEN sales_order_id_count
                        ELSE 0
                    END
                )
            ) OVER (
                PARTITION BY venture,
                ds
                ORDER BY hh ASC
            ) AS app_sales_order_id_count,
            SUM(
                SUM(
                    CASE
                        WHEN platform IN ('App', 'Pc', 'Wap', 'Unknown')
                        AND attr_model = 'lt_1d_p' THEN order_id_count
                        ELSE 0
                    END
                )
            ) OVER (
                PARTITION BY venture,
                ds
                ORDER BY hh ASC
            ) AS order_id_count,
            SUM(
                SUM(
                    CASE
                        WHEN platform IN ('App')
                        AND attr_model = 'lt_1d_p' THEN order_id_count
                        ELSE 0
                    END
                )
            ) OVER (
                PARTITION BY venture,
                ds
                ORDER BY hh ASC
            ) AS app_order_id_count,
            SUM(
                SUM(
                    CASE
                        WHEN platform IN ('App', 'Pc', 'Wap', 'Unknown')
                        AND attr_model = 'lt_1d_p' THEN gmv
                        ELSE 0
                    END
                )
            ) OVER (
                PARTITION BY venture,
                ds
                ORDER BY hh ASC
            ) AS gmv,
            SUM(
                SUM(
                    CASE
                        WHEN platform IN ('App')
                        AND attr_model = 'lt_1d_p' THEN gmv
                        ELSE 0
                    END
                )
            ) OVER (
                PARTITION BY venture,
                ds
                ORDER BY hh ASC
            ) AS app_gmv
        FROM lazada_ads.ads_lzd_mkt_rt_uam_campaign_hi
        WHERE 1 = 1
            AND funding_bucket IN ('Lazada OM')
            AND channel IN ('CPS Affiliate', 'CPI Affiliate')
            AND attr_model IN ('ft_1d_np', 'lt_1d', 'lt_1d_p')
        GROUP BY ds,
            hh,
            venture,
            channel GROUPING SETS ((ds, hh, venture))
        UNION ALL
        SELECT ds,
            hh,
            venture,
            COALESCE(channel, "rta") AS channel,
            SUM(
                SUM(
                    CASE
                        WHEN platform IN ('App', 'Pc', 'Wap', 'Unknown')
                        AND attr_model = 'lt_1d' THEN installs
                        ELSE 0
                    END
                )
            ) OVER (
                PARTITION BY venture,
                ds
                ORDER BY hh ASC
            ) AS installs,
            SUM(
                SUM(
                    CASE
                        WHEN platform IN ('App')
                        AND attr_model = 'lt_1d' THEN installs
                        ELSE 0
                    END
                )
            ) OVER (
                PARTITION BY venture,
                ds
                ORDER BY hh ASC
            ) AS app_installs,
            SUM(
                SUM(
                    CASE
                        WHEN platform IN ('App', 'Pc', 'Wap', 'Unknown')
                        AND attr_model = 'lt_1d' THEN reinstalls
                        ELSE 0
                    END
                )
            ) OVER (
                PARTITION BY venture,
                ds
                ORDER BY hh ASC
            ) AS reinstalls,
            SUM(
                SUM(
                    CASE
                        WHEN platform IN ('App')
                        AND attr_model = 'lt_1d' THEN reinstalls
                        ELSE 0
                    END
                )
            ) OVER (
                PARTITION BY venture,
                ds
                ORDER BY hh ASC
            ) AS app_reinstalls,
            SUM(
                SUM(
                    CASE
                        WHEN platform IN ('App', 'Pc', 'Wap', 'Unknown')
                        AND attr_model = 'ft_1d_np' THEN visits
                        ELSE 0
                    END
                )
            ) OVER (
                PARTITION BY venture,
                ds
                ORDER BY hh ASC
            ) AS visits,
            SUM(
                SUM(
                    CASE
                        WHEN platform IN ('App')
                        AND attr_model = 'ft_1d_np' THEN visits
                        ELSE 0
                    END
                )
            ) OVER (
                PARTITION BY venture,
                ds
                ORDER BY hh ASC
            ) AS app_visits,
            SUM(
                SUM(
                    CASE
                        WHEN platform IN ('App', 'Pc', 'Wap', 'Unknown')
                        AND attr_model = 'ft_1d_np' THEN dau
                        ELSE 0
                    END
                )
            ) OVER (
                PARTITION BY venture,
                ds
                ORDER BY hh ASC
            ) AS dau,
            SUM(
                SUM(
                    CASE
                        WHEN platform IN ('App')
                        AND attr_model = 'ft_1d_np' THEN dau
                        ELSE 0
                    END
                )
            ) OVER (
                PARTITION BY venture,
                ds
                ORDER BY hh ASC
            ) AS app_dau,
            SUM(
                SUM(
                    CASE
                        WHEN platform IN ('App', 'Pc', 'Wap', 'Unknown')
                        AND attr_model = 'lt_1d' THEN pdp_pv
                        ELSE 0
                    END
                )
            ) OVER (
                PARTITION BY venture,
                ds
                ORDER BY hh ASC
            ) AS pdp_pv,
            SUM(
                SUM(
                    CASE
                        WHEN platform IN ('App')
                        AND attr_model = 'lt_1d' THEN pdp_pv
                        ELSE 0
                    END
                )
            ) OVER (
                PARTITION BY venture,
                ds
                ORDER BY hh ASC
            ) AS app_pdp_pv,
            SUM(
                SUM(
                    CASE
                        WHEN platform IN ('App', 'Pc', 'Wap', 'Unknown')
                        AND attr_model = 'lt_1d' THEN pdp_uv
                        ELSE 0
                    END
                )
            ) OVER (
                PARTITION BY venture,
                ds
                ORDER BY hh ASC
            ) AS pdp_uv,
            SUM(
                SUM(
                    CASE
                        WHEN platform IN ('App')
                        AND attr_model = 'lt_1d' THEN pdp_uv
                        ELSE 0
                    END
                )
            ) OVER (
                PARTITION BY venture,
                ds
                ORDER BY hh ASC
            ) AS app_pdp_uv,
            SUM(
                SUM(
                    CASE
                        WHEN platform IN ('App', 'Pc', 'Wap', 'Unknown')
                        AND attr_model = 'lt_1d_p' THEN a2c
                        ELSE 0
                    END
                )
            ) OVER (
                PARTITION BY venture,
                ds
                ORDER BY hh ASC
            ) AS a2c,
            SUM(
                SUM(
                    CASE
                        WHEN platform IN ('App')
                        AND attr_model = 'lt_1d_p' THEN a2c
                        ELSE 0
                    END
                )
            ) OVER (
                PARTITION BY venture,
                ds
                ORDER BY hh ASC
            ) AS app_a2c,
            SUM(
                SUM(
                    CASE
                        WHEN platform IN ('App', 'Pc', 'Wap', 'Unknown')
                        AND attr_model = 'lt_1d_p' THEN a2c_uv
                        ELSE 0
                    END
                )
            ) OVER (
                PARTITION BY venture,
                ds
                ORDER BY hh ASC
            ) AS a2c_uv,
            SUM(
                SUM(
                    CASE
                        WHEN platform IN ('App')
                        AND attr_model = 'lt_1d_p' THEN a2c_uv
                        ELSE 0
                    END
                )
            ) OVER (
                PARTITION BY venture,
                ds
                ORDER BY hh ASC
            ) AS app_a2c_uv,
            SUM(
                SUM(
                    CASE
                        WHEN platform IN ('App', 'Pc', 'Wap', 'Unknown')
                        AND attr_model = 'lt_1d_p' THEN buyers
                        ELSE 0
                    END
                )
            ) OVER (
                PARTITION BY venture,
                ds
                ORDER BY hh ASC
            ) AS buyers,
            SUM(
                SUM(
                    CASE
                        WHEN platform IN ('App')
                        AND attr_model = 'lt_1d_p' THEN buyers
                        ELSE 0
                    END
                )
            ) OVER (
                PARTITION BY venture,
                ds
                ORDER BY hh ASC
            ) AS app_buyers,
            SUM(
                SUM(
                    CASE
                        WHEN platform IN ('App', 'Pc', 'Wap', 'Unknown')
                        AND attr_model = 'lt_1d_p' THEN new_buyers
                        ELSE 0
                    END
                )
            ) OVER (
                PARTITION BY venture,
                ds
                ORDER BY hh ASC
            ) AS new_buyers,
            SUM(
                SUM(
                    CASE
                        WHEN platform IN ('App')
                        AND attr_model = 'lt_1d_p' THEN new_buyers
                        ELSE 0
                    END
                )
            ) OVER (
                PARTITION BY venture,
                ds
                ORDER BY hh ASC
            ) AS app_new_buyers,
            SUM(
                SUM(
                    CASE
                        WHEN platform IN ('App', 'Pc', 'Wap', 'Unknown')
                        AND attr_model = 'lt_1d_p' THEN reacquired_buyers
                        ELSE 0
                    END
                )
            ) OVER (
                PARTITION BY venture,
                ds
                ORDER BY hh ASC
            ) AS reacquired_buyers,
            SUM(
                SUM(
                    CASE
                        WHEN platform IN ('App')
                        AND attr_model = 'lt_1d_p' THEN reacquired_buyers
                        ELSE 0
                    END
                )
            ) OVER (
                PARTITION BY venture,
                ds
                ORDER BY hh ASC
            ) AS app_reacquired_buyers,
            SUM(
                SUM(
                    CASE
                        WHEN platform IN ('App', 'Pc', 'Wap', 'Unknown')
                        AND attr_model = 'lt_1d_p' THEN sales_order_id_count
                        ELSE 0
                    END
                )
            ) OVER (
                PARTITION BY venture,
                ds
                ORDER BY hh ASC
            ) AS sales_order_id_count,
            SUM(
                SUM(
                    CASE
                        WHEN platform IN ('App')
                        AND attr_model = 'lt_1d_p' THEN sales_order_id_count
                        ELSE 0
                    END
                )
            ) OVER (
                PARTITION BY venture,
                ds
                ORDER BY hh ASC
            ) AS app_sales_order_id_count,
            SUM(
                SUM(
                    CASE
                        WHEN platform IN ('App', 'Pc', 'Wap', 'Unknown')
                        AND attr_model = 'lt_1d_p' THEN order_id_count
                        ELSE 0
                    END
                )
            ) OVER (
                PARTITION BY venture,
                ds
                ORDER BY hh ASC
            ) AS order_id_count,
            SUM(
                SUM(
                    CASE
                        WHEN platform IN ('App')
                        AND attr_model = 'lt_1d_p' THEN order_id_count
                        ELSE 0
                    END
                )
            ) OVER (
                PARTITION BY venture,
                ds
                ORDER BY hh ASC
            ) AS app_order_id_count,
            SUM(
                SUM(
                    CASE
                        WHEN platform IN ('App', 'Pc', 'Wap', 'Unknown')
                        AND attr_model = 'lt_1d_p' THEN gmv
                        ELSE 0
                    END
                )
            ) OVER (
                PARTITION BY venture,
                ds
                ORDER BY hh ASC
            ) AS gmv,
            SUM(
                SUM(
                    CASE
                        WHEN platform IN ('App')
                        AND attr_model = 'lt_1d_p' THEN gmv
                        ELSE 0
                    END
                )
            ) OVER (
                PARTITION BY venture,
                ds
                ORDER BY hh ASC
            ) AS app_gmv
        FROM lazada_ads.ads_lzd_mkt_rt_uam_campaign_hi
        WHERE 1 = 1
            AND funding_bucket IN ('Lazada OM')
            AND channel IN ('RTA')
            AND attr_model IN ('ft_1d_np', 'lt_1d', 'lt_1d_p')
        GROUP BY ds,
            hh,
            venture,
            channel GROUPING SETS ((ds, hh, venture))
    )
WHERE 1 = 1
GROUP BY ds,
    hh,
    venture
ORDER BY ds DESC,
    hh DESC;
SELECT CONCAT(
        SUBSTR(ds, 1, 4),
        '-',
        SUBSTR(ds, 5, 2),
        '-',
        SUBSTR(ds, 7, 2)
    ) AS ds,
    hh,
    venture,
    COALESCE(
        SUM(
            CASE
                WHEN channel IN ('total') THEN installs
                ELSE 0
            END
        ),
        0
    ) AS total_installs --<< Start installs, app_installs
,
    COALESCE(
        SUM(
            CASE
                WHEN channel IN ('total') THEN app_installs
                ELSE 0
            END
        ),
        0
    ) AS total_app_installs,
    COALESCE(
        SUM(
            CASE
                WHEN channel IN ('organic') THEN installs
                ELSE 0
            END
        ),
        0
    ) AS organic_installs,
    COALESCE(
        SUM(
            CASE
                WHEN channel IN ('organic') THEN app_installs
                ELSE 0
            END
        ),
        0
    ) AS organic_app_installs,
    COALESCE(
        SUM(
            CASE
                WHEN channel IN ('mkt') THEN installs
                ELSE 0
            END
        ),
        0
    ) AS mkt_installs,
    COALESCE(
        SUM(
            CASE
                WHEN channel IN ('mkt') THEN app_installs
                ELSE 0
            END
        ),
        0
    ) AS mkt_app_installs,
    COALESCE(
        SUM(
            CASE
                WHEN channel IN ('other_mkt') THEN installs
                ELSE 0
            END
        ),
        0
    ) AS other_mkt_installs,
    COALESCE(
        SUM(
            CASE
                WHEN channel IN ('other_mkt') THEN app_installs
                ELSE 0
            END
        ),
        0
    ) AS other_mkt_app_installs,
    COALESCE(
        SUM(
            CASE
                WHEN channel IN ('crm') THEN installs
                ELSE 0
            END
        ),
        0
    ) AS crm_installs,
    COALESCE(
        SUM(
            CASE
                WHEN channel IN ('crm') THEN app_installs
                ELSE 0
            END
        ),
        0
    ) AS crm_app_installs,
    COALESCE(
        SUM(
            CASE
                WHEN channel IN ('om') THEN installs
                ELSE 0
            END
        ),
        0
    ) AS om_installs,
    COALESCE(
        SUM(
            CASE
                WHEN channel IN ('om') THEN app_installs
                ELSE 0
            END
        ),
        0
    ) AS om_app_installs,
    COALESCE(
        SUM(
            CASE
                WHEN channel IN ('ppc') THEN installs
                ELSE 0
            END
        ),
        0
    ) AS ppc_installs,
    COALESCE(
        SUM(
            CASE
                WHEN channel IN ('ppc') THEN app_installs
                ELSE 0
            END
        ),
        0
    ) AS ppc_app_installs,
    COALESCE(
        SUM(
            CASE
                WHEN channel IN ('affiliate') THEN installs
                ELSE 0
            END
        ),
        0
    ) AS affiliate_installs,
    COALESCE(
        SUM(
            CASE
                WHEN channel IN ('affiliate') THEN app_installs
                ELSE 0
            END
        ),
        0
    ) AS affiliate_app_installs,
    COALESCE(
        SUM(
            CASE
                WHEN channel IN ('rta') THEN installs
                ELSE 0
            END
        ),
        0
    ) AS rta_installs,
    COALESCE(
        SUM(
            CASE
                WHEN channel IN ('rta') THEN app_installs
                ELSE 0
            END
        ),
        0
    ) AS rta_app_installs --<< End
,
    COALESCE(
        SUM(
            CASE
                WHEN channel IN ('total') THEN reinstalls
                ELSE 0
            END
        ),
        0
    ) AS total_reinstalls --<< Start rereinstalls, app_rereinstalls
,
    COALESCE(
        SUM(
            CASE
                WHEN channel IN ('total') THEN app_reinstalls
                ELSE 0
            END
        ),
        0
    ) AS total_app_reinstalls,
    COALESCE(
        SUM(
            CASE
                WHEN channel IN ('organic') THEN reinstalls
                ELSE 0
            END
        ),
        0
    ) AS organic_reinstalls,
    COALESCE(
        SUM(
            CASE
                WHEN channel IN ('organic') THEN app_reinstalls
                ELSE 0
            END
        ),
        0
    ) AS organic_app_reinstalls,
    COALESCE(
        SUM(
            CASE
                WHEN channel IN ('mkt') THEN reinstalls
                ELSE 0
            END
        ),
        0
    ) AS mkt_reinstalls,
    COALESCE(
        SUM(
            CASE
                WHEN channel IN ('mkt') THEN app_reinstalls
                ELSE 0
            END
        ),
        0
    ) AS mkt_app_reinstalls,
    COALESCE(
        SUM(
            CASE
                WHEN channel IN ('other_mkt') THEN reinstalls
                ELSE 0
            END
        ),
        0
    ) AS other_mkt_reinstalls,
    COALESCE(
        SUM(
            CASE
                WHEN channel IN ('other_mkt') THEN app_reinstalls
                ELSE 0
            END
        ),
        0
    ) AS other_mkt_app_reinstalls,
    COALESCE(
        SUM(
            CASE
                WHEN channel IN ('crm') THEN reinstalls
                ELSE 0
            END
        ),
        0
    ) AS crm_reinstalls,
    COALESCE(
        SUM(
            CASE
                WHEN channel IN ('crm') THEN app_reinstalls
                ELSE 0
            END
        ),
        0
    ) AS crm_app_reinstalls,
    COALESCE(
        SUM(
            CASE
                WHEN channel IN ('om') THEN reinstalls
                ELSE 0
            END
        ),
        0
    ) AS om_reinstalls,
    COALESCE(
        SUM(
            CASE
                WHEN channel IN ('om') THEN app_reinstalls
                ELSE 0
            END
        ),
        0
    ) AS om_app_reinstalls,
    COALESCE(
        SUM(
            CASE
                WHEN channel IN ('ppc') THEN reinstalls
                ELSE 0
            END
        ),
        0
    ) AS ppc_reinstalls,
    COALESCE(
        SUM(
            CASE
                WHEN channel IN ('ppc') THEN app_reinstalls
                ELSE 0
            END
        ),
        0
    ) AS ppc_app_reinstalls,
    COALESCE(
        SUM(
            CASE
                WHEN channel IN ('affiliate') THEN reinstalls
                ELSE 0
            END
        ),
        0
    ) AS affiliate_reinstalls,
    COALESCE(
        SUM(
            CASE
                WHEN channel IN ('affiliate') THEN app_reinstalls
                ELSE 0
            END
        ),
        0
    ) AS affiliate_app_reinstalls,
    COALESCE(
        SUM(
            CASE
                WHEN channel IN ('rta') THEN reinstalls
                ELSE 0
            END
        ),
        0
    ) AS rta_reinstalls,
    COALESCE(
        SUM(
            CASE
                WHEN channel IN ('rta') THEN app_reinstalls
                ELSE 0
            END
        ),
        0
    ) AS rta_app_reinstalls --<< End
,
    COALESCE(
        SUM(
            CASE
                WHEN channel IN ('total') THEN visits
                ELSE 0
            END
        ),
        0
    ) AS total_visits --<< Start visits, app_visits
,
    COALESCE(
        SUM(
            CASE
                WHEN channel IN ('total') THEN app_visits
                ELSE 0
            END
        ),
        0
    ) AS total_app_visits,
    COALESCE(
        SUM(
            CASE
                WHEN channel IN ('organic') THEN visits
                ELSE 0
            END
        ),
        0
    ) AS organic_visits,
    COALESCE(
        SUM(
            CASE
                WHEN channel IN ('organic') THEN app_visits
                ELSE 0
            END
        ),
        0
    ) AS organic_app_visits,
    COALESCE(
        SUM(
            CASE
                WHEN channel IN ('mkt') THEN visits
                ELSE 0
            END
        ),
        0
    ) AS mkt_visits,
    COALESCE(
        SUM(
            CASE
                WHEN channel IN ('mkt') THEN app_visits
                ELSE 0
            END
        ),
        0
    ) AS mkt_app_visits,
    COALESCE(
        SUM(
            CASE
                WHEN channel IN ('other_mkt') THEN visits
                ELSE 0
            END
        ),
        0
    ) AS other_mkt_visits,
    COALESCE(
        SUM(
            CASE
                WHEN channel IN ('other_mkt') THEN app_visits
                ELSE 0
            END
        ),
        0
    ) AS other_mkt_app_visits,
    COALESCE(
        SUM(
            CASE
                WHEN channel IN ('crm') THEN visits
                ELSE 0
            END
        ),
        0
    ) AS crm_visits,
    COALESCE(
        SUM(
            CASE
                WHEN channel IN ('crm') THEN app_visits
                ELSE 0
            END
        ),
        0
    ) AS crm_app_visits,
    COALESCE(
        SUM(
            CASE
                WHEN channel IN ('om') THEN visits
                ELSE 0
            END
        ),
        0
    ) AS om_visits,
    COALESCE(
        SUM(
            CASE
                WHEN channel IN ('om') THEN app_visits
                ELSE 0
            END
        ),
        0
    ) AS om_app_visits,
    COALESCE(
        SUM(
            CASE
                WHEN channel IN ('ppc') THEN visits
                ELSE 0
            END
        ),
        0
    ) AS ppc_visits,
    COALESCE(
        SUM(
            CASE
                WHEN channel IN ('ppc') THEN app_visits
                ELSE 0
            END
        ),
        0
    ) AS ppc_app_visits,
    COALESCE(
        SUM(
            CASE
                WHEN channel IN ('affiliate') THEN visits
                ELSE 0
            END
        ),
        0
    ) AS affiliate_visits,
    COALESCE(
        SUM(
            CASE
                WHEN channel IN ('affiliate') THEN app_visits
                ELSE 0
            END
        ),
        0
    ) AS affiliate_app_visits,
    COALESCE(
        SUM(
            CASE
                WHEN channel IN ('rta') THEN visits
                ELSE 0
            END
        ),
        0
    ) AS rta_visits,
    COALESCE(
        SUM(
            CASE
                WHEN channel IN ('rta') THEN app_visits
                ELSE 0
            END
        ),
        0
    ) AS rta_app_visits --<< End
,
    COALESCE(
        SUM(
            CASE
                WHEN channel IN ('total') THEN dau
                ELSE 0
            END
        ),
        0
    ) AS total_dau --<< Start dau, app_dau
,
    COALESCE(
        SUM(
            CASE
                WHEN channel IN ('total') THEN app_dau
                ELSE 0
            END
        ),
        0
    ) AS total_app_dau,
    COALESCE(
        SUM(
            CASE
                WHEN channel IN ('organic') THEN dau
                ELSE 0
            END
        ),
        0
    ) AS organic_dau,
    COALESCE(
        SUM(
            CASE
                WHEN channel IN ('organic') THEN app_dau
                ELSE 0
            END
        ),
        0
    ) AS organic_app_dau,
    COALESCE(
        SUM(
            CASE
                WHEN channel IN ('mkt') THEN dau
                ELSE 0
            END
        ),
        0
    ) AS mkt_dau,
    COALESCE(
        SUM(
            CASE
                WHEN channel IN ('mkt') THEN app_dau
                ELSE 0
            END
        ),
        0
    ) AS mkt_app_dau,
    COALESCE(
        SUM(
            CASE
                WHEN channel IN ('other_mkt') THEN dau
                ELSE 0
            END
        ),
        0
    ) AS other_mkt_dau,
    COALESCE(
        SUM(
            CASE
                WHEN channel IN ('other_mkt') THEN app_dau
                ELSE 0
            END
        ),
        0
    ) AS other_mkt_app_dau,
    COALESCE(
        SUM(
            CASE
                WHEN channel IN ('crm') THEN dau
                ELSE 0
            END
        ),
        0
    ) AS crm_dau,
    COALESCE(
        SUM(
            CASE
                WHEN channel IN ('crm') THEN app_dau
                ELSE 0
            END
        ),
        0
    ) AS crm_app_dau,
    COALESCE(
        SUM(
            CASE
                WHEN channel IN ('om') THEN dau
                ELSE 0
            END
        ),
        0
    ) AS om_dau,
    COALESCE(
        SUM(
            CASE
                WHEN channel IN ('om') THEN app_dau
                ELSE 0
            END
        ),
        0
    ) AS om_app_dau,
    COALESCE(
        SUM(
            CASE
                WHEN channel IN ('ppc') THEN dau
                ELSE 0
            END
        ),
        0
    ) AS ppc_dau,
    COALESCE(
        SUM(
            CASE
                WHEN channel IN ('ppc') THEN app_dau
                ELSE 0
            END
        ),
        0
    ) AS ppc_app_dau,
    COALESCE(
        SUM(
            CASE
                WHEN channel IN ('affiliate') THEN dau
                ELSE 0
            END
        ),
        0
    ) AS affiliate_dau,
    COALESCE(
        SUM(
            CASE
                WHEN channel IN ('affiliate') THEN app_dau
                ELSE 0
            END
        ),
        0
    ) AS affiliate_app_dau,
    COALESCE(
        SUM(
            CASE
                WHEN channel IN ('rta') THEN dau
                ELSE 0
            END
        ),
        0
    ) AS rta_dau,
    COALESCE(
        SUM(
            CASE
                WHEN channel IN ('rta') THEN app_dau
                ELSE 0
            END
        ),
        0
    ) AS rta_app_dau --<< End
,
    COALESCE(
        SUM(
            CASE
                WHEN channel IN ('total') THEN pdp_pv
                ELSE 0
            END
        ),
        0
    ) AS total_pdp_pv --<< Start pdp_pv, app_pdp_pv
,
    COALESCE(
        SUM(
            CASE
                WHEN channel IN ('total') THEN app_pdp_pv
                ELSE 0
            END
        ),
        0
    ) AS total_app_pdp_pv,
    COALESCE(
        SUM(
            CASE
                WHEN channel IN ('organic') THEN pdp_pv
                ELSE 0
            END
        ),
        0
    ) AS organic_pdp_pv,
    COALESCE(
        SUM(
            CASE
                WHEN channel IN ('organic') THEN app_pdp_pv
                ELSE 0
            END
        ),
        0
    ) AS organic_app_pdp_pv,
    COALESCE(
        SUM(
            CASE
                WHEN channel IN ('mkt') THEN pdp_pv
                ELSE 0
            END
        ),
        0
    ) AS mkt_pdp_pv,
    COALESCE(
        SUM(
            CASE
                WHEN channel IN ('mkt') THEN app_pdp_pv
                ELSE 0
            END
        ),
        0
    ) AS mkt_app_pdp_pv,
    COALESCE(
        SUM(
            CASE
                WHEN channel IN ('other_mkt') THEN pdp_pv
                ELSE 0
            END
        ),
        0
    ) AS other_mkt_pdp_pv,
    COALESCE(
        SUM(
            CASE
                WHEN channel IN ('other_mkt') THEN app_pdp_pv
                ELSE 0
            END
        ),
        0
    ) AS other_mkt_app_pdp_pv,
    COALESCE(
        SUM(
            CASE
                WHEN channel IN ('crm') THEN pdp_pv
                ELSE 0
            END
        ),
        0
    ) AS crm_pdp_pv,
    COALESCE(
        SUM(
            CASE
                WHEN channel IN ('crm') THEN app_pdp_pv
                ELSE 0
            END
        ),
        0
    ) AS crm_app_pdp_pv,
    COALESCE(
        SUM(
            CASE
                WHEN channel IN ('om') THEN pdp_pv
                ELSE 0
            END
        ),
        0
    ) AS om_pdp_pv,
    COALESCE(
        SUM(
            CASE
                WHEN channel IN ('om') THEN app_pdp_pv
                ELSE 0
            END
        ),
        0
    ) AS om_app_pdp_pv,
    COALESCE(
        SUM(
            CASE
                WHEN channel IN ('ppc') THEN pdp_pv
                ELSE 0
            END
        ),
        0
    ) AS ppc_pdp_pv,
    COALESCE(
        SUM(
            CASE
                WHEN channel IN ('ppc') THEN app_pdp_pv
                ELSE 0
            END
        ),
        0
    ) AS ppc_app_pdp_pv,
    COALESCE(
        SUM(
            CASE
                WHEN channel IN ('affiliate') THEN pdp_pv
                ELSE 0
            END
        ),
        0
    ) AS affiliate_pdp_pv,
    COALESCE(
        SUM(
            CASE
                WHEN channel IN ('affiliate') THEN app_pdp_pv
                ELSE 0
            END
        ),
        0
    ) AS affiliate_app_pdp_pv,
    COALESCE(
        SUM(
            CASE
                WHEN channel IN ('rta') THEN pdp_pv
                ELSE 0
            END
        ),
        0
    ) AS rta_pdp_pv,
    COALESCE(
        SUM(
            CASE
                WHEN channel IN ('rta') THEN app_pdp_pv
                ELSE 0
            END
        ),
        0
    ) AS rta_app_pdp_pv --<< End
,
    COALESCE(
        SUM(
            CASE
                WHEN channel IN ('total') THEN pdp_uv
                ELSE 0
            END
        ),
        0
    ) AS total_pdp_uv --<< Start pdp_uv, app_pdp_uv
,
    COALESCE(
        SUM(
            CASE
                WHEN channel IN ('total') THEN app_pdp_uv
                ELSE 0
            END
        ),
        0
    ) AS total_app_pdp_uv,
    COALESCE(
        SUM(
            CASE
                WHEN channel IN ('organic') THEN pdp_uv
                ELSE 0
            END
        ),
        0
    ) AS organic_pdp_uv,
    COALESCE(
        SUM(
            CASE
                WHEN channel IN ('organic') THEN app_pdp_uv
                ELSE 0
            END
        ),
        0
    ) AS organic_app_pdp_uv,
    COALESCE(
        SUM(
            CASE
                WHEN channel IN ('mkt') THEN pdp_uv
                ELSE 0
            END
        ),
        0
    ) AS mkt_pdp_uv,
    COALESCE(
        SUM(
            CASE
                WHEN channel IN ('mkt') THEN app_pdp_uv
                ELSE 0
            END
        ),
        0
    ) AS mkt_app_pdp_uv,
    COALESCE(
        SUM(
            CASE
                WHEN channel IN ('other_mkt') THEN pdp_uv
                ELSE 0
            END
        ),
        0
    ) AS other_mkt_pdp_uv,
    COALESCE(
        SUM(
            CASE
                WHEN channel IN ('other_mkt') THEN app_pdp_uv
                ELSE 0
            END
        ),
        0
    ) AS other_mkt_app_pdp_uv,
    COALESCE(
        SUM(
            CASE
                WHEN channel IN ('crm') THEN pdp_uv
                ELSE 0
            END
        ),
        0
    ) AS crm_pdp_uv,
    COALESCE(
        SUM(
            CASE
                WHEN channel IN ('crm') THEN app_pdp_uv
                ELSE 0
            END
        ),
        0
    ) AS crm_app_pdp_uv,
    COALESCE(
        SUM(
            CASE
                WHEN channel IN ('om') THEN pdp_uv
                ELSE 0
            END
        ),
        0
    ) AS om_pdp_uv,
    COALESCE(
        SUM(
            CASE
                WHEN channel IN ('om') THEN app_pdp_uv
                ELSE 0
            END
        ),
        0
    ) AS om_app_pdp_uv,
    COALESCE(
        SUM(
            CASE
                WHEN channel IN ('ppc') THEN pdp_uv
                ELSE 0
            END
        ),
        0
    ) AS ppc_pdp_uv,
    COALESCE(
        SUM(
            CASE
                WHEN channel IN ('ppc') THEN app_pdp_uv
                ELSE 0
            END
        ),
        0
    ) AS ppc_app_pdp_uv,
    COALESCE(
        SUM(
            CASE
                WHEN channel IN ('affiliate') THEN pdp_uv
                ELSE 0
            END
        ),
        0
    ) AS affiliate_pdp_uv,
    COALESCE(
        SUM(
            CASE
                WHEN channel IN ('affiliate') THEN app_pdp_uv
                ELSE 0
            END
        ),
        0
    ) AS affiliate_app_pdp_uv,
    COALESCE(
        SUM(
            CASE
                WHEN channel IN ('rta') THEN pdp_uv
                ELSE 0
            END
        ),
        0
    ) AS rta_pdp_uv,
    COALESCE(
        SUM(
            CASE
                WHEN channel IN ('rta') THEN app_pdp_uv
                ELSE 0
            END
        ),
        0
    ) AS rta_app_pdp_uv --<< End
,
    COALESCE(
        SUM(
            CASE
                WHEN channel IN ('total') THEN a2c
                ELSE 0
            END
        ),
        0
    ) AS total_a2c --<< Start a2c, app_a2c
,
    COALESCE(
        SUM(
            CASE
                WHEN channel IN ('total') THEN app_a2c
                ELSE 0
            END
        ),
        0
    ) AS total_app_a2c,
    COALESCE(
        SUM(
            CASE
                WHEN channel IN ('organic') THEN a2c
                ELSE 0
            END
        ),
        0
    ) AS organic_a2c,
    COALESCE(
        SUM(
            CASE
                WHEN channel IN ('organic') THEN app_a2c
                ELSE 0
            END
        ),
        0
    ) AS organic_app_a2c,
    COALESCE(
        SUM(
            CASE
                WHEN channel IN ('mkt') THEN a2c
                ELSE 0
            END
        ),
        0
    ) AS mkt_a2c,
    COALESCE(
        SUM(
            CASE
                WHEN channel IN ('mkt') THEN app_a2c
                ELSE 0
            END
        ),
        0
    ) AS mkt_app_a2c,
    COALESCE(
        SUM(
            CASE
                WHEN channel IN ('other_mkt') THEN a2c
                ELSE 0
            END
        ),
        0
    ) AS other_mkt_a2c,
    COALESCE(
        SUM(
            CASE
                WHEN channel IN ('other_mkt') THEN app_a2c
                ELSE 0
            END
        ),
        0
    ) AS other_mkt_app_a2c,
    COALESCE(
        SUM(
            CASE
                WHEN channel IN ('crm') THEN a2c
                ELSE 0
            END
        ),
        0
    ) AS crm_a2c,
    COALESCE(
        SUM(
            CASE
                WHEN channel IN ('crm') THEN app_a2c
                ELSE 0
            END
        ),
        0
    ) AS crm_app_a2c,
    COALESCE(
        SUM(
            CASE
                WHEN channel IN ('om') THEN a2c
                ELSE 0
            END
        ),
        0
    ) AS om_a2c,
    COALESCE(
        SUM(
            CASE
                WHEN channel IN ('om') THEN app_a2c
                ELSE 0
            END
        ),
        0
    ) AS om_app_a2c,
    COALESCE(
        SUM(
            CASE
                WHEN channel IN ('ppc') THEN a2c
                ELSE 0
            END
        ),
        0
    ) AS ppc_a2c,
    COALESCE(
        SUM(
            CASE
                WHEN channel IN ('ppc') THEN app_a2c
                ELSE 0
            END
        ),
        0
    ) AS ppc_app_a2c,
    COALESCE(
        SUM(
            CASE
                WHEN channel IN ('affiliate') THEN a2c
                ELSE 0
            END
        ),
        0
    ) AS affiliate_a2c,
    COALESCE(
        SUM(
            CASE
                WHEN channel IN ('affiliate') THEN app_a2c
                ELSE 0
            END
        ),
        0
    ) AS affiliate_app_a2c,
    COALESCE(
        SUM(
            CASE
                WHEN channel IN ('rta') THEN a2c
                ELSE 0
            END
        ),
        0
    ) AS rta_a2c,
    COALESCE(
        SUM(
            CASE
                WHEN channel IN ('rta') THEN app_a2c
                ELSE 0
            END
        ),
        0
    ) AS rta_app_a2c --<< End
,
    COALESCE(
        SUM(
            CASE
                WHEN channel IN ('total') THEN a2c_uv
                ELSE 0
            END
        ),
        0
    ) AS total_a2c_uv --<< Start a2c_uv, app_a2c_uv
,
    COALESCE(
        SUM(
            CASE
                WHEN channel IN ('total') THEN app_a2c_uv
                ELSE 0
            END
        ),
        0
    ) AS total_app_a2c_uv,
    COALESCE(
        SUM(
            CASE
                WHEN channel IN ('organic') THEN a2c_uv
                ELSE 0
            END
        ),
        0
    ) AS organic_a2c_uv,
    COALESCE(
        SUM(
            CASE
                WHEN channel IN ('organic') THEN app_a2c_uv
                ELSE 0
            END
        ),
        0
    ) AS organic_app_a2c_uv,
    COALESCE(
        SUM(
            CASE
                WHEN channel IN ('mkt') THEN a2c_uv
                ELSE 0
            END
        ),
        0
    ) AS mkt_a2c_uv,
    COALESCE(
        SUM(
            CASE
                WHEN channel IN ('mkt') THEN app_a2c_uv
                ELSE 0
            END
        ),
        0
    ) AS mkt_app_a2c_uv,
    COALESCE(
        SUM(
            CASE
                WHEN channel IN ('other_mkt') THEN a2c_uv
                ELSE 0
            END
        ),
        0
    ) AS other_mkt_a2c_uv,
    COALESCE(
        SUM(
            CASE
                WHEN channel IN ('other_mkt') THEN app_a2c_uv
                ELSE 0
            END
        ),
        0
    ) AS other_mkt_app_a2c_uv,
    COALESCE(
        SUM(
            CASE
                WHEN channel IN ('crm') THEN a2c_uv
                ELSE 0
            END
        ),
        0
    ) AS crm_a2c_uv,
    COALESCE(
        SUM(
            CASE
                WHEN channel IN ('crm') THEN app_a2c_uv
                ELSE 0
            END
        ),
        0
    ) AS crm_app_a2c_uv,
    COALESCE(
        SUM(
            CASE
                WHEN channel IN ('om') THEN a2c_uv
                ELSE 0
            END
        ),
        0
    ) AS om_a2c_uv,
    COALESCE(
        SUM(
            CASE
                WHEN channel IN ('om') THEN app_a2c_uv
                ELSE 0
            END
        ),
        0
    ) AS om_app_a2c_uv,
    COALESCE(
        SUM(
            CASE
                WHEN channel IN ('ppc') THEN a2c_uv
                ELSE 0
            END
        ),
        0
    ) AS ppc_a2c_uv,
    COALESCE(
        SUM(
            CASE
                WHEN channel IN ('ppc') THEN app_a2c_uv
                ELSE 0
            END
        ),
        0
    ) AS ppc_app_a2c_uv,
    COALESCE(
        SUM(
            CASE
                WHEN channel IN ('affiliate') THEN a2c_uv
                ELSE 0
            END
        ),
        0
    ) AS affiliate_a2c_uv,
    COALESCE(
        SUM(
            CASE
                WHEN channel IN ('affiliate') THEN app_a2c_uv
                ELSE 0
            END
        ),
        0
    ) AS affiliate_app_a2c_uv,
    COALESCE(
        SUM(
            CASE
                WHEN channel IN ('rta') THEN a2c_uv
                ELSE 0
            END
        ),
        0
    ) AS rta_a2c_uv,
    COALESCE(
        SUM(
            CASE
                WHEN channel IN ('rta') THEN app_a2c_uv
                ELSE 0
            END
        ),
        0
    ) AS rta_app_a2c_uv --<< End
,
    COALESCE(
        SUM(
            CASE
                WHEN channel IN ('total') THEN buyers
                ELSE 0
            END
        ),
        0
    ) AS total_buyers --<< Start buyers, app_buyers
,
    COALESCE(
        SUM(
            CASE
                WHEN channel IN ('total') THEN app_buyers
                ELSE 0
            END
        ),
        0
    ) AS total_app_buyers,
    COALESCE(
        SUM(
            CASE
                WHEN channel IN ('organic') THEN buyers
                ELSE 0
            END
        ),
        0
    ) AS organic_buyers,
    COALESCE(
        SUM(
            CASE
                WHEN channel IN ('organic') THEN app_buyers
                ELSE 0
            END
        ),
        0
    ) AS organic_app_buyers,
    COALESCE(
        SUM(
            CASE
                WHEN channel IN ('mkt') THEN buyers
                ELSE 0
            END
        ),
        0
    ) AS mkt_buyers,
    COALESCE(
        SUM(
            CASE
                WHEN channel IN ('mkt') THEN app_buyers
                ELSE 0
            END
        ),
        0
    ) AS mkt_app_buyers,
    COALESCE(
        SUM(
            CASE
                WHEN channel IN ('other_mkt') THEN buyers
                ELSE 0
            END
        ),
        0
    ) AS other_mkt_buyers,
    COALESCE(
        SUM(
            CASE
                WHEN channel IN ('other_mkt') THEN app_buyers
                ELSE 0
            END
        ),
        0
    ) AS other_mkt_app_buyers,
    COALESCE(
        SUM(
            CASE
                WHEN channel IN ('crm') THEN buyers
                ELSE 0
            END
        ),
        0
    ) AS crm_buyers,
    COALESCE(
        SUM(
            CASE
                WHEN channel IN ('crm') THEN app_buyers
                ELSE 0
            END
        ),
        0
    ) AS crm_app_buyers,
    COALESCE(
        SUM(
            CASE
                WHEN channel IN ('om') THEN buyers
                ELSE 0
            END
        ),
        0
    ) AS om_buyers,
    COALESCE(
        SUM(
            CASE
                WHEN channel IN ('om') THEN app_buyers
                ELSE 0
            END
        ),
        0
    ) AS om_app_buyers,
    COALESCE(
        SUM(
            CASE
                WHEN channel IN ('ppc') THEN buyers
                ELSE 0
            END
        ),
        0
    ) AS ppc_buyers,
    COALESCE(
        SUM(
            CASE
                WHEN channel IN ('ppc') THEN app_buyers
                ELSE 0
            END
        ),
        0
    ) AS ppc_app_buyers,
    COALESCE(
        SUM(
            CASE
                WHEN channel IN ('affiliate') THEN buyers
                ELSE 0
            END
        ),
        0
    ) AS affiliate_buyers,
    COALESCE(
        SUM(
            CASE
                WHEN channel IN ('affiliate') THEN app_buyers
                ELSE 0
            END
        ),
        0
    ) AS affiliate_app_buyers,
    COALESCE(
        SUM(
            CASE
                WHEN channel IN ('rta') THEN buyers
                ELSE 0
            END
        ),
        0
    ) AS rta_buyers,
    COALESCE(
        SUM(
            CASE
                WHEN channel IN ('rta') THEN app_buyers
                ELSE 0
            END
        ),
        0
    ) AS rta_app_buyers --<< End
,
    COALESCE(
        SUM(
            CASE
                WHEN channel IN ('total') THEN new_buyers
                ELSE 0
            END
        ),
        0
    ) AS total_new_buyers --<< Start new_buyers, app_new_buyers
,
    COALESCE(
        SUM(
            CASE
                WHEN channel IN ('total') THEN app_new_buyers
                ELSE 0
            END
        ),
        0
    ) AS total_app_new_buyers,
    COALESCE(
        SUM(
            CASE
                WHEN channel IN ('organic') THEN new_buyers
                ELSE 0
            END
        ),
        0
    ) AS organic_new_buyers,
    COALESCE(
        SUM(
            CASE
                WHEN channel IN ('organic') THEN app_new_buyers
                ELSE 0
            END
        ),
        0
    ) AS organic_app_new_buyers,
    COALESCE(
        SUM(
            CASE
                WHEN channel IN ('mkt') THEN new_buyers
                ELSE 0
            END
        ),
        0
    ) AS mkt_new_buyers,
    COALESCE(
        SUM(
            CASE
                WHEN channel IN ('mkt') THEN app_new_buyers
                ELSE 0
            END
        ),
        0
    ) AS mkt_app_new_buyers,
    COALESCE(
        SUM(
            CASE
                WHEN channel IN ('other_mkt') THEN new_buyers
                ELSE 0
            END
        ),
        0
    ) AS other_mkt_new_buyers,
    COALESCE(
        SUM(
            CASE
                WHEN channel IN ('other_mkt') THEN app_new_buyers
                ELSE 0
            END
        ),
        0
    ) AS other_mkt_app_new_buyers,
    COALESCE(
        SUM(
            CASE
                WHEN channel IN ('crm') THEN new_buyers
                ELSE 0
            END
        ),
        0
    ) AS crm_new_buyers,
    COALESCE(
        SUM(
            CASE
                WHEN channel IN ('crm') THEN app_new_buyers
                ELSE 0
            END
        ),
        0
    ) AS crm_app_new_buyers,
    COALESCE(
        SUM(
            CASE
                WHEN channel IN ('om') THEN new_buyers
                ELSE 0
            END
        ),
        0
    ) AS om_new_buyers,
    COALESCE(
        SUM(
            CASE
                WHEN channel IN ('om') THEN app_new_buyers
                ELSE 0
            END
        ),
        0
    ) AS om_app_new_buyers,
    COALESCE(
        SUM(
            CASE
                WHEN channel IN ('ppc') THEN new_buyers
                ELSE 0
            END
        ),
        0
    ) AS ppc_new_buyers,
    COALESCE(
        SUM(
            CASE
                WHEN channel IN ('ppc') THEN app_new_buyers
                ELSE 0
            END
        ),
        0
    ) AS ppc_app_new_buyers,
    COALESCE(
        SUM(
            CASE
                WHEN channel IN ('affiliate') THEN new_buyers
                ELSE 0
            END
        ),
        0
    ) AS affiliate_new_buyers,
    COALESCE(
        SUM(
            CASE
                WHEN channel IN ('affiliate') THEN app_new_buyers
                ELSE 0
            END
        ),
        0
    ) AS affiliate_app_new_buyers,
    COALESCE(
        SUM(
            CASE
                WHEN channel IN ('rta') THEN new_buyers
                ELSE 0
            END
        ),
        0
    ) AS rta_new_buyers,
    COALESCE(
        SUM(
            CASE
                WHEN channel IN ('rta') THEN app_new_buyers
                ELSE 0
            END
        ),
        0
    ) AS rta_app_new_buyers --<< End
,
    COALESCE(
        SUM(
            CASE
                WHEN channel IN ('total') THEN reacquired_buyers
                ELSE 0
            END
        ),
        0
    ) AS total_reacquired_buyers --<< Start reacquired_buyers, app_reacquired_buyers
,
    COALESCE(
        SUM(
            CASE
                WHEN channel IN ('total') THEN app_reacquired_buyers
                ELSE 0
            END
        ),
        0
    ) AS total_app_reacquired_buyers,
    COALESCE(
        SUM(
            CASE
                WHEN channel IN ('organic') THEN reacquired_buyers
                ELSE 0
            END
        ),
        0
    ) AS organic_reacquired_buyers,
    COALESCE(
        SUM(
            CASE
                WHEN channel IN ('organic') THEN app_reacquired_buyers
                ELSE 0
            END
        ),
        0
    ) AS organic_app_reacquired_buyers,
    COALESCE(
        SUM(
            CASE
                WHEN channel IN ('mkt') THEN reacquired_buyers
                ELSE 0
            END
        ),
        0
    ) AS mkt_reacquired_buyers,
    COALESCE(
        SUM(
            CASE
                WHEN channel IN ('mkt') THEN app_reacquired_buyers
                ELSE 0
            END
        ),
        0
    ) AS mkt_app_reacquired_buyers,
    COALESCE(
        SUM(
            CASE
                WHEN channel IN ('other_mkt') THEN reacquired_buyers
                ELSE 0
            END
        ),
        0
    ) AS other_mkt_reacquired_buyers,
    COALESCE(
        SUM(
            CASE
                WHEN channel IN ('other_mkt') THEN app_reacquired_buyers
                ELSE 0
            END
        ),
        0
    ) AS other_mkt_app_reacquired_buyers,
    COALESCE(
        SUM(
            CASE
                WHEN channel IN ('crm') THEN reacquired_buyers
                ELSE 0
            END
        ),
        0
    ) AS crm_reacquired_buyers,
    COALESCE(
        SUM(
            CASE
                WHEN channel IN ('crm') THEN app_reacquired_buyers
                ELSE 0
            END
        ),
        0
    ) AS crm_app_reacquired_buyers,
    COALESCE(
        SUM(
            CASE
                WHEN channel IN ('om') THEN reacquired_buyers
                ELSE 0
            END
        ),
        0
    ) AS om_reacquired_buyers,
    COALESCE(
        SUM(
            CASE
                WHEN channel IN ('om') THEN app_reacquired_buyers
                ELSE 0
            END
        ),
        0
    ) AS om_app_reacquired_buyers,
    COALESCE(
        SUM(
            CASE
                WHEN channel IN ('ppc') THEN reacquired_buyers
                ELSE 0
            END
        ),
        0
    ) AS ppc_reacquired_buyers,
    COALESCE(
        SUM(
            CASE
                WHEN channel IN ('ppc') THEN app_reacquired_buyers
                ELSE 0
            END
        ),
        0
    ) AS ppc_app_reacquired_buyers,
    COALESCE(
        SUM(
            CASE
                WHEN channel IN ('affiliate') THEN reacquired_buyers
                ELSE 0
            END
        ),
        0
    ) AS affiliate_reacquired_buyers,
    COALESCE(
        SUM(
            CASE
                WHEN channel IN ('affiliate') THEN app_reacquired_buyers
                ELSE 0
            END
        ),
        0
    ) AS affiliate_app_reacquired_buyers,
    COALESCE(
        SUM(
            CASE
                WHEN channel IN ('rta') THEN reacquired_buyers
                ELSE 0
            END
        ),
        0
    ) AS rta_reacquired_buyers,
    COALESCE(
        SUM(
            CASE
                WHEN channel IN ('rta') THEN app_reacquired_buyers
                ELSE 0
            END
        ),
        0
    ) AS rta_app_reacquired_buyers --<< End
,
    COALESCE(
        SUM(
            CASE
                WHEN channel IN ('total') THEN sales_order_id_count
                ELSE 0
            END
        ),
        0
    ) AS total_sales_order_id_count --<< Start sales_order_id_count, app_sales_order_id_count
,
    COALESCE(
        SUM(
            CASE
                WHEN channel IN ('total') THEN app_sales_order_id_count
                ELSE 0
            END
        ),
        0
    ) AS total_app_sales_order_id_count,
    COALESCE(
        SUM(
            CASE
                WHEN channel IN ('organic') THEN sales_order_id_count
                ELSE 0
            END
        ),
        0
    ) AS organic_sales_order_id_count,
    COALESCE(
        SUM(
            CASE
                WHEN channel IN ('organic') THEN app_sales_order_id_count
                ELSE 0
            END
        ),
        0
    ) AS organic_app_sales_order_id_count,
    COALESCE(
        SUM(
            CASE
                WHEN channel IN ('mkt') THEN sales_order_id_count
                ELSE 0
            END
        ),
        0
    ) AS mkt_sales_order_id_count,
    COALESCE(
        SUM(
            CASE
                WHEN channel IN ('mkt') THEN app_sales_order_id_count
                ELSE 0
            END
        ),
        0
    ) AS mkt_app_sales_order_id_count,
    COALESCE(
        SUM(
            CASE
                WHEN channel IN ('other_mkt') THEN sales_order_id_count
                ELSE 0
            END
        ),
        0
    ) AS other_mkt_sales_order_id_count,
    COALESCE(
        SUM(
            CASE
                WHEN channel IN ('other_mkt') THEN app_sales_order_id_count
                ELSE 0
            END
        ),
        0
    ) AS other_mkt_app_sales_order_id_count,
    COALESCE(
        SUM(
            CASE
                WHEN channel IN ('crm') THEN sales_order_id_count
                ELSE 0
            END
        ),
        0
    ) AS crm_sales_order_id_count,
    COALESCE(
        SUM(
            CASE
                WHEN channel IN ('crm') THEN app_sales_order_id_count
                ELSE 0
            END
        ),
        0
    ) AS crm_app_sales_order_id_count,
    COALESCE(
        SUM(
            CASE
                WHEN channel IN ('om') THEN sales_order_id_count
                ELSE 0
            END
        ),
        0
    ) AS om_sales_order_id_count,
    COALESCE(
        SUM(
            CASE
                WHEN channel IN ('om') THEN app_sales_order_id_count
                ELSE 0
            END
        ),
        0
    ) AS om_app_sales_order_id_count,
    COALESCE(
        SUM(
            CASE
                WHEN channel IN ('ppc') THEN sales_order_id_count
                ELSE 0
            END
        ),
        0
    ) AS ppc_sales_order_id_count,
    COALESCE(
        SUM(
            CASE
                WHEN channel IN ('ppc') THEN app_sales_order_id_count
                ELSE 0
            END
        ),
        0
    ) AS ppc_app_sales_order_id_count,
    COALESCE(
        SUM(
            CASE
                WHEN channel IN ('affiliate') THEN sales_order_id_count
                ELSE 0
            END
        ),
        0
    ) AS affiliate_sales_order_id_count,
    COALESCE(
        SUM(
            CASE
                WHEN channel IN ('affiliate') THEN app_sales_order_id_count
                ELSE 0
            END
        ),
        0
    ) AS affiliate_app_sales_order_id_count,
    COALESCE(
        SUM(
            CASE
                WHEN channel IN ('rta') THEN sales_order_id_count
                ELSE 0
            END
        ),
        0
    ) AS rta_sales_order_id_count,
    COALESCE(
        SUM(
            CASE
                WHEN channel IN ('rta') THEN app_sales_order_id_count
                ELSE 0
            END
        ),
        0
    ) AS rta_app_sales_order_id_count --<< End
,
    COALESCE(
        SUM(
            CASE
                WHEN channel IN ('total') THEN order_id_count
                ELSE 0
            END
        ),
        0
    ) AS total_order_id_count --<< Start order_id_count, app_order_id_count
,
    COALESCE(
        SUM(
            CASE
                WHEN channel IN ('total') THEN app_order_id_count
                ELSE 0
            END
        ),
        0
    ) AS total_app_order_id_count,
    COALESCE(
        SUM(
            CASE
                WHEN channel IN ('organic') THEN order_id_count
                ELSE 0
            END
        ),
        0
    ) AS organic_order_id_count,
    COALESCE(
        SUM(
            CASE
                WHEN channel IN ('organic') THEN app_order_id_count
                ELSE 0
            END
        ),
        0
    ) AS organic_app_order_id_count,
    COALESCE(
        SUM(
            CASE
                WHEN channel IN ('mkt') THEN order_id_count
                ELSE 0
            END
        ),
        0
    ) AS mkt_order_id_count,
    COALESCE(
        SUM(
            CASE
                WHEN channel IN ('mkt') THEN app_order_id_count
                ELSE 0
            END
        ),
        0
    ) AS mkt_app_order_id_count,
    COALESCE(
        SUM(
            CASE
                WHEN channel IN ('other_mkt') THEN order_id_count
                ELSE 0
            END
        ),
        0
    ) AS other_mkt_order_id_count,
    COALESCE(
        SUM(
            CASE
                WHEN channel IN ('other_mkt') THEN app_order_id_count
                ELSE 0
            END
        ),
        0
    ) AS other_mkt_app_order_id_count,
    COALESCE(
        SUM(
            CASE
                WHEN channel IN ('crm') THEN order_id_count
                ELSE 0
            END
        ),
        0
    ) AS crm_order_id_count,
    COALESCE(
        SUM(
            CASE
                WHEN channel IN ('crm') THEN app_order_id_count
                ELSE 0
            END
        ),
        0
    ) AS crm_app_order_id_count,
    COALESCE(
        SUM(
            CASE
                WHEN channel IN ('om') THEN order_id_count
                ELSE 0
            END
        ),
        0
    ) AS om_order_id_count,
    COALESCE(
        SUM(
            CASE
                WHEN channel IN ('om') THEN app_order_id_count
                ELSE 0
            END
        ),
        0
    ) AS om_app_order_id_count,
    COALESCE(
        SUM(
            CASE
                WHEN channel IN ('ppc') THEN order_id_count
                ELSE 0
            END
        ),
        0
    ) AS ppc_order_id_count,
    COALESCE(
        SUM(
            CASE
                WHEN channel IN ('ppc') THEN app_order_id_count
                ELSE 0
            END
        ),
        0
    ) AS ppc_app_order_id_count,
    COALESCE(
        SUM(
            CASE
                WHEN channel IN ('affiliate') THEN order_id_count
                ELSE 0
            END
        ),
        0
    ) AS affiliate_order_id_count,
    COALESCE(
        SUM(
            CASE
                WHEN channel IN ('affiliate') THEN app_order_id_count
                ELSE 0
            END
        ),
        0
    ) AS affiliate_app_order_id_count,
    COALESCE(
        SUM(
            CASE
                WHEN channel IN ('rta') THEN order_id_count
                ELSE 0
            END
        ),
        0
    ) AS rta_order_id_count,
    COALESCE(
        SUM(
            CASE
                WHEN channel IN ('rta') THEN app_order_id_count
                ELSE 0
            END
        ),
        0
    ) AS rta_app_order_id_count --<< End
,
    COALESCE(
        SUM(
            CASE
                WHEN channel IN ('total') THEN gmv
                ELSE 0
            END
        ),
        0
    ) AS total_gmv --<< Start gmv, app_gmv
,
    COALESCE(
        SUM(
            CASE
                WHEN channel IN ('total') THEN app_gmv
                ELSE 0
            END
        ),
        0
    ) AS total_app_gmv,
    COALESCE(
        SUM(
            CASE
                WHEN channel IN ('organic') THEN gmv
                ELSE 0
            END
        ),
        0
    ) AS organic_gmv,
    COALESCE(
        SUM(
            CASE
                WHEN channel IN ('organic') THEN app_gmv
                ELSE 0
            END
        ),
        0
    ) AS organic_app_gmv,
    COALESCE(
        SUM(
            CASE
                WHEN channel IN ('mkt') THEN gmv
                ELSE 0
            END
        ),
        0
    ) AS mkt_gmv,
    COALESCE(
        SUM(
            CASE
                WHEN channel IN ('mkt') THEN app_gmv
                ELSE 0
            END
        ),
        0
    ) AS mkt_app_gmv,
    COALESCE(
        SUM(
            CASE
                WHEN channel IN ('other_mkt') THEN gmv
                ELSE 0
            END
        ),
        0
    ) AS other_mkt_gmv,
    COALESCE(
        SUM(
            CASE
                WHEN channel IN ('other_mkt') THEN app_gmv
                ELSE 0
            END
        ),
        0
    ) AS other_mkt_app_gmv,
    COALESCE(
        SUM(
            CASE
                WHEN channel IN ('crm') THEN gmv
                ELSE 0
            END
        ),
        0
    ) AS crm_gmv,
    COALESCE(
        SUM(
            CASE
                WHEN channel IN ('crm') THEN app_gmv
                ELSE 0
            END
        ),
        0
    ) AS crm_app_gmv,
    COALESCE(
        SUM(
            CASE
                WHEN channel IN ('om') THEN gmv
                ELSE 0
            END
        ),
        0
    ) AS om_gmv,
    COALESCE(
        SUM(
            CASE
                WHEN channel IN ('om') THEN app_gmv
                ELSE 0
            END
        ),
        0
    ) AS om_app_gmv,
    COALESCE(
        SUM(
            CASE
                WHEN channel IN ('ppc') THEN gmv
                ELSE 0
            END
        ),
        0
    ) AS ppc_gmv,
    COALESCE(
        SUM(
            CASE
                WHEN channel IN ('ppc') THEN app_gmv
                ELSE 0
            END
        ),
        0
    ) AS ppc_app_gmv,
    COALESCE(
        SUM(
            CASE
                WHEN channel IN ('affiliate') THEN gmv
                ELSE 0
            END
        ),
        0
    ) AS affiliate_gmv,
    COALESCE(
        SUM(
            CASE
                WHEN channel IN ('affiliate') THEN app_gmv
                ELSE 0
            END
        ),
        0
    ) AS affiliate_app_gmv,
    COALESCE(
        SUM(
            CASE
                WHEN channel IN ('rta') THEN gmv
                ELSE 0
            END
        ),
        0
    ) AS rta_gmv,
    COALESCE(
        SUM(
            CASE
                WHEN channel IN ('rta') THEN app_gmv
                ELSE 0
            END
        ),
        0
    ) AS rta_app_gmv --<< End
FROM (
        SELECT ds,
            hh,
            venture,
            COALESCE(channel, "total") AS channel,
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
        FROM lazada_ads.ads_lzd_mkt_rt_uam_campaign_hi
        WHERE 1 = 1
            AND attr_model IN ('ft_1d_np', 'lt_1d', 'lt_1d_p')
        GROUP BY ds,
            hh,
            venture,
            channel GROUPING SETS ((ds, hh, venture))
        UNION ALL
        SELECT ds,
            hh,
            venture,
            COALESCE(channel, "organic") AS channel,
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
        FROM lazada_ads.ads_lzd_mkt_rt_uam_campaign_hi
        WHERE 1 = 1
            AND funding_bucket IN ('Unknown')
            AND channel IN ('Direct')
            AND attr_model IN ('ft_1d_np', 'lt_1d', 'lt_1d_p')
        GROUP BY ds,
            hh,
            venture,
            channel GROUPING SETS ((ds, hh, venture))
        UNION ALL
        SELECT ds,
            hh,
            venture,
            COALESCE(channel, "mkt") AS channel,
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
        FROM lazada_ads.ads_lzd_mkt_rt_uam_campaign_hi
        WHERE 1 = 1
            AND funding_bucket NOT IN ('Unknown')
            AND channel NOT IN ('Direct')
            AND attr_model IN ('ft_1d_np', 'lt_1d', 'lt_1d_p')
        GROUP BY ds,
            hh,
            venture,
            channel GROUPING SETS ((ds, hh, venture))
        UNION ALL
        SELECT ds,
            hh,
            venture,
            COALESCE(channel, "other_mkt") AS channel,
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
        FROM lazada_ads.ads_lzd_mkt_rt_uam_campaign_hi
        WHERE 1 = 1
            AND funding_bucket NOT IN ('Lazada OM', 'CRM', 'Unknown')
            AND attr_model IN ('ft_1d_np', 'lt_1d', 'lt_1d_p')
        GROUP BY ds,
            hh,
            venture,
            channel GROUPING SETS ((ds, hh, venture))
        UNION ALL
        SELECT ds,
            hh,
            venture,
            COALESCE(channel, "crm") AS channel,
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
        FROM lazada_ads.ads_lzd_mkt_rt_uam_campaign_hi
        WHERE 1 = 1
            AND funding_bucket IN ('CRM')
            AND attr_model IN ('ft_1d_np', 'lt_1d', 'lt_1d_p')
        GROUP BY ds,
            hh,
            venture,
            channel GROUPING SETS ((ds, hh, venture))
        UNION ALL
        SELECT ds,
            hh,
            venture,
            COALESCE(channel, "om") AS channel,
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
        FROM lazada_ads.ads_lzd_mkt_rt_uam_campaign_hi
        WHERE 1 = 1
            AND funding_bucket IN ('Lazada OM')
            AND attr_model IN ('ft_1d_np', 'lt_1d', 'lt_1d_p')
        GROUP BY ds,
            hh,
            venture,
            channel GROUPING SETS ((ds, hh, venture))
        UNION ALL
        SELECT ds,
            hh,
            venture,
            COALESCE(channel, "ppc") AS channel,
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
        FROM lazada_ads.ads_lzd_mkt_rt_uam_campaign_hi
        WHERE 1 = 1
            AND funding_bucket IN ('Lazada OM')
            AND channel IN ('Apple Search', 'Display', 'SEM', 'Social', 'TikTok')
            AND attr_model IN ('ft_1d_np', 'lt_1d', 'lt_1d_p')
        GROUP BY ds,
            hh,
            venture,
            channel GROUPING SETS ((ds, hh, venture))
        UNION ALL
        SELECT ds,
            hh,
            venture,
            COALESCE(channel, "affiliate") AS channel,
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
        FROM lazada_ads.ads_lzd_mkt_rt_uam_campaign_hi
        WHERE 1 = 1
            AND funding_bucket IN ('Lazada OM')
            AND channel IN ('CPS Affiliate', 'CPI Affiliate')
            AND attr_model IN ('ft_1d_np', 'lt_1d', 'lt_1d_p')
        GROUP BY ds,
            hh,
            venture,
            channel GROUPING SETS ((ds, hh, venture))
        UNION ALL
        SELECT ds,
            hh,
            venture,
            COALESCE(channel, "rta") AS channel,
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
        FROM lazada_ads.ads_lzd_mkt_rt_uam_campaign_hi
        WHERE 1 = 1
            AND funding_bucket IN ('Lazada OM')
            AND channel IN ('RTA')
            AND attr_model IN ('ft_1d_np', 'lt_1d', 'lt_1d_p')
        GROUP BY ds,
            hh,
            venture,
            channel GROUPING SETS ((ds, hh, venture))
    )
WHERE 1 = 1
GROUP BY ds,
    hh,
    venture
ORDER BY ds DESC,
    hh DESC;
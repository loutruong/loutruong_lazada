-- MaxCompute SQL 
-- ********************************************************************--
-- author:vanthanh.truong
-- create time:2023-07-24 11:19:56
-- ********************************************************************--
-- DROP TABLE IF EXISTS temp_loutruong_usr_uid_monetary_d
-- ;
-- CREATE TABLE IF NOT EXISTS temp_loutruong_usr_uid_monetary_d
-- LIFECYCLE 7 AS
-- SELECT  *
-- FROM    lazada_datamining.ads_lzd_asset_usr_uid_monetary_d
-- WHERE   1 = 1
-- AND     venture = 'VN'
-- ;
-- DROP TABLE IF EXISTS temp_loutruong_utdid_info
-- ;
-- CREATE TABLE IF NOT EXISTS temp_loutruong_utdid_info
-- LIFECYCLE 7 AS
-- SELECT  t1.utdid AS utdid
--         ,t1.user_id AS user_id
--         ,COALESCE(t1.g_level,'Undefined') AS g_level
--         ,COALESCE(t2.monetary_tag,'Undefined') AS monetary_tag
--         ,COALESCE(t3.age_group,'Undefined') AS age_group
--         ,COALESCE(t3.gender,'Undefined') AS gender
--         ,COALESCE(t3.user_city_name,'Undefined') AS user_city_name
-- FROM    (
--             SELECT  utdid
--                     ,highest_user_id AS user_id
--                     ,highest_g_level AS g_level
--             FROM    lazada_datamining.ads_lzd_asset_usr_utdid_glevel_d
--             WHERE   1 = 1
--             AND     ds = 20230719 --<< Fix logic with BI team
--             AND     venture = 'VN'
--         ) AS t1
-- LEFT JOIN   (
--                 SELECT  user_id AS user_id
--                         ,MAX(monetary_tag) AS monetary_tag
--                 FROM    lazada_datamining.ads_lzd_asset_usr_uid_monetary_d
--                 WHERE   1 = 1
--                 AND     ds IN ('20230719') --<< Fix logic with BI team
--                 AND     venture = 'VN'
--                 GROUP BY user_id
--             ) AS t2
-- ON      t1.user_id = t2.user_id
-- LEFT JOIN   (
--                 SELECT  user_id
--                         ,gender
--                         ,age_group
--                         ,user_city_name
--                 FROM    lazada_analyst_dev.datx_usr_demo
--             ) AS t3
-- ON      t1.user_id = t3.user_id
-- GROUP BY t1.utdid
--          ,t1.user_id
--          ,COALESCE(t1.g_level,'Undefined')
--          ,COALESCE(t2.monetary_tag,'Undefined')
--          ,COALESCE(t3.age_group,'Undefined')
--          ,COALESCE(t3.gender,'Undefined')
--          ,COALESCE(t3.user_city_name,'Undefined')
-- ;
--Input = temp_loutruong_utdid_info
--@@ Input = loutruong_price_model
--@@ Input = loutruong_trf_touch_app_di
--@@ Input = loutruong_trn_order_app_di
--@@ Output = loutruong_om_clm_spend_di
DROP TABLE IF EXISTS temp_loutruong_om_clm_g_di
;

CREATE TABLE IF NOT EXISTS
	temp_loutruong_om_clm_g_di LIFECYCLE 7 AS
SELECT
	COALESCE(COALESCE(g_level, "Undefined"), "All")        AS g_level,
	COALESCE(COALESCE(user_city_name, "Undefined"), "All") AS user_city_name,
	COALESCE(COALESCE(age_group, "Undefined"), "All")      AS age_group,
	COALESCE(COALESCE(gender, "Undefined"), "All")         AS gender,
	COALESCE(COALESCE(monetary_tag, "Undefined"), "All")   AS monetary_tag,
	SUM(om_spend_l30d)                                     AS om_spend_l30d,
	SUM(om_spend_l90d)                                     AS om_spend_l90d
FROM
	(
		SELECT
			source,
			g_level,
			monetary_tag,
			age_group,
			gender,
			user_city_name,
			COALESCE(SUM(spend), 0) AS om_spend_l30d,
			0                       AS om_spend_l90d
		FROM
			(
				SELECT
					t1.source AS source --<< clm spend ppc
,
					t1.sub_source AS sub_source,
					t3.g_level AS g_level,
					t3.monetary_tag AS monetary_tag,
					t3.age_group AS age_group,
					t3.gender AS gender,
					t3.user_city_name AS user_city_name,
					COALESCE(SUM(t2.price_app), 0) AS spend
				FROM
					(
						SELECT
							ds --<< Performance app ppc
,
							source,
							sub_source,
							channel,
							bucket,
							sub_channel,
							campaign,
							utdid
						FROM
							loutruong_trf_touch_app_di
						WHERE
							1 = 1
							AND TOLOWER (source) IN ('lazada om')
							AND TOLOWER (sub_source) IN ('ppc')
							AND ds >= TO_CHAR(
								DATEADD (TO_DATE(20230719, 'yyyymmdd'), -29, 'dd'),
								'yyyymmdd'
							)
							AND ds <= 20230719
						GROUP BY
							ds,
							source,
							sub_source,
							channel,
							bucket,
							sub_channel,
							campaign,
							utdid
					) AS t1
					INNER JOIN (
						SELECT
							ds --<< Spend ppc
,
							source,
							sub_source,
							channel,
							bucket,
							sub_channel,
							campaign,
							price_app
						FROM
							loutruong_price_model
						WHERE
							1 = 1
							AND TOLOWER (sub_source) IN ('ppc')
							AND TOLOWER (campaign) NOT IN ('all')
							AND ds >= TO_CHAR(
								DATEADD (TO_DATE(20230719, 'yyyymmdd'), -29, 'dd'),
								'yyyymmdd'
							)
							AND ds <= 20230719
						GROUP BY
							ds,
							source,
							sub_source,
							channel,
							bucket,
							sub_channel,
							campaign,
							price_app
					) AS t2 ON t1.ds = t2.ds
					AND t1.sub_channel = t2.sub_channel
					AND t1.campaign = t2.campaign
					LEFT JOIN (
						SELECT
							utdid --<< Clm app ppc
,
							g_level,
							monetary_tag,
							age_group,
							gender,
							user_city_name
						FROM
							temp_loutruong_utdid_info
						GROUP BY
							utdid,
							g_level,
							monetary_tag,
							age_group,
							gender,
							user_city_name
					) AS t3 ON t1.utdid = t3.utdid
				GROUP BY
					t1.source,
					t1.sub_source,
					t3.g_level,
					t3.monetary_tag,
					t3.age_group,
					t3.gender,
					t3.user_city_name
				UNION ALL
				--<< Break point
				SELECT
					t1.source AS source --<< clm spend rta
,
					t1.sub_source AS sub_source,
					t3.g_level AS g_level,
					t3.monetary_tag AS monetary_tag,
					t3.age_group AS age_group,
					t3.gender AS gender,
					t3.user_city_name AS user_city_name,
					COALESCE(SUM(t2.price_app), 0) AS spend
				FROM
					(
						SELECT
							ds --<< Performance app rta
,
							source,
							sub_source,
							channel,
							bucket,
							sub_channel,
							campaign,
							utdid
						FROM
							loutruong_trf_touch_app_di
						WHERE
							1 = 1
							AND TOLOWER (source) IN ('lazada om')
							AND TOLOWER (sub_source) IN ('rta')
							AND ds >= TO_CHAR(
								DATEADD (TO_DATE(20230719, 'yyyymmdd'), -29, 'dd'),
								'yyyymmdd'
							)
							AND ds <= 20230719
						GROUP BY
							ds,
							source,
							sub_source,
							channel,
							bucket,
							sub_channel,
							campaign,
							utdid
					) AS t1
					INNER JOIN (
						SELECT
							ds --<< Spend rta
,
							source,
							sub_source,
							channel,
							bucket,
							sub_channel,
							campaign,
							price_app
						FROM
							loutruong_price_model
						WHERE
							1 = 1
							AND TOLOWER (sub_source) IN ('rta')
							AND TOLOWER (campaign) NOT IN ('all')
							AND ds >= TO_CHAR(
								DATEADD (TO_DATE(20230719, 'yyyymmdd'), -29, 'dd'),
								'yyyymmdd'
							)
							AND ds <= 20230719
						GROUP BY
							ds,
							source,
							sub_source,
							channel,
							bucket,
							sub_channel,
							campaign,
							price_app
					) AS t2 ON t1.ds = t2.ds
					AND t1.sub_channel = t2.sub_channel
					AND t1.campaign = t2.campaign
					LEFT JOIN (
						SELECT
							utdid --<< Clm app rta
,
							g_level,
							monetary_tag,
							age_group,
							gender,
							user_city_name
						FROM
							temp_loutruong_utdid_info
						GROUP BY
							utdid,
							g_level,
							monetary_tag,
							age_group,
							gender,
							user_city_name
					) AS t3 ON t1.utdid = t3.utdid
				GROUP BY
					t1.source,
					t1.sub_source,
					t3.g_level,
					t3.monetary_tag,
					t3.age_group,
					t3.gender,
					t3.user_city_name
				UNION ALL
				--<< Break point
				SELECT
					t1.source AS source --<< clm spend affiliate
,
					t1.sub_source AS sub_source,
					COALESCE(t3.g_level, 'Undefined') AS g_level,
					COALESCE(t3.monetary_tag, 'Undefined') AS monetary_tag,
					COALESCE(t3.age_group, 'Undefined') AS age_group,
					COALESCE(t3.gender, 'Undefined') AS gender,
					COALESCE(t3.user_city_name, 'Undefined') AS user_city_name,
					COALESCE(SUM(t1.order_app_cnt * t2.price_app), 0) AS spend
				FROM
					(
						SELECT
							ds --<< Performance app affiliate
,
							source,
							sub_source,
							channel,
							bucket,
							sub_channel,
							campaign,
							utdid,
							buyer_id,
							check_out_id,
							order_app_cnt
						FROM
							loutruong_trn_order_app_di
						WHERE
							1 = 1
							AND ds >= TO_CHAR(
								DATEADD (TO_DATE(20230719, 'yyyymmdd'), -29, 'dd'),
								'yyyymmdd'
							)
							AND ds <= 20230719
						GROUP BY
							ds,
							source,
							sub_source,
							channel,
							bucket,
							sub_channel,
							campaign,
							utdid,
							buyer_id,
							check_out_id,
							order_app_cnt
					) AS t1
					INNER JOIN (
						SELECT
							ds --<< Spend affiliate
,
							source,
							sub_source,
							channel,
							bucket,
							sub_channel,
							campaign,
							price_app
						FROM
							loutruong_price_model
						WHERE
							1 = 1
							AND TOLOWER (sub_source) IN ('affiliate')
							AND TOLOWER (campaign) NOT IN ('all')
							AND ds >= TO_CHAR(
								DATEADD (TO_DATE(20230719, 'yyyymmdd'), -29, 'dd'),
								'yyyymmdd'
							)
							AND ds <= 20230719
						GROUP BY
							ds,
							source,
							sub_source,
							channel,
							bucket,
							sub_channel,
							campaign,
							price_app
					) AS t2 ON t1.ds = t2.ds
					AND t1.sub_channel = t2.sub_channel
					AND t1.campaign = t2.campaign
					LEFT JOIN (
						SELECT
							utdid --<< Clm app affiliate
,
							g_level,
							monetary_tag,
							age_group,
							gender,
							user_city_name
						FROM
							temp_loutruong_utdid_info
						GROUP BY
							utdid,
							g_level,
							monetary_tag,
							age_group,
							gender,
							user_city_name
					) AS t3 ON t1.utdid = t3.utdid
				GROUP BY
					t1.source,
					t1.sub_source,
					COALESCE(t3.g_level, 'Undefined'),
					COALESCE(t3.monetary_tag, 'Undefined'),
					COALESCE(t3.age_group, 'Undefined'),
					COALESCE(t3.gender, 'Undefined'),
					COALESCE(t3.user_city_name, 'Undefined')
			)
		GROUP BY
			source,
			g_level,
			monetary_tag,
			age_group,
			gender,
			user_city_name
		UNION ALL
		--<< BIG BREAK POINT PLS BE ATTENTION (l30d, l90d)
		SELECT
			source,
			g_level,
			monetary_tag,
			age_group,
			gender,
			user_city_name,
			0                       AS om_spend_l30d,
			COALESCE(SUM(spend), 0) AS om_spend_l90d
		FROM
			(
				SELECT
					t1.source AS source --<< clm spend ppc
,
					t1.sub_source AS sub_source,
					t3.g_level AS g_level,
					t3.monetary_tag AS monetary_tag,
					t3.age_group AS age_group,
					t3.gender AS gender,
					t3.user_city_name AS user_city_name,
					COALESCE(SUM(t2.price_app), 0) AS spend
				FROM
					(
						SELECT
							ds --<< Performance app ppc
,
							source,
							sub_source,
							channel,
							bucket,
							sub_channel,
							campaign,
							utdid
						FROM
							loutruong_trf_touch_app_di
						WHERE
							1 = 1
							AND TOLOWER (source) IN ('lazada om')
							AND TOLOWER (sub_source) IN ('ppc')
							AND ds >= TO_CHAR(
								DATEADD (TO_DATE(20230719, 'yyyymmdd'), -89, 'dd'),
								'yyyymmdd'
							)
							AND ds <= 20230719
						GROUP BY
							ds,
							source,
							sub_source,
							channel,
							bucket,
							sub_channel,
							campaign,
							utdid
					) AS t1
					INNER JOIN (
						SELECT
							ds --<< Spend ppc
,
							source,
							sub_source,
							channel,
							bucket,
							sub_channel,
							campaign,
							price_app
						FROM
							loutruong_price_model
						WHERE
							1 = 1
							AND TOLOWER (sub_source) IN ('ppc')
							AND TOLOWER (campaign) NOT IN ('all')
							AND ds >= TO_CHAR(
								DATEADD (TO_DATE(20230719, 'yyyymmdd'), -89, 'dd'),
								'yyyymmdd'
							)
							AND ds <= 20230719
						GROUP BY
							ds,
							source,
							sub_source,
							channel,
							bucket,
							sub_channel,
							campaign,
							price_app
					) AS t2 ON t1.ds = t2.ds
					AND t1.sub_channel = t2.sub_channel
					AND t1.campaign = t2.campaign
					LEFT JOIN (
						SELECT
							utdid --<< Clm app ppc
,
							g_level,
							monetary_tag,
							age_group,
							gender,
							user_city_name
						FROM
							temp_loutruong_utdid_info
						GROUP BY
							utdid,
							g_level,
							monetary_tag,
							age_group,
							gender,
							user_city_name
					) AS t3 ON t1.utdid = t3.utdid
				GROUP BY
					t1.source,
					t1.sub_source,
					t3.g_level,
					t3.monetary_tag,
					t3.age_group,
					t3.gender,
					t3.user_city_name
				UNION ALL
				--<< Break point
				SELECT
					t1.source AS source --<< clm spend rta
,
					t1.sub_source AS sub_source,
					t3.g_level AS g_level,
					t3.monetary_tag AS monetary_tag,
					t3.age_group AS age_group,
					t3.gender AS gender,
					t3.user_city_name AS user_city_name,
					COALESCE(SUM(t2.price_app), 0) AS spend
				FROM
					(
						SELECT
							ds --<< Performance app rta
,
							source,
							sub_source,
							channel,
							bucket,
							sub_channel,
							campaign,
							utdid
						FROM
							loutruong_trf_touch_app_di
						WHERE
							1 = 1
							AND TOLOWER (source) IN ('lazada om')
							AND TOLOWER (sub_source) IN ('rta')
							AND ds >= TO_CHAR(
								DATEADD (TO_DATE(20230719, 'yyyymmdd'), -89, 'dd'),
								'yyyymmdd'
							)
							AND ds <= 20230719
						GROUP BY
							ds,
							source,
							sub_source,
							channel,
							bucket,
							sub_channel,
							campaign,
							utdid
					) AS t1
					INNER JOIN (
						SELECT
							ds --<< Spend rta
,
							source,
							sub_source,
							channel,
							bucket,
							sub_channel,
							campaign,
							price_app
						FROM
							loutruong_price_model
						WHERE
							1 = 1
							AND TOLOWER (sub_source) IN ('rta')
							AND TOLOWER (campaign) NOT IN ('all')
							AND ds >= TO_CHAR(
								DATEADD (TO_DATE(20230719, 'yyyymmdd'), -89, 'dd'),
								'yyyymmdd'
							)
							AND ds <= 20230719
						GROUP BY
							ds,
							source,
							sub_source,
							channel,
							bucket,
							sub_channel,
							campaign,
							price_app
					) AS t2 ON t1.ds = t2.ds
					AND t1.sub_channel = t2.sub_channel
					AND t1.campaign = t2.campaign
					LEFT JOIN (
						SELECT
							utdid --<< Clm app rta
,
							g_level,
							monetary_tag,
							age_group,
							gender,
							user_city_name
						FROM
							temp_loutruong_utdid_info
						GROUP BY
							utdid,
							g_level,
							monetary_tag,
							age_group,
							gender,
							user_city_name
					) AS t3 ON t1.utdid = t3.utdid
				GROUP BY
					t1.source,
					t1.sub_source,
					t3.g_level,
					t3.monetary_tag,
					t3.age_group,
					t3.gender,
					t3.user_city_name
				UNION ALL
				--<< Break point
				SELECT
					t1.source AS source --<< clm spend affiliate
,
					t1.sub_source AS sub_source,
					COALESCE(t3.g_level, 'Undefined') AS g_level,
					COALESCE(t3.monetary_tag, 'Undefined') AS monetary_tag,
					COALESCE(t3.age_group, 'Undefined') AS age_group,
					COALESCE(t3.gender, 'Undefined') AS gender,
					COALESCE(t3.user_city_name, 'Undefined') AS user_city_name,
					COALESCE(SUM(t1.order_app_cnt * t2.price_app), 0) AS spend
				FROM
					(
						SELECT
							ds --<< Performance app affiliate
,
							source,
							sub_source,
							channel,
							bucket,
							sub_channel,
							campaign,
							utdid,
							buyer_id,
							check_out_id,
							order_app_cnt
						FROM
							loutruong_trn_order_app_di
						WHERE
							1 = 1
							AND ds >= TO_CHAR(
								DATEADD (TO_DATE(20230719, 'yyyymmdd'), -89, 'dd'),
								'yyyymmdd'
							)
							AND ds <= 20230719
						GROUP BY
							ds,
							source,
							sub_source,
							channel,
							bucket,
							sub_channel,
							campaign,
							utdid,
							buyer_id,
							check_out_id,
							order_app_cnt
					) AS t1
					INNER JOIN (
						SELECT
							ds --<< Spend affiliate
,
							source,
							sub_source,
							channel,
							bucket,
							sub_channel,
							campaign,
							price_app
						FROM
							loutruong_price_model
						WHERE
							1 = 1
							AND TOLOWER (sub_source) IN ('affiliate')
							AND TOLOWER (campaign) NOT IN ('all')
							AND ds >= TO_CHAR(
								DATEADD (TO_DATE(20230719, 'yyyymmdd'), -89, 'dd'),
								'yyyymmdd'
							)
							AND ds <= 20230719
						GROUP BY
							ds,
							source,
							sub_source,
							channel,
							bucket,
							sub_channel,
							campaign,
							price_app
					) AS t2 ON t1.ds = t2.ds
					AND t1.sub_channel = t2.sub_channel
					AND t1.campaign = t2.campaign
					LEFT JOIN (
						SELECT
							utdid --<< Clm app affiliate
,
							g_level,
							monetary_tag,
							age_group,
							gender,
							user_city_name
						FROM
							temp_loutruong_utdid_info
						GROUP BY
							utdid,
							g_level,
							monetary_tag,
							age_group,
							gender,
							user_city_name
					) AS t3 ON t1.utdid = t3.utdid
				GROUP BY
					t1.source,
					t1.sub_source,
					COALESCE(t3.g_level, 'Undefined'),
					COALESCE(t3.monetary_tag, 'Undefined'),
					COALESCE(t3.age_group, 'Undefined'),
					COALESCE(t3.gender, 'Undefined'),
					COALESCE(t3.user_city_name, 'Undefined')
			)
		GROUP BY
			source,
			g_level,
			monetary_tag,
			age_group,
			gender,
			user_city_name
	)
GROUP BY
	CUBE (
		COALESCE(g_level, "Undefined"),
		COALESCE(user_city_name, "Undefined"),
		COALESCE(age_group, "Undefined"),
		COALESCE(gender, "Undefined"),
		COALESCE(monetary_tag, "Undefined")
	)
;

CREATE TABLE IF NOT EXISTS
	loutruong_om_ui_spend_g_seg LIFECYCLE 7 AS
SELECT
	g_level,
	user_city_name,
	age_group,
	gender,
	monetary_tag,
	om_spend_l30d,
	ui_spend_l30d,
	om_spend_l30d + ui_spend_l30d AS om_ui_spend_l30d,
	om_spend_l90d,
	ui_spend_l90d,
	om_spend_l90d + ui_spend_l90d AS om_ui_spend_l90d
FROM
	(
		SELECT
			g_level,
			user_city_name,
			age_group,
			gender,
			monetary_tag,
			SUM(om_spend_l30d) AS om_spend_l30d,
			SUM(om_spend_l90d) AS om_spend_l90d,
			SUM(ui_spend_l30d) AS ui_spend_l30d,
			SUM(ui_spend_l90d) AS ui_spend_l90d
		FROM
			(
				SELECT
					g_level,
					user_city_name,
					age_group,
					gender,
					monetary_tag,
					COALESCE(om_spend_l30d, 0) AS om_spend_l30d,
					COALESCE(om_spend_l90d, 0) AS om_spend_l90d,
					0                          AS ui_spend_l30d,
					0                          AS ui_spend_l90d
				FROM
					temp_loutruong_om_clm_g_di
				UNION ALL
				SELECT
					g_level,
					user_city_name,
					age_group,
					gender,
					monetary_tag,
					0                                            AS om_spend_l30d,
					0                                            AS om_spend_l90d,
					COALESCE(l30d_net_promotion_spending_usd, 0) AS ui_spend_l30d,
					COALESCE(l90d_net_promotion_spending_usd, 0) AS ui_spend_l90d
				FROM
					tyty_ug_adhoc_buyer_segmentation_final
			)
		GROUP BY
			g_level,
			user_city_name,
			age_group,
			gender,
			monetary_tag
	)
;
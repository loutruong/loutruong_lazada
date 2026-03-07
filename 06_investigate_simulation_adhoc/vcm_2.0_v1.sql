-- MaxCompute SQL 
-- ********************************************************************--
-- author:Truong, Van Thanh
-- create time:2024-07-29 17:30:27
-- ********************************************************************--
SELECT camp_type2,
    bu_1,
    bu_2,
    industry_name,
    COUNT(DISTINCT seller_id) AS total_slr_cnt,
    COUNT(
        DISTINCT CASE
            WHEN LPI_slr_cnt = 1 THEN seller_id
            ELSE NULL
        END
    ) AS LPI_slr_count,
    SUM(cms_price) AS unit_price,
    SUM(fsm_cms) AS fsm_cms,
    SUM(lpi_cms) AS lpi_cms,
    SUM(pmt_fee) AS pmt_fee,
    SUM(LPI_0030_50K) AS LPI_0030_50K,
    SUM(LPI_0030_100K) AS LPI_0030_100K,
    SUM(LPI_0035_100K) AS LPI_0035_100K,
    SUM(LPI_0035_50K) AS LPI_0035_50K,
    SUM(LPI_0040_100K) AS LPI_0040_100K
FROM (
        SELECT t3.ds,
            t4.camp_type2,
            t3.bu_1,
            t3.bu_2,
            t3.seller_id,
            t3.industry_name,
            t3.is_lpi_seller AS LPI_slr_cnt,
            SUM(t3.cms_price) AS cms_price,
            SUM(abs(t3.fsm_cms)) AS fsm_cms,
            SUM(t3.lpi_cms) AS lpi_cms,
            SUM(t3.pmt_fee) AS pmt_fee,
            SUM(t3.LPI_0030_50K) AS LPI_0030_50K,
            SUM(t3.LPI_0030_100K) AS LPI_0030_100K,
            SUM(t3.LPI_0035_100K) AS LPI_0035_100K,
            SUM(t3.LPI_0035_50K) AS LPI_0035_50K,
            SUM(t3.LPI_0040_100K) AS LPI_0040_100K
        FROM (
                SELECT t1.ds,
                    t1.check_out_id,
                    t1.sales_order_id,
                    t1.order_id,
                    t1.sales_order_item_id,
                    t1.bu_1,
                    t1.bu_2,
                    t1.seller_id,
                    t2.industry_name,
                    t2.is_lpi_seller,
                    t1.unit_price AS cms_price,
                    t2.exp_fsm_comm_amt AS fsm_cms,
                    COALESCE(t2.exp_lpi_comm_amt, 0) AS lpi_cms,
                    t2.exp_payment_fee_amt AS pmt_fee,
CASE
                        WHEN t2.is_lpi_seller = 1 THEN LEAST(t1.unit_price * (0.03 / 1.1), 50000)
                        ELSE 0
                    END AS LPI_0030_50K,
CASE
                        WHEN t2.is_lpi_seller = 1 THEN LEAST(t1.unit_price * (0.03 / 1.1), 100000)
                        ELSE 0
                    END AS LPI_0030_100K,
CASE
                        WHEN t2.is_lpi_seller = 1 THEN LEAST(t1.unit_price * (0.035 / 1.1), 100000)
                        ELSE 0
                    END AS LPI_0035_100K,
CASE
                        WHEN t2.is_lpi_seller = 1 THEN LEAST(t1.unit_price * (0.035 / 1.1), 50000)
                        ELSE 0
                    END AS LPI_0035_50K,
CASE
                        WHEN t2.is_lpi_seller = 1 THEN LEAST(t1.unit_price * (0.04 / 1.1), 100000)
                        ELSE 0
                    END AS LPI_0040_100K
                FROM (
                        SELECT ds,
                            check_out_id,
                            sales_order_id,
                            order_id,
                            sales_order_item_id,
                            business_type AS bu_1,
                            business_type_level2 AS bu_2,
                            seller_id,
                            unit_price,
                            actual_gmv,
                            exchange_rate
                        FROM lazada_cdm.dwd_lzd_trd_core_fulfill_di_vn
                        WHERE 1 = 1
                            AND ds >= "${bizdate_from}"
                            AND ds <= "${bizdate_to}"
                            AND COALESCE(business_application, 'LZD') IN ('LZD,ZAL', 'LZD') --< bug shit
                            AND is_revenue = 1
                    ) AS t1
                    LEFT JOIN (
                        SELECT sales_order_item_id,
                            industry_name,
                            seller_id,
                            exp_comm_amt,
                            exp_lpi_comm_amt,
                            exp_fsm_comm_amt,
                            exp_payment_fee_amt,
                            exp_comm_amt_detail,
                            check_out_id,
                            MAX(
                                MAX(
                                    CASE
                                        WHEN exp_lpi_comm_amt > 0 THEN 1
                                        ELSE 0
                                    END
                                )
                            ) OVER (PARTITION BY ds, seller_id) AS is_lpi_seller
                        FROM lazada_analyst_dev.tmp_loutruong_dwd_lzd_fin_trd_commission_di
                        GROUP BY ds,
                            sales_order_item_id,
                            industry_name,
                            seller_id,
                            exp_comm_amt,
                            exp_lpi_comm_amt,
                            exp_fsm_comm_amt,
                            exp_payment_fee_amt,
                            exp_comm_amt_detail,
                            check_out_id
                    ) AS t2 ON t1.sales_order_item_id = t2.sales_order_item_id
            ) AS t3
            LEFT JOIN (
                SELECT ds,
                    day_type,
                    camp_type,
CASE
                        WHEN TOLOWER(camp_type) IN ('mega') THEN 'Mega'
                        WHEN TOLOWER(camp_type) IN ('pd') THEN 'A+'
                        WHEN TOLOWER(camp_type) IN ('holiday') THEN 'BAU'
                        ELSE camp_type
                    END AS camp_type2
                FROM lazada_analyst.cp_calendar
                WHERE 1 = 1
                    AND ds BETWEEN '20240401' AND TO_CHAR(DATEADD(GETDATE(), -1, 'dd'), 'yyyymmdd')
            ) AS t4 ON t3.ds = t4.ds
        GROUP BY t3.ds,
            t4.camp_type2,
            t3.bu_1,
            t3.bu_2,
            t3.seller_id,
            t3.industry_name,
            t3.is_lpi_seller
    )
GROUP BY camp_type2,
    bu_1,
    bu_2,
    industry_name;
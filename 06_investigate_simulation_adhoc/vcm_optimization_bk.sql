-- MaxCompute SQL 
-- ********************************************************************--
-- author:Truong, Van Thanh
-- create time:2024-11-19 12:55:30
-- ********************************************************************--
WITH t_t_zero AS (
    SELECT *,
        CASE
            WHEN z_score BETWEEN lower_bound_1 AND upper_bound_1 THEN 1
            ELSE 0
        END AS is_in_bound_1
    FROM (
            SELECT *,
                (gmv - AVG(gmv) OVER ()) / STDDEV(gmv) OVER () AS z_score,
                AVG(gmv) OVER () AS avg_gmv,
                STDDEV(gmv) OVER () AS std_gmv,
                AVG(gmv) OVER () - STDDEV(gmv) OVER () AS lower_bound_1,
                AVG(gmv) OVER () + STDDEV(gmv) OVER () AS upper_bound_1
            FROM lazada_analyst_dev.tmp_loutruong_vcm_optimization_cohort
            WHERE 1 = 1
                AND mm IN ('T-1')
        )
),
t_t_minus_1 AS (
    SELECT *,
        CASE
            WHEN z_score BETWEEN lower_bound_1 AND upper_bound_1 THEN 1
            ELSE 0
        END AS is_in_bound_1
    FROM (
            SELECT *,
                (gmv - AVG(gmv) OVER ()) / STDDEV(gmv) OVER () AS z_score,
                AVG(gmv) OVER () AS avg_gmv,
                STDDEV(gmv) OVER () AS std_gmv,
                AVG(gmv) OVER () - STDDEV(gmv) OVER () AS lower_bound_1,
                AVG(gmv) OVER () + STDDEV(gmv) OVER () AS upper_bound_1
            FROM lazada_analyst_dev.tmp_loutruong_vcm_optimization_cohort
            WHERE 1 = 1
                AND mm IN ('T0')
        )
),
t_final AS (
    SELECT *,
        ROW_NUMBER() OVER (
            PARTITION BY mm
            ORDER BY seller_id ASC
        ) AS rn
    FROM (
            SELECT *,
                MAX(cum_in_bound_1) OVER (PARTITION BY seller_id) AS number_data_set
            FROM (
                    SELECT *,
                        SUM(is_in_bound_1) OVER (
                            PARTITION BY seller_id
                            ORDER BY mm ASC
                        ) AS cum_in_bound_1
                    FROM (
                            SELECT *
                            FROM t_t_zero
                            UNION ALL
                            SELECT *
                            FROM t_t_minus_1
                        )
                )
        )
)
SELECT *
FROM t_final;
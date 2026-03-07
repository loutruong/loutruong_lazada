-- MaxCompute SQL 
-- ********************************************************************--
-- author:Truong, Van Thanh
-- create time:2024-01-30 12:49:13
-- ********************************************************************--
SELECT member_id,
    member_name,
    group_segment_2,
    segment_2,
    SUM(COALESCE(platform_payout_vnd, 0)) AS platform_payout_vnd,
    SUM(COALESCE(brand_payout_vnd, 0)) AS brand_payout_vnd,
    COALESCE(
        SUM(
            CASE
                WHEN member_id IN (150321263) THEN COALESCE(brand_payout_vnd, 0) * 0.1
                WHEN member_id IN (155861333) THEN COALESCE(brand_payout_vnd, 0) * 0.5
                WHEN member_id IN (150361221) THEN COALESCE(brand_payout_vnd, 0) * 0.5
                WHEN member_id IN (230001252) THEN COALESCE(brand_payout_vnd, 0) * 0.5
                ELSE 0
            END
        ),
        0
    ) AS monthly_program,
    COALESCE(
        COALESCE(
            SUM(
                CASE
                    WHEN member_id IN (150321263)
                    AND ds BETWEEN 20231212 AND 20231214
                    AND seller_id IN (
                        200160891318,
                        1000132285,
                        1000035115,
                        200159784007,
                        1000128028,
                        1000248565,
                        200158218693
                    ) THEN COALESCE(brand_payout_vnd, 0) * 0.5
                    ELSE 0
                END
            ) --<< Need to change 7 seller_id
,
            0
        ) + COALESCE(
            SUM(
                CASE
                    WHEN member_id IN (150321263)
                    AND ds BETWEEN 20231212 AND 20231214
                    AND (
                        hh BETWEEN 07 AND 07
                        OR hh BETWEEN 11 AND 13
                        OR hh BETWEEN 20 AND 23
                    )
                    AND TOLOWER(cluster) IN ('el', 'fashion', 'gm') THEN COALESCE(platform_payout_vnd, 0) * 1
                    ELSE 0
                END
            ),
            0
        ) + COALESCE(
            SUM(
                CASE
                    WHEN member_id IN (150321263)
                    AND ds BETWEEN 20231212 AND 20231214
                    AND (
                        hh BETWEEN 07 AND 07
                        OR hh BETWEEN 11 AND 13
                        OR hh BETWEEN 20 AND 23
                    )
                    AND TOLOWER(cat_lv1) IN ('health', 'beauty', 'mother & baby', 'groceries') THEN COALESCE(platform_payout_vnd, 0) * 1
                    ELSE 0
                END
            ),
            0
        ),
        0
    ) AS double_digit_program,
    COALESCE(
        COALESCE(
            SUM(
                CASE
                    WHEN member_id IN (150321263)
                    AND ds BETWEEN 20231115 AND 20231117
                    AND TOLOWER(bu_lv1) IN ('lazmall') THEN COALESCE(brand_payout_vnd, 0) * 0.5
                    ELSE 0
                END
            ),
            0
        ) + COALESCE(
            SUM(
                CASE
                    WHEN member_id IN (150321263)
                    AND ds BETWEEN 20231212 AND 20231214
                    AND (
                        hh BETWEEN 07 AND 07
                        OR hh BETWEEN 11 AND 13
                        OR hh BETWEEN 20 AND 23
                    )
                    AND TOLOWER(cluster) IN ('el', 'fashion', 'gm') THEN COALESCE(platform_payout_vnd, 0) * 1
                    ELSE 0
                END
            ),
            0
        ) + COALESCE(
            SUM(
                CASE
                    WHEN member_id IN (150321263)
                    AND ds BETWEEN 20231212 AND 20231214
                    AND (
                        hh BETWEEN 07 AND 07
                        OR hh BETWEEN 11 AND 13
                        OR hh BETWEEN 20 AND 23
                    )
                    AND TOLOWER(cat_lv1) IN ('health', 'beauty', 'mother & baby', 'groceries') THEN COALESCE(platform_payout_vnd, 0) * 1
                    ELSE 0
                END
            ),
            0
        ),
        0
    ) AS lms_program,
    COALESCE(
        COALESCE(
            SUM(
                CASE
                    WHEN member_id IN (150321263)
                    AND ds BETWEEN 20231225 AND 20231229
                    AND TOLOWER(bu_lv1) IN ('lazmall') THEN COALESCE(brand_payout_vnd, 0) * 0.5
                    ELSE 0
                END
            ),
            0
        ) + COALESCE(
            SUM(
                CASE
                    WHEN member_id IN (150321263)
                    AND ds BETWEEN 20231225 AND 20231229
                    AND (
                        hh BETWEEN 07 AND 07
                        OR hh BETWEEN 11 AND 13
                        OR hh BETWEEN 20 AND 23
                    )
                    AND TOLOWER(cluster) IN ('el', 'fashion', 'gm') THEN COALESCE(platform_payout_vnd, 0) * 1
                    ELSE 0
                END
            ),
            0
        ) + COALESCE(
            SUM(
                CASE
                    WHEN member_id IN (150321263)
                    AND ds BETWEEN 20231225 AND 20231229
                    AND (
                        hh BETWEEN 07 AND 07
                        OR hh BETWEEN 11 AND 13
                        OR hh BETWEEN 20 AND 23
                    )
                    AND TOLOWER(cat_lv1) IN ('health', 'beauty', 'mother & baby', 'groceries') THEN COALESCE(platform_payout_vnd, 0) * 1
                    ELSE 0
                END
            ),
            0
        ),
        0
    ) AS mes_program,
    COALESCE(
        COALESCE(
            SUM(
                CASE
                    WHEN member_id IN (150321263)
                    AND ds BETWEEN 20231201 AND 20231202
                    AND TOLOWER(cluster) IN ('el')
                    AND TOLOWER(bu_lv1) IN ('lazmall') THEN COALESCE(brand_payout_vnd, 0) * 0.5
                END
            ),
            0
        ) + COALESCE(
            SUM(
                CASE
                    WHEN member_id IN (150321263)
                    AND ds BETWEEN 20231201 AND 20231202
                    AND (
                        hh BETWEEN 07 AND 07
                        OR hh BETWEEN 11 AND 13
                        OR hh BETWEEN 20 AND 23
                    )
                    AND TOLOWER(cluster) IN ('el') THEN COALESCE(platform_payout_vnd, 0) * 1
                END
            ),
            0
        ) + COALESCE(
            SUM(
                CASE
                    WHEN member_id IN (150321263)
                    AND ds BETWEEN 20231202 AND 20231203
                    AND TOLOWER(cluster) IN ('fashion')
                    AND TOLOWER(bu_lv1) IN ('lazmall') THEN COALESCE(brand_payout_vnd, 0) * 0.5
                END
            ),
            0
        ) + COALESCE(
            SUM(
                CASE
                    WHEN member_id IN (150321263)
                    AND ds BETWEEN 20231202 AND 20231203
                    AND (
                        hh BETWEEN 07 AND 07
                        OR hh BETWEEN 11 AND 13
                        OR hh BETWEEN 20 AND 23
                    )
                    AND TOLOWER(cluster) IN ('fashion') THEN COALESCE(platform_payout_vnd, 0) * 1
                END
            ),
            0
        ) + COALESCE(
            SUM(
                CASE
                    WHEN member_id IN (150321263)
                    AND ds BETWEEN 20231218 AND 20231218
                    AND TOLOWER(cat_lv1) IN ('health', 'beauty')
                    AND TOLOWER(bu_lv1) IN ('lazmall') THEN COALESCE(brand_payout_vnd, 0) * 0.5
                END
            ),
            0
        ) + COALESCE(
            SUM(
                CASE
                    WHEN member_id IN (150321263)
                    AND ds BETWEEN 20231218 AND 20231218
                    AND (
                        hh BETWEEN 07 AND 07
                        OR hh BETWEEN 11 AND 13
                        OR hh BETWEEN 20 AND 23
                    )
                    AND TOLOWER(cat_lv1) IN ('health', 'beauty') THEN COALESCE(platform_payout_vnd, 0) * 1
                END
            ),
            0
        ) + COALESCE(
            SUM(
                CASE
                    WHEN member_id IN (150321263)
                    AND ds BETWEEN 20231221 AND 20231221
                    AND TOLOWER(cat_lv1) IN ('groceries')
                    AND TOLOWER(bu_lv1) IN ('lazmall') THEN COALESCE(brand_payout_vnd, 0) * 0.5
                END
            ),
            0
        ) + COALESCE(
            SUM(
                CASE
                    WHEN member_id IN (150321263)
                    AND ds BETWEEN 20231221 AND 20231221
                    AND (
                        hh BETWEEN 07 AND 07
                        OR hh BETWEEN 11 AND 13
                        OR hh BETWEEN 20 AND 23
                    )
                    AND TOLOWER(cat_lv1) IN ('groceries') THEN COALESCE(platform_payout_vnd, 0) * 1
                END
            ),
            0
        ) + COALESCE(
            SUM(
                CASE
                    WHEN member_id IN (150321263)
                    AND ds BETWEEN 20231222 AND 20231222
                    AND TOLOWER(cat_lv1) IN ('health', 'beauty')
                    AND TOLOWER(bu_lv1) IN ('lazmall') THEN COALESCE(brand_payout_vnd, 0) * 0.5
                END
            ),
            0
        ) + COALESCE(
            SUM(
                CASE
                    WHEN member_id IN (150321263)
                    AND ds BETWEEN 20231222 AND 20231222
                    AND (
                        hh BETWEEN 07 AND 07
                        OR hh BETWEEN 11 AND 13
                        OR hh BETWEEN 20 AND 23
                    )
                    AND TOLOWER(cat_lv1) IN ('health', 'beauty') THEN COALESCE(platform_payout_vnd, 0) * 1
                END
            ),
            0
        ) + COALESCE(
            SUM(
                CASE
                    WHEN member_id IN (150321263)
                    AND ds BETWEEN 20231223 AND 20231223
                    AND TOLOWER(cat_lv1) IN ('mother & baby')
                    AND TOLOWER(bu_lv1) IN ('lazmall') THEN COALESCE(brand_payout_vnd, 0) * 0.5
                END
            ),
            0
        ) + COALESCE(
            SUM(
                CASE
                    WHEN member_id IN (150321263)
                    AND ds BETWEEN 20231223 AND 20231223
                    AND (
                        hh BETWEEN 07 AND 07
                        OR hh BETWEEN 11 AND 13
                        OR hh BETWEEN 20 AND 23
                    )
                    AND TOLOWER(cat_lv1) IN ('mother & baby') THEN COALESCE(platform_payout_vnd, 0) * 1
                END
            ),
            0
        ) + COALESCE(
            SUM(
                CASE
                    WHEN member_id IN (150321263)
                    AND ds BETWEEN 20231222 AND 20231224
                    AND TOLOWER(cluster) IN ('el')
                    AND TOLOWER(bu_lv1) IN ('lazmall') THEN COALESCE(brand_payout_vnd, 0) * 0.5
                END
            ),
            0
        ) + COALESCE(
            SUM(
                CASE
                    WHEN member_id IN (150321263)
                    AND ds BETWEEN 20231222 AND 20231224
                    AND (
                        hh BETWEEN 07 AND 07
                        OR hh BETWEEN 11 AND 13
                        OR hh BETWEEN 20 AND 23
                    )
                    AND TOLOWER(cluster) IN ('el') THEN COALESCE(platform_payout_vnd, 0) * 1
                END
            ),
            0
        ) + COALESCE(
            SUM(
                CASE
                    WHEN member_id IN (150321263)
                    AND ds BETWEEN 20231230 AND 20231231
                    AND TOLOWER(cluster) IN ('gm')
                    AND TOLOWER(bu_lv1) IN ('lazmall') THEN COALESCE(brand_payout_vnd, 0) * 0.5
                END
            ),
            0
        ) + COALESCE(
            SUM(
                CASE
                    WHEN member_id IN (150321263)
                    AND ds BETWEEN 20231230 AND 20231231
                    AND (
                        hh BETWEEN 07 AND 07
                        OR hh BETWEEN 11 AND 13
                        OR hh BETWEEN 20 AND 23
                    )
                    AND TOLOWER(cluster) IN ('gm') THEN COALESCE(platform_payout_vnd, 0) * 1
                END
            ),
            0
        ),
        0
    ) AS sbd_cd_program
FROM lazada_analyst_dev.loutruong_aff_console_di
WHERE 1 = 1
    AND TO_CHAR(TO_DATE(ds, 'yyyymmdd'), 'yyyymm') = 202312
    AND TOLOWER(adjust_type) NOT IN ('stop_first_order')
    AND is_fraud = 0
    AND TOLOWER(status) IN ('delivered')
    AND member_id IN (155861333, 150321263, 150361221, 230001252)
GROUP BY member_id,
    member_name,
    group_segment_2,
    segment_2;
SELECT member_id,
    member_name,
    segment_2,
    SUM(
        CASE
            WHEN member_id IN (150321263) THEN COALESCE(brand_payout_vnd, 0) * 0.3
            WHEN member_id IN (155861333, 230001252) THEN COALESCE(brand_payout_vnd, 0) * 0.5
            ELSE 0
        END
    ) AS extra
FROM lazada_analyst_dev.loutruong_aff_console_di
WHERE 1 = 1
    AND ds BETWEEN 20240101 AND 20240202
    AND TOLOWER(adjust_type) NOT IN ('stop_first_order')
    AND is_fraud = 0
    AND TOLOWER(status) IN ('delivered')
    AND member_id IN (150321263, 155861333, 230001252)
GROUP BY member_id,
    member_name,
    segment_2;
SELECT member_id,
    member_name,
    segment_2,
    SUM(COALESCE(brand_payout_vnd, 0) * 0.2) AS extra
FROM lazada_analyst_dev.loutruong_aff_console_di
WHERE 1 = 1
    AND ds BETWEEN 20240215 AND 20240225
    AND TOLOWER(adjust_type) NOT IN ('stop_first_order')
    AND is_fraud = 0
    AND TOLOWER(status) IN ('delivered')
    AND member_id IN (150321263, 155861333, 230001252)
GROUP BY member_id,
    member_name,
    segment_2;
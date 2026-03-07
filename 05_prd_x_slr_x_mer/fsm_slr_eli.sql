-- MaxCompute SQL 
-- ********************************************************************--
-- author:Truong, Van Thanh
-- create time:2024-07-25 11:14:55
-- ********************************************************************--
DROP TABLE IF EXISTS lazada_analyst_dev.TT_Seller_will_be_invited_FSMax_Monday_update;
CREATE TABLE lazada_analyst_dev.TT_Seller_will_be_invited_FSMax_Monday_update LIFECYCLE 9 AS
SELECT DISTINCT base.*
FROM (
        SELECT *
        FROM lazada_analyst_dev.TT_Seller_fsmax_with_criteria --<< Updated every sunday by 19h 
        WHERE 1 = 1
            AND seller_status = 1 --<< Active seller
            AND COALESCE(purchasable_product_count, 0) > 0 --
            -- AND     COALESCE(is_dbs,0) = 0
            AND is_invited_FSM = 0
            AND cluster_name NOT IN ('DG')
            AND COALESCE(closing_balance, 0) >= 0
            AND business_type_level2 IN (
                'LazMall - MP local',
                'LazMall - MP cb',
                'Lazada - MP',
                'LazGlobal - Non TBC'
            ) --
            -- AND     (
            --             (
            --                         business_type_level2 IN ('LazMall - MP local','LazMall - MP cb')
            --             )
            --             OR      (
            --                         business_type_level2 = 'Lazada - MP'
            --                         AND     is_emerging_plus = 1
            --             )
            --             OR      (
            --                         business_type_level2 = 'Lazada - MP'
            --                         AND     is_hunted_Seller = 1
            --                         AND     is_new_live_seller = 1
            --                         AND     COALESCE(purchasable_product_count,0) >= 30
            --             )
            --             OR      (
            --                         business_type_level2 = 'Lazada - MP'
            --                         AND     COALESCE(is_hunted_Seller,0) = 0
            --                         AND     is_new_live_seller = 1
            --                         AND     COALESCE(purchasable_product_count,0) >= 10
            --             ) --<< Small seller only apply for existing, not apply for new
            --             OR      (
            --                         business_type_level2 = 'Lazada - MP'
            --                         AND     seller_segment_id_v3 IN (5,6,7)
            --                         AND     COALESCE(is_new_live_seller,0) = 0
            --                         AND     COALESCE(purchasable_product_count,0) >= 5
            --                         AND     COALESCE(L30D_orders,0) >= 10
            --             ) --<< 20210314 Release criteria for HN Seller L30D_Order >= 5
            --             OR      (
            --                         business_type_level2 = 'Lazada - MP'
            --                         AND     seller_segment_id_v3 IN (5,6,7)
            --                         AND     COALESCE(is_new_live_seller,0) = 0
            --                         AND     COALESCE(purchasable_product_count,0) >= 5
            --                         AND     warehouse_region1_name = 'Hà Nội'
            --                         AND     COALESCE(L30D_orders,0) >= 5
            --             )
            --             OR      (
            --                         business_type_level2 = 'LazGlobal - Non TBC'
            --                         AND     is_emerging_plus = 1
            --                         AND     cancellation_percent < 0.0245
            --                         AND     (
            --                                     positive_seller_rating >= 94.45
            --                                     OR      positive_seller_rating IS NULL
            --                         )
            --             )
            --             OR      (
            --                         business_type_level2 = 'LazGlobal - Non TBC'
            --                         AND     seller_segment_id_v3 IN (5,6,7)
            --                         AND     cancellation_percent < 0.0245
            --                         AND     (
            --                                     positive_seller_rating >= 94.45
            --                                     OR      positive_seller_rating IS NULL
            --                         )
            --                         AND     COALESCE(L30D_orders / 30,0) >= 1
            --             )
            -- )
    ) AS base
WHERE 1 = 1
    AND base.seller_id NOT IN (
        SELECT DISTINCT seller_id
        FROM lazada_analyst_dev.TT_FSMax_seller_daily_status
        WHERE 1 = 1
            AND ds >= TO_CHAR(DATEADD(GETDATE(), -3, 'dd'), 'yyyymmdd')
            AND ds <= TO_CHAR(DATEADD(GETDATE(), -1, 'dd'), 'yyyymmdd')
    )
    AND base.seller_id NOT IN (
        SELECT DISTINCT seller_id
        FROM lazada_analyst_dev.SBP_invited_seller_list
        WHERE 1 = 1
            AND ds <= TO_CHAR(DATEADD(GETDATE(), -1, 'dd'), 'yyyymmdd')
    );
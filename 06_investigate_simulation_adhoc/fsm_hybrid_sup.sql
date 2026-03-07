-- MaxCompute SQL 
-- ********************************************************************--
-- author:Truong, Van Thanh
-- create time:2024-10-11 18:07:37
-- ********************************************************************--
-- DROP TABLE IF EXISTS lazada_analyst_dev.tmp_loutruong_1
-- ;
-- CREATE TABLE IF NOT EXISTS lazada_analyst_dev.tmp_loutruong_1
-- LIFECYCLE 30 AS
-- SELECT  '1' AS data_set
--         ,user_id
-- FROM    lazada_ab.dim_lzd_ab_users_d
-- WHERE   1 = 1
-- AND     ds BETWEEN 20240715 AND 20240723
-- AND     venture = 'VN'
-- AND     bucket_id IN (1234378,1241607,1251580)
-- GROUP BY user_id
-- ;
-- DROP TABLE IF EXISTS lazada_analyst_dev.tmp_loutruong_fsm_usr_distribute_status_1
-- ;
-- CREATE TABLE IF NOT EXISTS lazada_analyst_dev.loutruong_dim_ab_di
-- (
--     stat_date                 STRING
--     ,venture                  STRING
--     ,user_id                  STRING
--     ,release_id               STRING
--     ,bucket_id                STRING
--     ,aliabtest                STRING
--     ,first_bucket_date        STRING
--     ,cur_date_first_bucket_ts BIGINT
-- )
-- PARTITIONED BY 
-- (
--     ds                        STRING COMMENT 'yyyymmdd'
-- )
-- LIFECYCLE 180
-- ;
INSERT OVERWRITE TABLE lazada_analyst_dev.loutruong_dim_ab_di PARTITION (ds)
SELECT stat_date,
    venture,
    user_id,
    release_id,
    bucket_id,
    aliabtest,
    first_bucket_date,
    cur_date_first_bucket_ts,
    ds
FROM lazada_ab.dim_lzd_ab_users_d
WHERE 1 = 1
    AND ds BETWEEN '${start}' AND '${end}'
    AND venture = 'VN'
    AND bucket_id IN (1234378, 1241607, 1251580);
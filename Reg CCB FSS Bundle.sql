WITH 
grass_date(grass_date) AS (SELECT * FROM UNNEST(SEQUENCE(date'2021-01-01', current_date - interval '1' day)))
, ex AS (
   SELECT
     grass_date
   , exchange_rate
   FROM
     shopee_vn.dim_exrate
   WHERE (country = 'VN')
  ) 

, service_fee_id AS (
  SELECT *
  FROM
     shopee_vn.shopee_vn_op_team__payment_service_fee_rule_list
  WHERE (ingestion_timestamp = (SELECT "max"(ingestion_timestamp) col_1
  FROM
  shopee_vn.shopee_vn_op_team__payment_service_fee_rule_list
  ))) 
  
, rev AS (
   SELECT
     orderid
    , "sum"((CASE WHEN (s.rule_name = 'Freeship_Extra') THEN (CAST("json_extract_scalar"(service_fee_rule_json, '$.fee_amt') AS double) / e.exchange_rate/1.1) ELSE 0 END)) fss_rev
    , "sum"((CASE WHEN (s.rule_name = 'CCB') THEN (CAST("json_extract_scalar"(service_fee_rule_json, '$.fee_amt') AS double) / e.exchange_rate/1.1) ELSE 0 END)) ccb_rev
    , sum(coalesce(commission_fee_usd,0)) base_comm_usd
    FROM
     (((
       (
          SELECT  
                  item_id itemid
                  ,order_id orderid
              ,service_fee_info_list service_fee_info
              ,CAST(split(create_datetime, ' ')[1] AS DATE) AS grass_date
              ,commission_fee_usd
          FROM shopee_vn.order_mart_dwd_order_item_all_event_final_status_df
          where CAST(split(create_datetime, ' ')[1] AS DATE)  between date '2020-07-01' AND current_date - interval '1' day
          and is_net_order = 1
       ) o
   CROSS JOIN UNNEST(CAST("json_parse"(service_fee_info) AS array(json))) t (service_fee_rule_json))
   INNER JOIN (
      SELECT DISTINCT *
      FROM
        service_fee_id
   )  s ON (CAST("json_extract_scalar"(service_fee_rule_json, '$.rule_id') AS integer) = CAST(s.rule_id AS bigint)))
   LEFT JOIN ex e ON (e.grass_date = o.grass_date))
   GROUP BY 1
  ) ,

--CCB

ccb_list AS (
(
      SELECT
        shop_id
      , (CASE WHEN (start_time IS NULL) THEN (current_date - INTERVAL  '1' DAY) ELSE TRY_CAST(start_time AS date) END) start_time
      , TRY_CAST(end_time AS date) end_time
      , 1 is_ccb
      FROM
        shopee_vn.shopee_vn_bi_team__ccb_seller_list
      WHERE (ingestion_timestamp = (SELECT "max"(ingestion_timestamp) col_2
FROM
  shopee_vn.shopee_vn_bi_team__ccb_seller_list
))
   ) UNION ALL (
      SELECT
        shopid
      , TRY_CAST(start_time AS date) start_time
      , TRY_CAST(end_time AS date) end_time
      , 2 is_ccb
      FROM
        shopee_vn.shopee_vn_bi_team__ccb_free_trial_list
      WHERE (ingestion_timestamp = (SELECT "max"(ingestion_timestamp) col_3
FROM
  shopee_vn.shopee_vn_bi_team__ccb_free_trial_list
))
   ) )
   
,ccb_shop_by_date AS (
   SELECT
     grass_date
   , cast(f.shop_id as bigint) shop_id
   , "min"(is_ccb) is_ccb
   FROM
     (ccb_list f
   INNER JOIN grass_date g ON (g.grass_date BETWEEN "date"(f.start_time) AND COALESCE("date"(f.end_time), (current_date - INTERVAL  '1' DAY))))
   GROUP BY 1, 2
)


--FSS
, fss_seller_list as
((SELECT -- paid fss local list 
    shopid,
    start_time,
    end_time,
    1 AS is_fss
FROM
    shopee_vn_bi_team__fss_seller_list   
WHERE
    ingestion_timestamp = (SELECT MAX(ingestion_timestamp) FROM shopee_vn_bi_team__fss_seller_list))
UNION ALL
(SELECT -- free trial fss local
    shopid,
    start_time,
    end_time,
    2 AS is_fss
FROM
    shopee_vn_bi_team__fss_free_trial
WHERE
    ingestion_timestamp =(SELECT MAX(ingestion_timestamp) FROM shopee_vn_bi_team__fss_free_trial)
    )
UNION ALL
(SELECT -- paid and free trial fss cb
    shopid,
    start_time,
    end_time,
    CASE WHEN is_free = '1' THEN 2 ELSE 1 END as is_fss
FROM
    shopee_vn_bt_team__cb_freeship_package_seller
WHERE ingestion_timestamp != 'a'
--     ingestion_timestamp =(SELECT MAX(ingestion_timestamp) FROM shopee_vn_bt_team__cb_freeship_package_seller)
    )    
)


, fss_shop_by_date AS -- consolidated fss list by date
(SELECT
    grass_date,
    try_cast(f.shopid as bigint) shopid,
    MIN(is_fss) AS is_fss
FROM
    fss_seller_list f
JOIN
    grass_date g
ON
    g.grass_date BETWEEN TRY_CAST(f.start_time AS DATE) AND COALESCE(TRY_CAST(f.end_time AS DATE), CURRENT_DATE - INTERVAL '1' DAY)
GROUP BY 1, 2)


, seller_segment as 
    (SELECT
        u.grass_date
        , case when f.shopid is not null and c.shop_id is not null then 'FSS+CCB' 
                when f.shopid is not null then 'FSS Only'
                when c.shop_id is not null then 'CCB Only'
                else 'Other' end as segment 
        , u.shop_id
    FROM user_mart_dim_shop u 
    LEFT JOIN fss_shop_by_date f on u.grass_date = f.grass_date and u.shop_id = f.shopid
    LEFT JOIN ccb_shop_by_date c on u.grass_date = c.grass_date and c.shop_id = u.shop_id
    WHERE u.grass_date between date'2020-07-01' and current_date - interval '1' day
        )

, ccb_incremental_coin_cost as (
    select
        DATE(split(create_datetime, ' ')[1]) grass_date
        , coalesce(segment,'Other') segment
        , coalesce(sum(case when is_net_order = 1 and pv_coin_earn_by_shopee_amt_usd > 0 then coalesce(pv_coin_earn_by_shopee_amt_usd,0) end), 0) as ccb_incremental_coin_cost
    from shopee_vn.order_mart_dwd_order_item_all_event_final_status_df a
    left join seller_segment s on a.shop_id = s.shop_id and CAST(split(create_datetime, ' ')[1] AS DATE) = s.grass_date
    where
        DATE(split(create_datetime, ' ')[1]) BETWEEN date'2020-07-01' AND CURRENT_DATE - interval '1' day  
        and is_net_order = 1
        and (pv_voucher_code like 'CCB%')
    group by 1,2
    )


--LOGISTICS COST
, pickup_order AS 
    (
    SELECT 
        distinct orderid
    FROM 
        shopee_logistics_audit_v3_db__logistics_audit_tab
    WHERE 
        new_status =2
    )
, schedule_order AS 
    (
    SELECT 
        distinct orderid
    FROM 
        shopee_logistics_audit_v3_db__logistics_audit_tab
    WHERE 
        new_status =1
    )
, pickup_failed AS 
    (
    SELECT 
        distinct orderid
    FROM 
        shopee_logistics_audit_v3_db__logistics_audit_tab
    WHERE 
        new_status = 4
    )
--  select FSVs airpay sponsor 
, airpay_fsv AS 
(SELECT
    *
FROM
    shopee_vn_bi_team__fsv_type
WHERE
    ingestion_timestamp = (SELECT MAX(ingestion_timestamp) FROM shopee_vn_bi_team__fsv_type)
AND
    fsv_type like '%FSS%')

, data AS 
    ((SELECT
        shop_id shopid,
            DATE(CAST(o.create_datetime AS TIMESTAMP)) AS grass_date,
            order_id,
            CASE WHEN p.orderid IS NOT NULL THEN 1 --  pickup order
                WHEN s.orderid IS NOT NULL AND  pf.orderid IS NULL AND o.order_be_status <> 'INVALID' THEN 1
                WHEN o.order_be_status IN ('ESCROW_PENDING','ESCROW_VERIFIED','ESCROW_CREATED','ESCROW_PAYOUT','ESCROW_PAID','PAID','COMPLETED','UNPAID') THEN 1 -- if not yet picked up, use not yet cancelled order
             ELSE 0 END AS is_pick_up,
            cpo.scheduled_net_cpo,
            CASE WHEN a.promotionid IS NULL THEN 0 ELSE 1 END AS is_ap
        FROM
            order_mart_dwd_order_all_event_final_status_df o 
        LEFT JOIN
            shopee_vn_anlys.shopee_bi_team_logistics_masking_cpo cpo
        ON
            cpo.orderid = o.order_id
        LEFT JOIN
            pickup_order p
        ON
            p.orderid = o.order_id
        LEFT JOIN
            schedule_order s
        ON
            s.orderid = o.order_id
        LEFT JOIN
            pickup_failed pf
        ON
            pf.orderid = o.order_id
        LEFT JOIN
            airpay_fsv a
        ON
            TRY_CAST(a.promotionid AS BIGINT) = o.fsv_promotion_id
        WHERE
            DATE(CAST(o.create_datetime AS TIMESTAMP)) BETWEEN date'2021-01-01' AND date_add('day',-1,current_date)
        )
        UNION
        (SELECT
        shop_id shopid,
            DATE(CAST(o.create_datetime AS TIMESTAMP)) AS grass_date,
            order_id,
            CASE WHEN p.orderid IS NOT NULL THEN 1 --  pickup order
                WHEN s.orderid IS NOT NULL AND  pf.orderid IS NULL AND o.order_be_status <> 'INVALID' THEN 1
                WHEN o.order_be_status IN ('ESCROW_PENDING','ESCROW_VERIFIED','ESCROW_CREATED','ESCROW_PAYOUT','ESCROW_PAID','PAID','COMPLETED','UNPAID') THEN 1 -- if not yet picked up, use not yet cancelled order
             ELSE 0 END AS is_pick_up,
            cpo.scheduled_net_cpo,
            CASE WHEN a.promotionid IS NULL THEN 0 ELSE 1 END AS is_ap
        FROM
            order_mart_dwd_order_all_event_final_status_df o 
        LEFT JOIN
            shopee_vn_anlys.shopee_bi_team_logistics_masking_cpo cpo
        ON
            cpo.orderid = o.order_id
        LEFT JOIN
            pickup_order p
        ON
            p.orderid = o.order_id
        LEFT JOIN
            schedule_order s
        ON
            s.orderid = o.order_id
        LEFT JOIN
            pickup_failed pf
        ON
            pf.orderid = o.order_id
        LEFT JOIN
            airpay_fsv a
        ON
            TRY_CAST(a.promotionid AS BIGINT) = o.fsv_promotion_id
        WHERE
            DATE(CAST(o.create_datetime AS TIMESTAMP)) BETWEEN date'2020-07-01' AND date'2020-12-31'
        ))

, fss_incremental_cost as (
SELECT
    a.grass_date,
    coalesce(c.segment,'Other') segment,
    COUNT(DISTINCT CASE WHEN is_pick_up = 1 THEN order_id ELSE NULL END) AS pickup_orders,
    SUM(CASE WHEN is_pick_up = 1 THEN scheduled_net_cpo ELSE 0 END) AS  fss_incremental_cost, 
    SUM(CASE WHEN is_pick_up = 1 AND is_ap <> 1 THEN scheduled_net_cpo ELSE 0 END) AS cpo_exc_airpay
FROM
    data a
left join seller_segment c on a.shopid = c.shop_id and a.grass_date = c.grass_date
GROUP BY 1,2)

, sales as (
select
    DATE(CAST(create_datetime AS TIMESTAMP)) grass_date
    , coalesce(segment,'Other') segment
    , sum(gmv_usd) as gmv
    , count(distinct a.order_id) ado
    , count(distinct a.shop_id) as selling_sellers
from order_mart_dwd_order_all_event_final_status_df a
left join seller_segment b on a.shop_id = b.shop_id and DATE(CAST(create_datetime AS TIMESTAMP)) = b.grass_date
where DATE(CAST(create_datetime AS TIMESTAMP)) >= date '2020-07-01' and (a.bi_exclude_reason is null)
group by 1,2
)

, revenue as
(SELECT
  CAST(split(create_datetime, ' ')[1] AS DATE) grass_date
, coalesce(c.segment,'Other') segment
-- , coalesce(b.is_fss,0) is_fss
, "sum"(COALESCE(ccb_rev, 0)) ccb_rev_usd
, "sum"(COALESCE(fss_rev, 0)) fss_rev_usd
FROM shopee_vn.order_mart_dwd_order_item_all_event_final_status_df a
left join seller_segment c on a.shop_id = c.shop_id and CAST(split(create_datetime, ' ')[1] AS DATE) = c.grass_date
LEFT JOIN rev r ON r.orderid = a.order_id
WHERE (1 = 1) AND is_net_order = 1 and CAST(split(create_datetime, ' ')[1] AS DATE)  between date '2020-07-01' AND current_date - interval '1' day
GROUP BY 1, 2)
, raw as 
(select
    coalesce(a.grass_date, b.grass_date, c.grass_date, d.grass_date) as grass_date
    , coalesce(a.segment, b.segment, c.segment, d.segment) as segment
    , coalesce(selling_sellers, 0) as selling_sellers 
    , coalesce(ado, 0) as ado 
    , coalesce(gmv, 0) as gmv
    , coalesce(ccb_rev_usd, 0) as ccb_rev_usd 
    , coalesce(fss_rev_usd, 0) as fss_rev_usd 
    , coalesce(ccb_incremental_coin_cost, 0) as ccb_incremental_coin_cost 
    , coalesce(fss_incremental_cost, 0) as fss_incremental_log_cost 
from 
    sales a
full join revenue b on a.grass_date = b.grass_date and a.segment = b.segment
full join fss_incremental_cost c on a.grass_date = c.grass_date and a.segment = c.segment
full join ccb_incremental_coin_cost d on a.grass_date = d.grass_date and a.segment = d.segment)

select date_trunc('month',grass_date) month, * from raw 

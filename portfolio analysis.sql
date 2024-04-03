DROP TABLE IF EXISTS tutorial.mz_portfolio_base_table_202403_YTD;
CREATE TABLE tutorial.mz_portfolio_base_table_202403_YTD AS
WITH all_purchase_rk AS (
  SELECT crm_member_id,
        original_order_id,
        order_paid_time,
        ROW_NUMBER () OVER (PARTITION BY crm_member_id ORDER BY order_paid_time ASC) AS rk
FROM (
SELECT DISTINCT trans.crm_member_id,
   trans.original_order_id,
   orders.order_paid_time
FROM edw.f_member_order_detail trans
LEFT JOIN (SELECT original_store_code, 
                  original_order_id,
                  MIN(order_paid_time) AS order_paid_time
            FROM edw.f_lcs_order_detail
             GROUP BY  1,2
        ) orders
      ON trans.original_order_id = CONCAT(orders.original_store_code, orders.original_order_id)
    WHERE is_rrp_sales_type = 1 
      AND if_eff_order_tag IS TRUE
      )
),

trans AS (
 SELECT DISTINCT 
           trans.crm_member_id,
           trans.lego_sku_id,
           trans.product_rrp,
           CASE WHEN adult_sku.lego_sku_id IS NOT NULL THEN 'adult product' ELSE 'kids product' END     AS kids_vs_adult_sku
    FROM edw.f_member_order_detail trans
LEFT JOIN tutorial.adult_sku_list_roy_v2  adult_sku
       ON trans.lego_sku_id = adult_sku.lego_sku_id
     WHERE is_rrp_sales_type = 1 
      AND if_eff_order_tag IS TRUE 
      AND crm_member_id IS NOT NULL
    ),
  
  member_x_product_tag AS (
  SELECT crm_member_id,
         MAX(CASE WHEN kids_vs_adult_sku = 'adult product' THEN 1 ELSE 0 END) AS purchased_adults,
         MAX(CASE WHEN kids_vs_adult_sku = 'kids product' THEN 1 ELSE 0 END)  AS purchased_kids
    FROM trans
    GROUP BY 1
    ),
    
    
  member_final_tag AS (
    SELECT crm_member_id,
           CASE WHEN purchased_adults = 0 AND purchased_kids = 1 THEN 'only_kids'
                WHEN purchased_adults = 1 AND purchased_kids = 0 THEN 'only_adult'
                WHEN purchased_adults = 1 AND purchased_kids = 1 THEN 'both_kids_and_adult' 
                ELSE 'NA'
             END           AS kids_vs_adult_member
    FROM member_x_product_tag
    ),
	
	
member_profile AS (
    
SELECT mbr.member_detail_id,
       CASE WHEN mbr.gender = 1 THEN 'male' 
            WHEN mbr.gender = 2 THEN 'female'
            WHEN mbr.gender = 0 THEN 'gender_unknown' END                                                                 AS gender,
       2024 - EXTRACT('year' FROM DATE(mbr.birthday))                                                                     AS age,
       CASE WHEN beneficiary_birthday IS NULL THEN 0 ELSE 1 END                                                           AS has_birthday,
       CASE WHEN (kids_birthday.member_detail_id IS NOT NULL) OR (mbr.has_kid = 1) THEN 'has kid' ELSE 'no kid' END       AS has_kid,
       member_final_tag.kids_vs_adult_member                                                                              AS lcs_kids_vs_adult_member_shopper,
       tier_code
  FROM edw.d_member_detail mbr
  LEFT JOIN (SELECT DISTINCT member_detail_id
               FROM edw.d_dl_crm_birthday_history kids_birthday
               WHERE person_type = 2
             ) kids_birthday
         ON mbr.member_detail_id::integer = kids_birthday.member_detail_id::integer
   LEFT JOIN member_final_tag
         ON mbr.member_detail_id::integer = member_final_tag.crm_member_id::integer
)

  SELECT DISTINCT 
           trans.crm_member_id,
           date_id,
           CASE WHEN trans.crm_member_id IS NULL THEN 'non_member' ELSE 'member' END AS is_member_sales, 
           if_eff_order_tag,  -- 后续聚合时需要用来判断是否为有效订单
           
           
           --------------------------------------------------------------------------
		   -- profile: 性别，年龄，城市，新老客
           member_profile.gender,
           member_profile.age,
           member_profile.has_kid,
           member_profile.lcs_kids_vs_adult_member_shopper,
           CASE WHEN trans.crm_member_id IS NULL THEN 'non_member'
                WHEN new_member.member_detail_id IS NOT NULL THEN 'new_member' 
                WHEN trans.crm_member_id IS NOT NULL AND new_member.member_detail_id IS NULL THEN 'existing_member' 
           END                                                                                                 AS new_vs_existing_member,
             ps.city_cn                                                                                        AS city_cn,
           CASE WHEN city_tier_list.city_tier in ('Tier 4', 'Tier 5', 'Tier 6') THEN 'Tier 4+'
                ELSE city_tier_list.city_tier
           END                                                                                                 AS city_tier,
           
           CASE WHEN ps.city_maturity_type IS NULL THEN 'not specified' ELSE ps.city_maturity_type END         AS city_maturity_type,
           
           -----------------------------------------------------------------------
		   --- 订单维度： 是lifestage首单还是复购
           trans.original_order_id,
           all_purchase_rk.rk                                                                                  AS order_rk,
           CASE WHEN trans.crm_member_id IS NULL THEN 'non_member'
                WHEN new_member.member_detail_id IS NOT NULL AND all_purchase_rk.rk = 1 THEN 'purely_new_0_1'
                WHEN new_member.member_detail_id IS NULL AND all_purchase_rk.rk = 1 THEN 'existing_0_1'
                WHEN all_purchase_rk.rk >= 2 THEN 'repurchase'
           END                                                                                                 AS order_type_initial_vs_repurchase,
           
           -------------------------------------------------------------------------
		   --- sku维度： 是否新品，产品rrp，产品是kids 还是adult，产品price point
           trans.lego_sku_id,
           product.lego_sku_name_cn                                                                           AS lego_sku_name_cn,
           product.cn_line,
           product.product_cn_lcs_launch_date,
           CASE WHEN product.product_cn_lcs_launch_date >= '2024-01-01' THEN 1 ELSE 0 END                      AS novelty, 
           product.product_rrp                                                                                 AS product_rrp,
           CASE WHEN adult_sku.lego_sku_id IS NOT NULL THEN 'adult product' ELSE 'kids product' END            AS kids_vs_adult_sku,
           CASE WHEN adult_sku.lego_sku_id IS NOT NULL AND product.product_rrp >= 499 AND product.product_rrp < 1600 THEN 'MPP'
                  WHEN adult_sku.lego_sku_id IS NOT NULL AND product.product_rrp < 499 THEN 'LPP'
                  WHEN adult_sku.lego_sku_id IS NOT NULL AND product.product_rrp >= 1600 THEN 'HPP'
                  WHEN adult_sku.lego_sku_id IS NULL AND product.product_rrp >= 399 AND product.product_rrp < 1000 THEN 'MPP'
                  WHEN adult_sku.lego_sku_id IS NULL AND product.product_rrp < 399 THEN 'LPP'
                  WHEN adult_sku.lego_sku_id IS NULL AND product.product_rrp >= 1000 THEN 'HPP'
            END                                                                                                AS product_rrp_price_range,
           -----------------------------------------------------------------------------------------------------------
           -- 卖的件数和钱
           trans.sales_qty                                                                                     AS sales_qty,
           order_rrp_amt                                                                                       AS order_rrp_amt   --- 在tableau聚合是要加sales qty判断逻辑
      FROM edw.f_member_order_detail trans
   LEFT JOIN member_profile
          ON trans.crm_member_id::integer = member_profile.member_detail_id::integer
  LEFT JOIN tutorial.adult_sku_list_roy_v2            adult_sku
          ON trans.lego_sku_id = adult_sku.lego_sku_id
   LEFT JOIN (SELECT DISTINCT lego_store_code, city_cn, city_en, city_maturity.city_type AS city_maturity_type
                FROM  edw.d_dl_phy_store store
                LEFT JOIN tutorial.mkt_city_type_roy_v1 city_maturity  
                      ON city_maturity.city_chn = store.city_cn
              ) as ps 
          ON trans.lego_store_code = ps.lego_store_code
   LEFT JOIN tutorial.mz_city_tier_v2 city_tier_list
          ON UPPER(ps.city_en) = UPPER(city_tier_list.city_eng)
   LEFT JOIN (SELECT DISTINCT member_detail_id 
                FROM edw.d_member_detail 
               WHERE DATE(join_time) >= '2024-01-01'
                 AND DATE(join_time) < '2024-03-01'
                 AND eff_reg_channel LIKE '%LCS%'
             ) new_member
           ON trans.crm_member_id::integer = new_member.member_detail_id::integer
    LEFT JOIN all_purchase_rk
           ON trans.crm_member_id::integer = all_purchase_rk.crm_member_id::integer
          AND trans.original_order_id = all_purchase_rk.original_order_id
    LEFT JOIN (
                     SELECT lego_sku_id,
                           MIN(lego_sku_name_cn)                                                                       AS lego_sku_name_cn,
                           cn_line,
                           product_cn_lcs_launch_date,
                           MAX(product_rrp)                                                                            AS product_rrp
                       FROM edw.f_member_order_detail
                      WHERE is_rrp_sales_type = 1 
                      AND if_eff_order_tag IS TRUE 
                      GROUP BY 1,3,4
               ) product
        ON trans.lego_sku_id = product.lego_sku_id
     WHERE is_rrp_sales_type = 1
      AND trans.crm_member_id IS NOT NULL
      AND date_id >=  '2024-01-01'
      AND date_id < '2024-03-01';
      
      
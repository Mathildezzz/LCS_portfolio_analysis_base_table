delete from tutorial.portfolio_base_table_LCS;  -- for the subsequent update
insert into tutorial.portfolio_base_table_LCS

WITH trans AS (
    SELECT DISTINCT 
      trans.crm_member_id,
      trans.lego_sku_id,
      trans.product_rrp,
      CASE WHEN adult_sku.lego_sku_id IS NOT NULL THEN 'adult product' ELSE 'kids product' END     AS kids_vs_adult_sku
    FROM 
        edw.f_member_order_detail trans
    LEFT JOIN 
        tutorial.adult_sku_list_BM_y22_y23_y24  adult_sku
        ON trans.lego_sku_id = adult_sku.lego_sku_id
    WHERE is_rrp_sales_type = 1 
      AND if_eff_order_tag IS TRUE 
      AND trans.crm_member_id IS NOT NULL
    ),
  
member_x_product_tag AS (
    SELECT trans.crm_member_id,
         MAX(CASE WHEN kids_vs_adult_sku = 'adult product' THEN 1 ELSE 0 END) AS purchased_adults,
         MAX(CASE WHEN kids_vs_adult_sku = 'kids product' THEN 1 ELSE 0 END)  AS purchased_kids
    FROM trans
    GROUP BY 1
),


member_final_tag AS (
    SELECT member_x_product_tag.crm_member_id,
      CASE WHEN purchased_adults = 0 AND purchased_kids = 1 THEN 'only_kids'
            WHEN purchased_adults = 1 AND purchased_kids = 0 THEN 'only_adult'
            WHEN purchased_adults = 1 AND purchased_kids = 1 THEN 'both_kids_and_adult' 
            ELSE 'NA'
         END           AS kids_vs_adult_member
    FROM member_x_product_tag
)


,member_profile AS (
    SELECT mbr.member_detail_id,
      CASE WHEN mbr.gender = 1 THEN 'male' 
            WHEN mbr.gender = 2 THEN 'female'
            WHEN mbr.gender = 0 THEN 'gender_unknown' END                                                                 AS gender,
      EXTRACT(YEAR FROM CURRENT_DATE)  - EXTRACT('year' FROM DATE(mbr.birthday))                                         AS age,
      beneficiary_birthday,
      CASE WHEN beneficiary_birthday IS NULL THEN 0 ELSE 1 END                                                           AS has_birthday,
      CASE WHEN (kids_birthday.member_detail_id IS NOT NULL) OR (mbr.has_kid = 1) THEN 'has kid' ELSE 'no kid' END       AS has_kid,
      member_final_tag.kids_vs_adult_member                                                                              AS lcs_kids_vs_adult_member_shopper,
      mbr.tier_code                                                                                                      AS tier_code,
      mbr.beneficiary_type
    FROM edw.d_member_detail mbr
    LEFT JOIN (SELECT DISTINCT member_detail_id
              FROM edw.d_dl_crm_birthday_history kids_birthday
              WHERE person_type = 2
             ) kids_birthday
         ON mbr.member_detail_id::integer = kids_birthday.member_detail_id::integer
    LEFT JOIN member_final_tag
         ON mbr.member_detail_id::integer = member_final_tag.crm_member_id::integer
)

, purchase_rk AS (
    SELECT crm_member_id,
            original_order_id,
            order_paid_time,
            dense_rank () OVER (PARTITION BY crm_member_id ORDER BY order_paid_time ASC) AS rk,
            dense_rank () OVER (PARTITION BY crm_member_id, extract('year' from order_paid_time) ORDER BY order_paid_time ASC) AS ytd_rk
    FROM (
        SELECT DISTINCT trans.crm_member_id,
          trans.original_order_id,
          trans.order_paid_time
        FROM edw.f_member_order_detail trans
        WHERE is_rrp_sales_type = 1 
          AND if_eff_order_tag IS TRUE
          )
)



,parent_order as(
    select
        date_id,crm_member_id,original_order_id,sum(sales_qty) as qty_of_parent_order
    FROM 
        edw.f_member_order_detail trans
    where is_rrp_sales_type = 1
        AND trans.crm_member_id IS NOT NULL
    group by date_id,crm_member_id,original_order_id
)

,bh as(
   select
        member_detail_id
        ,birthday_status
        ,birthday
        ,create_time
        ,EXTRACT(YEAR FROM CURRENT_DATE) - EXTRACT('year' FROM DATE(birthday))  as kid_age
    from
        (select member_detail_id
            ,birthday_status
            ,birthday
            ,create_time
            ,ROW_NUMBER () OVER (
                PARTITION BY member_detail_id 
                ORDER BY 
                        CASE WHEN birthday_status = 2 THEN 0 ELSE 1 END
                        ,create_time DESC
                    ) as rk
        from edw.d_dl_crm_birthday_history  bh 
        where bh.person_type = 2
            and birthday_status <> 0 )
    where rk=1
)

SELECT
    trans.date_id                                                                                     AS transaction_date_id,
    trans.sales_solar_calendar_year                                                                   AS transaction_solar_calendar_year,
    trans.sales_solar_calendar_month                                                                  AS transaction_solar_calendar_month,
    trans.sales_lego_calendar_year                                                                    AS transaction_lego_calendar_year,
    trans.sales_lego_calendar_week                                                                    AS transaction_legor_calendar_week,
    case when trans.date_id >= '2021-12-26' and trans.date_id<='2022-02-28' then 'CNY'
        when trans.date_id >= '2022-12-26' and trans.date_id<='2023-01-31' then 'CNY'
        when trans.date_id >= '2023-12-26' and trans.date_id<='2024-02-29' then 'CNY'
        
        when trans.date_id >= '2022-05-05' and trans.date_id<='2022-06-09' then 'CD'
        when trans.date_id >= '2023-05-10' and trans.date_id<='2023-06-07' then 'CD'
        when trans.date_id >= '2024-05-10' and trans.date_id<='2024-06-07' then 'CD'
        
        when trans.date_id >= '2022-06-10' and trans.date_id<='2022-08-31' then 'SUMMER'
        when trans.date_id >= '2023-06-08' and trans.date_id<='2023-08-31' then 'SUMMER'
        when trans.date_id >= '2024-06-08' and trans.date_id<='2024-08-31' then 'SUMMER'
        
        when trans.date_id >= '2022-11-20' and trans.date_id<='2022-12-25' then 'HOLIDAY'
        when trans.date_id >= '2023-11-20' and trans.date_id<='2023-12-25' then 'HOLIDAY'
        when trans.date_id >= '2024-11-20' and trans.date_id<='2024-12-25' then 'HOLIDAY'
    else 'OTHERS'
    end                                                                                               AS transaction_marketing_campaign_calendar,
    trans.lego_store_code,
    trans.store_name,
    CASE WHEN trans.crm_member_id IS NULL THEN 'non_member' ELSE 'member' END                         AS is_member_sales, 
    trans.if_eff_order_tag,  -- 后续聚合时需要用来判断是否为有效订单
    trans.crm_member_id,
    trans.original_order_id,
    trans.lego_sku_id,
    product.lego_sku_name_cn                                                                           AS lego_sku_name_cn,
    trans.sales_qty                                                                                    AS sales_qty,--本original_order_id中指定sku的件数
    trans.order_rrp_amt                                                                                AS order_rrp_amt,   --- 在tableau聚合是要加sales qty判断逻辑
 
   --------------------------------------------------------------------------
   -- profile: 性别，年龄，城市，新老客
    CASE WHEN trans.crm_member_id IS NULL THEN 'non_member' ELSE cast(member_profile.gender as varchar) END             AS profile_gender,
    CASE WHEN trans.crm_member_id IS NULL THEN 'non_member' ELSE cast(member_profile.age  as varchar)END                AS profile_age,
    CASE WHEN trans.crm_member_id IS NULL THEN 'non_member' ELSE cast(member_profile.has_kid  as varchar)END            AS profile_has_kid,
    case 
        when bh.kid_age >=0 and bh.kid_age <=2 then '0-2'
        when bh.kid_age >=3 and bh.kid_age <=5 then '3-5'
        when bh.kid_age >=6 and bh.kid_age <=8 then '6-8'
        when bh.kid_age >=9 and bh.kid_age <=12 then '9-12'
        when bh.kid_age >13 and bh.kid_age <=17 then '13-17'
        when bh.kid_age >=18 then '18+'
        else 'unknown'
    end                                                                                                                  AS profile_kid_age_group,
    CASE WHEN trans.crm_member_id IS NULL THEN 'non_member' 
         ELSE cast(member_profile.lcs_kids_vs_adult_member_shopper  as varchar) END                                       AS profile_kids_vs_adult_member_shopper,
   
    ps.city_cn                                                                                                            AS profile_city_cn,
    ps.province_cn                                                                                                        AS profile_province_cn,--new request
     
    city_tier_list.city_tier                                                                                              AS profile_city_tier,
   
    CASE WHEN ps.city_maturity_type IS NULL THEN 'not specified' ELSE ps.city_maturity_type END                           AS profile_city_maturity_type,
    
    trans.join_date                                                                                                       AS profile_join_date,
    case 
        when member_profile.age is null then 'age_unknown'
        when member_profile.age < 18 then '<18'
        when member_profile.age >=18 and member_profile.age <=25 then '18-25'
        when member_profile.age >=26 and member_profile.age <=30 then '26-30'
        when member_profile.age >=31 and member_profile.age <=35 then '31-35'
        when member_profile.age >=36 and member_profile.age <=40 then '36-40'
        when member_profile.age >=41 and member_profile.age <=45 then '41-45'
        when member_profile.age >=46 and member_profile.age <=50 then '46-50'
        when member_profile.age >=51 and member_profile.age <=55 then '51-55'
        when member_profile.age >=56 then '56+'
        else 'else'
    end                                                                                                                   AS profile_age_group,
    case
        when member_profile.beneficiary_birthday is not null then 'Y'
        else 'N'
    end                                                                                                                   AS profile_if_has_birthday_info,
    CASE WHEN trans.crm_member_id IS NULL THEN 'non_member' 
        when member_profile.tier_code = 'VIP1' then 'VIP2'
        else member_profile.tier_code 
    END                                                                                                                   AS profile_tier_code,
    case
        WHEN trans.crm_member_id IS NULL THEN 'non_member'
        when trans.join_date > trans.product_cn_lcs_launch_date and trans.eff_reg_channel like '%LCS%' then 'new_member'
        else 'existing_member'
    end                                                                                                                    AS profile_new_vs_existing_based_on_cn_lcs_launch_date, --cn_lcs_launch_date之后注册于LCS即为new，否则为existing


   
    CASE WHEN trans.crm_member_id IS NULL THEN 'non_member'
         WHEN trans.if_lcs_new_member_solar_calendar_ytd ='纯新会员' THEN 'new_member' 
         WHEN trans.crm_member_id IS NOT NULL AND trans.if_lcs_new_member_solar_calendar_ytd !='纯新会员' THEN 'existing_member' 
    END                                                                                                                    AS profile_new_vs_existing_based_on_YTD,
    
    CASE
        WHEN EXISTS (
              SELECT 1
              FROM
                edw.f_member_order_detail last_year_order
              WHERE
                last_year_order.crm_member_id = trans.crm_member_id
                AND last_year_order.cn_line = trans.cn_line
                AND EXTRACT(YEAR FROM last_year_order.date_id) = EXTRACT(YEAR FROM trans.date_id) - 1
                AND last_year_order.is_rrp_sales_type = 1
                AND last_year_order.if_eff_order_tag IS TRUE
        ) THEN 'Y'
        ELSE 'N'
    END                                                                                                                     AS profile_if_buy_same_cnline_last_year --去年是否买过本产品线

    ,CASE
        WHEN EXISTS (
              SELECT 1
              FROM
                edw.f_member_order_detail last_year_order
              WHERE
                last_year_order.crm_member_id = trans.crm_member_id
                AND EXTRACT(YEAR FROM last_year_order.date_id) = EXTRACT(YEAR FROM trans.date_id) - 1
                AND last_year_order.is_rrp_sales_type = 1
                AND last_year_order.if_eff_order_tag IS TRUE
                and last_year_order.lego_sku_id in (select distinct lego_sku_id from tutorial.adult_sku_list_BM_y22_y23_y24 )
        ) THEN 'Y'
        ELSE 'N'
    END                                                                                                                      AS profile_if_buy_adult_sku_last_year --去年是否买过adult

    ,CASE
        WHEN EXISTS (
              SELECT 1
              FROM
                edw.f_member_order_detail last_year_order
              WHERE
                last_year_order.crm_member_id = trans.crm_member_id
                AND EXTRACT(YEAR FROM last_year_order.date_id) = EXTRACT(YEAR FROM trans.date_id) - 1
                AND last_year_order.is_rrp_sales_type = 1
                AND last_year_order.if_eff_order_tag IS TRUE
                and last_year_order.lego_sku_id not in (select distinct lego_sku_id from tutorial.adult_sku_list_BM_y22_y23_y24 )
        ) THEN 'Y'
        ELSE 'N'
    END                                                                                                                       AS profile_if_buy_kid_sku_last_year --去年是否买过kid

    ,CASE
        WHEN EXISTS (
              SELECT 1
              FROM
                edw.f_member_order_detail last_year_order
              WHERE
                last_year_order.crm_member_id = trans.crm_member_id
                AND EXTRACT(YEAR FROM last_year_order.date_id) = EXTRACT(YEAR FROM trans.date_id) - 1
                AND last_year_order.is_rrp_sales_type = 1
                AND last_year_order.if_eff_order_tag IS TRUE
                and last_year_order.cn_line in('CITY','NINJAGO','MONKIE KID','FRIENDS','DREAMZZZ','DUPLO')
        ) THEN 'Y'
        ELSE 'N'
    END                                                                                                                        AS profile_if_buy_coreline_last_year, --去年是否买过coreline
 
   -------------------------------------------------------------------------
   --- product维度 是否新品，产品rrp，产品是kids 还是adult，产品price point
 
   product.cn_line                                                                                                              AS product_cnline,
   product.product_cn_lcs_launch_date                                                                                           AS product_cn_lcs_launch_date,
   CASE WHEN product_status_list.product_status IS NOT NULL THEN product_status_list.product_status ELSE 'NOT FOUND' END        AS product_status,
   product.product_rrp                                                                                                          AS product_rrp,
   CASE WHEN product.product_rrp > 0 AND product.product_rrp < 300 THEN 'LPP'
        WHEN product.product_rrp >= 300 AND product.product_rrp < 800 THEN 'MPP'
        WHEN product.product_rrp >= 800 THEN 'HPP'
   END                                                                                                                           AS product_rrp_price_range,
   CASE WHEN adult_sku.lego_sku_id IS NOT NULL THEN 'adult product' ELSE 'kids product' END                                      AS product_kids_vs_adult_sku,
   

  -----------------------------------------------------------------------
   --- 订单维度： 是lifestage首单还是复购

   purchase_rk.rk                                                                                                                           AS order_rk,
   purchase_rk.ytd_rk                                                                                                                       AS order_rk_ytd,
   
  CASE WHEN trans.crm_member_id IS NULL THEN 'non_member'
        WHEN purchase_rk.rk = 1 THEN 'lifetime_initial'
        WHEN purchase_rk.rk >= 2 THEN 'lifetime_repurchase'
  END                                                                                                                                        AS order_type_initial_vs_repurchase_lifetime, --new request 将2024改为对应年ytd
   
  CASE WHEN trans.crm_member_id IS NULL THEN 'non_member'
        WHEN purchase_rk.ytd_rk = 1 THEN 'ytd_initial'
        WHEN purchase_rk.ytd_rk >=2 THEN 'ytd_repurchase'
  END                                                                                                                                        AS order_type_initial_vs_repurchase_ytd,
    
      
         
  CASE WHEN parent_order.qty_of_parent_order >= 2 then 'Y'
        else 'N'
  end                                                                                                                                         AS order_type_if_buy_multiple_qty_in_same_order,--本original_order_id中所有sku的件数

  CASE 
        WHEN EXISTS (     
            SELECT 1  
            FROM  edw.f_member_order_detail AS sub  
            WHERE sub.original_order_id = trans.original_order_id 
                AND sub.lego_sku_id <> trans.lego_sku_id
                AND sub.is_rrp_sales_type = 1
                AND sub.if_eff_order_tag IS TRUE                                                            
        ) THEN 'Y' 
        ELSE 'N'                                                                                        
    END                                                                                                                     AS order_type_if_buy_different_sku_in_same_order
 
    ,CASE 
        WHEN EXISTS (     
            SELECT 1  
            FROM  edw.f_member_order_detail AS sub  
            WHERE sub.original_order_id = trans.original_order_id
                AND sub.cn_line = trans.cn_line
                AND sub.lego_sku_id <> trans.lego_sku_id
                AND sub.is_rrp_sales_type = 1
                AND sub.if_eff_order_tag IS TRUE
        ) THEN 'Y'
        ELSE 'N'
    END                                                                                                                     AS order_type_if_buy_different_sku_of_same_cnline_in_same_order 
    
    ,CASE 
        WHEN EXISTS (     
            SELECT 1  
            FROM  edw.f_member_order_detail AS sub  
            WHERE sub.original_order_id = trans.original_order_id
                AND sub.cn_line <> trans.cn_line
                AND sub.is_rrp_sales_type = 1
                AND sub.if_eff_order_tag IS TRUE
        ) THEN 'Y'
        ELSE 'N'
    END                                                                                                                     AS order_type_if_buy_different_cnline_in_same_order, 
    to_char(getdate(), 'yyyymmdd')                              AS dl_batch_date,
    getdate()                                                   AS dl_load_time

FROM 
    edw.f_member_order_detail trans
left join bh 
      on trans.crm_member_id::integer = bh.member_detail_id::integer
LEFT JOIN member_profile
      ON trans.crm_member_id::integer = member_profile.member_detail_id::integer
      
LEFT JOIN tutorial.adult_sku_list_BM_y22_y23_y24            adult_sku
      ON trans.lego_sku_id = adult_sku.lego_sku_id
      
LEFT JOIN (
            SELECT DISTINCT lego_store_code, city_cn, city_en,province_cn,province_en,city_maturity.city_type AS city_maturity_type
            FROM  edw.d_dl_phy_store store
            LEFT JOIN tutorial.mkt_city_type_roy_v1 city_maturity  
                  ON city_maturity.city_chn = store.city_cn
          ) as ps 
      ON trans.lego_store_code = ps.lego_store_code
      
LEFT JOIN edw.d_dl_city_tier city_tier_list
--tutorial.mz_city_tier_v2 替换成edw.d_dl_city_tier
      ON ps.city_cn= city_tier_list.city_chn
       
LEFT JOIN purchase_rk
      ON trans.crm_member_id::integer = purchase_rk.crm_member_id::integer
      AND trans.original_order_id = purchase_rk.original_order_id
      AND trans.order_paid_time = purchase_rk.order_paid_time
LEFT JOIN (
         select lego_sku_id,lego_sku_name_cn,lego_sku_name_en ,rsp AS product_rrp,
         CASE WHEN lego_sku_id IN ('10280',
                              '10281',
                              '10289',
                              '10309',
                              '10311',
                              '10313',
                              '10314',
                              '10328',
                              '10329',
                              '10340',
                              '10368',
                              '10369',
                              '10370',
                              '40460',
                              '40461',
                              '40524',
                              '40646',
                              '40647',
                              '40725',
                              '40747') THEN top_theme ELSE cn_line END AS cn_line,    -- PID乱标
         cn_lcs_launch_date as product_cn_lcs_launch_date 
         FROM edw.d_dl_product_info_latest
          ) product
    ON trans.lego_sku_id = product.lego_sku_id

left join parent_order 
    on trans.original_order_id=parent_order.original_order_id 
    and trans.date_id=parent_order.date_id
    
left join tutorial.all_assortment_status_removed_duplicates  product_status_list
    on trans.lego_sku_id =product_status_list.lego_sku_id
    
WHERE trans.is_rrp_sales_type = 1
AND trans.distributor_name <> 'LBR';
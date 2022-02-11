select 
    distinct
    za.accountid as zuoraAccountID
    , zs.subscriptionid
    , zrp.name as promo
    , zs.subscriptionstartdate as promoStartDate
    , rpc.effectivestartdate
    , rpc.effectiveenddate
    , rpct.discountpercentage
    , amendmenttype
    , case
        when promo like 'First Month Free%'  then add_months(promoStartDate, 1)
        when promo like 'Three Months Free' then add_months(promoStartDate, 3)
        when promo like '12 Month Discount' or promo like '100% Annual Discount' then add_months(promoStartDate, 12)
        ELSE null
        end as promoEndDate
    , case when datediff(day, getdate(), promoEndDate) <= 0 then 0
        else  datediff(day, getdate(), promoEndDate) 
        end as daysleftonpromo
    , case when promoEndDate is null then 'Forever' 
        else 'Limited Time' 
        end as promotype
from
    --dbt_vidyard_master.stg_zuora_subscription as zs
    {{ ref('stg_zuora_subscription') }} as zs
    --join dbt_vidyard_master.stg_zuora_account as za on zs.accountid = za.accountid
    join {{ ref('stg_zuora_account') }} as za on zs.accountid = za.accountid
    --join dbt_vidyard_master.stg_zuora_rate_plan as zrp on zs.subscriptionid = zrp.subscriptionid
    join {{ ref('stg_zuora_rate_plan') }} as zrp on zs.subscriptionid = zrp.subscriptionid
    --join dbt_vidyard_master.stg_zuora_product_rate_plan as zprp on zrp.productrateplanid = zprp.productrateplanid
    join {{ ref('stg_zuora_product_rate_plan') }} as zprp on zrp.productrateplanid = zprp.productrateplanid
    --join dbt_vidyard_master.stg_zuora_product as zp on zprp.productid = zp.productid
    join {{ ref('stg_zuora_product') }} as zp on zprp.productid = zp.productid
    --join dbt_vidyard_master.stg_zuora_rate_plan_charge rpc on zrp.rateplanid = rpc.rateplanid
    join {{ ref('stg_zuora_rate_plan_charge') }} as rpc on zrp.rateplanid = rpc.rateplanid
    --join dbt_vidyard_master.stg_zuora_rate_plan_charge_tier rpct on rpct.rateplanchargeid = rpc.rateplanchargeid
    join {{ ref('stg_zuora_rate_plan_charge_tier') }} as rpct on rpct.rateplanchargeid = rpc.rateplanchargeid
where
    zp.sku in ('SKU-00000009','SKU-00000020','SS-010')
    and  discountpercentage is not null
    and (amendmenttype is null or amendmenttype != 'RemoveProduct')
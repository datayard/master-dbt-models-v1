select 
    distinct
    zs.accountid
    , zs.subscriptionId
    , zs.contractEffectiveDate as freetrialstart
    , zs.serviceActivationDate as freetrialend
    , zs.serviceActivationDate - zs.contractEffectiveDate as initialtrial
    , zs.status
    , case
        when zs.serviceActivationDate - cast(getdate() as date) < 0 then 0
        else zs.serviceActivationDate - cast(getdate() as date)
        end as daysleftontrial
from
    --dbt_vidyard_master.stg_zuora_rate_plan as zrp
    {{ ref('stg_zuora_rate_plan') }} as zrp
    --join dbt_vidyard_master.stg_zuora_subscription as zs on zrp.subscriptionid = zs.subscriptionId
    join {{ ref('stg_zuora_subscription') }} as zs on zrp.subscriptionid = zs.subscriptionId
    --join dbt_vidyard_master.stg_zuora_product as zp on zrp.productId = zp.productId
    join {{ ref('stg_zuora_product') }} as zp on zrp.productId = zp.productId
where
    zs.contractEffectiveDate < zs.serviceActivationDate
    AND zp.sku in ( 'SS-010', 'SKU-00000009','SKU-00000020')
    SELECT
        zs.name as subscriptionName,
        zs.subscriptionid,
        zs.accountid,
        zrp.createddate,
        zrp.name as ratePlanName,
        zp.name as productName,
        zp.sku
    FROM
        --dbt_vidyard_master.stg_zuora_subscription as zs
        {{ ref('stg_zuora_subscription') }} as zs
    JOIN
        --dbt_vidyard_master.stg_zuora_rate_plan as zrp
        {{ ref('stg_zuora_rate_plan') }} as zrp
    ON
        zs.subscriptionid = zrp.subscriptionid
    JOIN
        --dbt_vidyard_master.stg_zuora_product_rate_plan as zprp
        {{ ref('stg_zuora_product_rate_plan') }} as zprp
    ON
        zrp.productrateplanid = zprp.productrateplanid
    JOIN
        --dbt_vidyard_master.stg_zuora_product as zp
        {{ ref('stg_zuora_product') }} as zp
    ON
        zprp.productid = zp.productid
    WHERE
        zp.sku LIKE 'SS-010'
        AND (
                zrp.productrateplanid LIKE '2c92a0107a187273017a1a86987d3717' --First Month Free
            OR zrp.productrateplanid LIKE '2c92a0fe7ac84f2c017ae3ed410a3631' --Three Months Free
        )
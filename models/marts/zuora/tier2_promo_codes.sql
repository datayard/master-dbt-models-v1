SELECT
    DISTINCT za.accountid as zuoraAccountID,
    zs.subscriptionid,
    za.accountname as accountName,
    vypa.email,
    vypa.userid,
    vypa.organizationid,
    zrp.name as promo,
    MIN (zs.createddate) as promoStartDate
    /*CASE WHEN zrp.name like 'First Month Free' THEN DATE_PART(month,zs.createddate) + 1
        else getdate()
    END AS promoEndDate*/
FROM
    --dbt_vidyard_master.stg_zuora_subscription as zs
    {{ ref('stg_zuora_subscription') }} as zs
JOIN
    --dbt_vidyard_master.stg_zuora_account as za
    {{ ref('stg_zuora_account') }} as za
ON
    zs.accountid = za.accountid
JOIN
    --dbt_vidyard_master.kube_personal_account_attributes as vypa
    {{ ref('kube_personal_account_attributes') }} as vypa
ON
    za.vidyardaccountid= vypa.organizationid
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
GROUP BY
    1,
    2,
    3,
    4,
    5,
    6,
    7
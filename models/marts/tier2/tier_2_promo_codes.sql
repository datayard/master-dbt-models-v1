
WITH PROMO_DATA AS (

SELECT
    DISTINCT za.accountid as zuoraAccountID,
    zs.subscriptionid,
    za.accountname as accountName,
    vypa.email,
    vypa.userid,
    vypa.organizationid,
    zrp.name as promo,
    MIN (zs.subscriptionstartdate) as promoStartDate
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
        or zrp.productrateplanid like '2c92a00f7c3056f9017c31f6135f64c2' --First Month Free - Advisor Group
        or zrp.productrateplanid like '2c92a00f7420c37601742288406615ab' --12 month discount
        or zrp.productrateplanid like '2c92a0117ac85d21017ace8398e30b63' -- 100% Annual Discount
    )
GROUP BY
    1,
    2,
    3,
    4,
    5,
    6,
    7

)
    SELECT
           *
        , CASE
            WHEN promo like 'First Month Free' THEN add_months(promoStartDate, 1)
            WHEN promo like 'Three Months Free' THEN add_months(promoStartDate, 3)
            WHEN promo like '12 Month Discount' or promo like '100% Annual Discount' then add_months(promoStartDate, 12)
            else getdate()
          END AS promoEndDate
        , datediff(day, getdate(), promoEndDate) as days_remaining
    FROM PROMO_DATA


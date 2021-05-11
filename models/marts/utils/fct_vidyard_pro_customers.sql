SELECT
    COUNT (DISTINCT subscription.subscriptionId) as activeProSubscriptions,
    subscription.status
FROM
    {{ ref('stg_zuora_account') }} as account
JOIN
    {{ ref('stg_zuora_subscription') }} as subscription
ON
    subscription.accountId = account.accountId
JOIN
    {{ ref('stg_zuora_rate_plan') }} as rateplan
ON
    subscription.subscriptionId = rateplan.subscriptionId
JOIN
    {{ ref('stg_zuora_product_rate_plan') }} as productrateplan
ON
    rateplan.productRatePlanId = productrateplan.productRatePlanId
JOIN
    {{ ref('stg_zuora_product') }} as product
ON
    productrateplan.productID = product.productId
WHERE
    product.sku LIKE 'SS-010'
GROUP BY
    subscription.status

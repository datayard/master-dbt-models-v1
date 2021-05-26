SELECT 
	account.accountID AS activeProaccounts,
	account.accountname,
	subscription.status AS subscriptionstatus,
	account.accountStatus,
	productrateplan.name,
	subscription.termstartdate,
	subscription.termenddate
FROM
	{{ ref('stg_zuora_account') }} as account
	JOIN {{ ref('stg_zuora_subscription') }} AS subscription ON subscription.accountId = account.accountId
	JOIN {{ ref('stg_zuora_rate_plan') }} AS rateplan ON subscription.subscriptionId = rateplan.subscriptionId
	JOIN {{ ref('stg_zuora_product_rate_plan') }} AS productrateplan ON rateplan.productRatePlanId = productrateplan.productRatePlanId
	JOIN {{ ref('stg_zuora_product') }} AS product ON productrateplan.productID = product.productId
WHERE
	product.sku LIKE 'SS-010'
	AND subscription.status LIKE 'Active'
	OR (
		subscription.status LIKE 'Cancelled' 
		AND subscription.termenddate >= getdate())
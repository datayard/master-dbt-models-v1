SELECT
    subscription.subscriptionId,
    account.accountId
FROM
    {{ ref('stg_zuora_subscription') }} as subscription
JOIN
    {{ ref('stg_zuora_account') }} as account
ON
    subscription.accountId = account.accountId
WHERE
    true
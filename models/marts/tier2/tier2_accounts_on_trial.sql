WITH accounts_on_trial as (
    SELECT MAX(zuorasub.subscriptionversion),
           zuorasub.accountid,
           zuorasub.contractEffectiveDate,
           zuorasub.serviceActivationDate,
           zuorasub.serviceActivationDate - zuorasub.contractEffectiveDate as initialtrial,
           zuorasub.status,
           CASE
               WHEN
                   zuorasub.serviceActivationDate - cast(getdate() as date) < 0 then 0
               else zuorasub.serviceActivationDate - cast(getdate() as date)
               end                                                             as daysleftontrial
    FROM --zuora.rate_plan as zuora_rtplan
            {{ ref('stg_zuora_rate_plan')}} as zuora_rtplan
             JOIN
         --zuora.subscription as zuorasub
         {{ ref('stg_zuora_subscription')}} as zuorasub
         ON
             zuora_rtplan.subscriptionid = zuorasub.subscriptionId
             JOIN
         --zuora.account as zuoraa
         {{ ref('stg_zuora_account')}} as zuoraa
         ON
             zuorasub.accountId = zuoraa.accountid
             JOIN
         --zuora.product as zuorap
         {{ ref('stg_zuora_product')}} as zuorap
         ON
             zuora_rtplan.productId = zuorap.productId
    WHERE zuorasub.contractEffectiveDate < zuorasub.serviceActivationDate
      AND zuorap.sku = 'SS-010'
    GROUP BY zuorasub.accountId,
             zuorasub.contractEffectiveDate,
             zuorasub.serviceActivationDate,
             zuorasub.status
)

SELECT
    *
FROM
    accounts_on_trial

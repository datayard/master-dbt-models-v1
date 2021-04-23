SELECT
    sfdc_account.accountid as accountId,
    listagg (distinct sfdc_opp.usecase,';') as useCase
FROM
    {{ ref('stg_salesforce_opportunity') }} as sfdc_opp
JOIN
    {{ ref('stg_salesforce_account') }} as sfdc_account
ON
    sfdc_opp.accountid = sfdc_account.accountid
WHERE
  sfdc_opp.usecase is not null
  and sfdc_opp.stagename LIKE '7 - Closed Won'
GROUP BY
  sfdc_opp.accountid
  

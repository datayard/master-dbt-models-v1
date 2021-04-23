SELECT
    sfdc_account.accountid,
    listagg (distinct sfdc_opp.vy_use_case__c,';')
FROM
    {{ ref('stg_salesforce_opportunity') }} as sfdc_opp
JOIN
    {{ ref('stg_salesforce_account') }} as sfdc_account
ON
    sfdc_opp.accountid = sfdc_account.accountid
WHERE
  sfdc_opp.vy_use_case__c is not null
  and sfdc_opp.stagename LIKE '7 - Closed Won'
GROUP BY
  sfdc_opp.accountid
  

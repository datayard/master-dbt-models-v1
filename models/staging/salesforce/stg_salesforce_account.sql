SELECT
    sfdc_account.id,
    sfdc_account.name
FROM
    salesforce_production.account as sfdc_account
WHERE
    sfdc_account.type LIKE 'Customer'
    AND sfdc_account.ispersonaccount = false
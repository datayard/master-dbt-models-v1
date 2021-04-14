SELECT
    sfdc_account.id as accountId,
    sfdc_account.name as accountName,
    sfdc_account.isdeleted as isDeleted,
    sfdc_account.type as accountType
FROM
    salesforce_production.account as sfdc_account
WHERE
    sfdc_account.type LIKE 'Customer'
    AND sfdc_account.ispersonaccount = false
limit 1000000
SELECT 
        usr.id as userId
        , usr.full_name__c as fullName
FROM 
    {{ source('salesforce_production', 'user') }} as usr
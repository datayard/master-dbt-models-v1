SELECT 
        usr.id as userid
        , usr.full_name__c as fullname
FROM 
    {{ source('salesforce_production', 'user') }} as usr
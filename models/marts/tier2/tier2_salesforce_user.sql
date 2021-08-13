SELECT 
    userid
    , fullname
FROM 
    {{ ref('stg_salesforce_user') }}
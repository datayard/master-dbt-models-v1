SELECT 
    partnerprogramid
    , partnerprogramname
    , partneraccount
    , opportunityid
    , partnertype
    , partnermanager
    , partnerecosystem
    , proportionalarr
    , partnersourced
FROM 
    {{ ref('stg_salesforce_partnerprogram') }}
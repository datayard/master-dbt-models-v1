SELECT 
    id as partnerprogramid
    , name as partnerprogramname
    , partner_account__c as partneraccount
    , opportunity__c as opportunityid
    , partner_type__c as partnertype
    , partner_manager__c as partnermanager
    , partner_eco_system__c as partnerecosystem
    , proportional_arr__c as proportionalarr
    , partner_sourced__c as partnersourced
FROM 
    {{ source('salesforce_production', 'partner_program__c') }} as sfdc_partner
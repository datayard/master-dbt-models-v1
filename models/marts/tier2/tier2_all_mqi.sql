SELECT u.leadid,
       u.email,
       u.domaintype,
       cm.createddate,
       cm.createddate as mqi_date,
       cm.mql_date_and_time__c as mql_date,
       cm.mql__c,
       cm.sal__c,
       cm.sal_date_and_time__c as sal_date,
       cm.sql__c,
       cm.sql_date_and_time__c as sql_date,
       cm.sqo__c,
       cm.sqo_date__c as sqo_date,
       cm.won__c,
       cm.opportunity_closed_won_date__c as won_date,
       cm.campaign_sourced_by__c,
       cm.id as parent_campaign_id,
       cm.cta_subtype__c
FROM {{ ref('tier2_salesforce_lead') }} as u
JOIN {{ ref('tier2_salesforce_campaign_and_members') }} as cm
    ON cm.leadid = u.leadid
LEFT JOIN {{ ref('stg_salesforce_campaign')}} as c
    ON c.campaignId = cm.campaign_parentid


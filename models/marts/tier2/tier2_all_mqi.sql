SELECT
        u.leadid
      , u.email
      , u.domainType
      , cm.createdDate as mqi_date
      , cm.mqlDate as mql_date
      , cm.mql
      , cm.sal
      , cm.salDate
      , cm.sql
      , cm.sqlDate
      , cm.sqo
      , cm.sqoDate
      , cm.won
      , cm.opportunityClosedWonDate
      , cm.campaignSourcedBy
      , c.campaignid as parentCampaignId
      , coalesce(c.campaignName, cm.campaign_name) as campaign
      , coalesce(c.cta, cm.campaign_cta) as cta_type
      , coalesce (c.channelPicklist, cm.campaign_channelpicklist) as channel
      , c.campaign_ctasubtype
FROM {{ ref('tier2_salesforce_lead') }} as u
JOIN {{ ref('tier2_salesforce_campaign_and_members') }} as cm
    ON cm.leadid = u.leadid
LEFT JOIN {{ ref('stg_salesforce_campaign')}} as c
    ON c.campaignId = cm.campaign_parentid
WHERE
      u.isconverted = 'false' -- need to ignore leads that were converted to contact
UNION
SELECT
        u.contactId
      , u.email
      , u.domainType
      , cm.createdDate as mqi_date
      , cm.mqlDate as mql_date
      , cm.mql
      , cm.sal
      , cm.salDate
      , cm.sql
      , cm.sqlDate
      , cm.sqo
      , cm.sqoDate
      , cm.won
      , cm.opportunityClosedWonDate
      , cm.campaign_sourcedby
      , c.campaignid as parentCampaignId
      , coalesce(c.campaignName, cm.campaign_name) as campaign
      , coalesce(c.cta, cm.campaign_cta) as cta_type
      , coalesce (c.channelPicklist, cm.campaign_channelpicklist) as channel
      , c.ctaSubtype
FROM {{ ref('tier2_salesforce_contact') }} as u
JOIN {{ ref('tier2_salesforce_campaign_and_members') }} as cm
    ON cm.contactId = u.contactId
LEFT JOIN {{ ref('stg_salesforce_campaign') }} as c
    ON c.campaignId = cm.campaign_parentid
JOIN {{ ref('tier2_salesforce_account') }} as a
    ON a.accountId = u.accountId
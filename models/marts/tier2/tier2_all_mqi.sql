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
      , coalesce(c.campaignName, cm.campaign_name) as parent_campaign
      , coalesce(c.cta, cm.campaign_cta) as parent_cta_type
      , coalesce (c.channelPicklist, cm.campaign_channelpicklist) as parent_channel
      , c.ctasubtype as parent_ctasubtype
      , coalesce(cm.campaign_name, c.campaignName) as child_campaign
      , coalesce(cm.campaign_cta, c.cta) as child_cta_type
      , coalesce (cm.campaign_channelpicklist, c.channelpicklist) as child_channel
      , cm.campaign_ctasubtype as child_ctasubtype

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
      , coalesce(c.campaignName, cm.campaign_name) as parentCampaign
      , coalesce(c.cta, cm.campaign_cta) as parentCTAtype
      , coalesce (c.channelPicklist, cm.campaign_channelpicklist) as parentChannel
      , c.ctasubtype as parentCTAsubtype
      , coalesce(cm.campaign_name, c.campaignName) as childCampaign
      , coalesce(cm.campaign_cta, c.cta) as childCTAtype
      , coalesce (cm.campaign_channelpicklist, c.channelpicklist) as childChannel
      , cm.campaign_ctasubtype as childCTAsubtype

FROM {{ ref('tier2_salesforce_contact') }} as u
JOIN {{ ref('tier2_salesforce_campaign_and_members') }} as cm
    ON cm.contactId = u.contactId
LEFT JOIN {{ ref('stg_salesforce_campaign') }} as c
    ON c.campaignId = cm.campaign_parentid
JOIN {{ ref('tier2_salesforce_account') }} as a
    ON a.accountId = u.accountId
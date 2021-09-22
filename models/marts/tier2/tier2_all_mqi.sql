SELECT
        u.leadid as id
      , u.email
      , u.domainType
      , case
   		when u.email like '%vidyard.com' then 1
        when u.email like '%viewedit.com' then 1
   		else 0
      end as excludeEmail
      , split_part(u.email, '@', 2) as domain
      , u.persona
      , u.accountId
      , cm.createdDate as mqi_date
      , cm.mqlDateGMT
      , cm.mqlDateEST
      , cm.mql
      , cm.sal
      , cm.salDateGMT
      , cm.salDateEST
      , cm.sql
      , cm.sqlDateGMT
      , cm.sqlDateEST
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
      , cm.responseStatus
FROM {{ ref('tier2_salesforce_lead') }} as u
JOIN {{ ref('tier2_salesforce_campaign_and_members') }} as cm
    ON cm.leadid = u.leadid
LEFT JOIN {{ ref('stg_salesforce_campaign')}} as c
    ON c.campaignId = cm.campaign_parentid
WHERE
      u.isconverted = 'false' -- need to ignore leads that were converted to contact
UNION
SELECT
        u.contactId as id
      , u.email
      , u.domainType
      , case
   		when u.email like '%vidyard.com' then 1
        when u.email like '%viewedit.com' then 1
   		else 0
      end as excludeEmail
      , split_part(u.email, '@', 2) as domain
      , u.persona
      , u.accountId
      , cm.createdDate as mqi_date
      , cm.mqlDateGMT
      , cm.mqlDateEST
      , cm.mql
      , cm.sal
      , cm.salDateGMT
      , cm.salDateEST
      , cm.sql
      , cm.sqlDateGMT
      , cm.sqlDateEST
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
      , cm.responseStatus


FROM {{ ref('tier2_salesforce_contact') }} as u
JOIN {{ ref('tier2_salesforce_campaign_and_members') }} as cm
    ON cm.contactId = u.contactId
LEFT JOIN {{ ref('stg_salesforce_campaign') }} as c
    ON c.campaignId = cm.campaign_parentid
JOIN {{ ref('tier2_salesforce_account') }} as a
    ON a.accountId = u.accountId

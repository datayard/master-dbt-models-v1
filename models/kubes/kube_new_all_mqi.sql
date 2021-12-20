
    SELECT
        u.leadid as id
      , u.email
      , u.domainType
      , u.domain
      , u.persona
      , u.accountId
      , cm.createdDateGMT as mqiDateGMT
      , cm.createdDateEST as mqiDateEST
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
      , cm.campaign_sourcedby --campaign_sourcedby is to be used for marketing funnnel after NNN
      , cm.campaign_sourcedby as acquisition_source --acquisition_source is intended to use only for NNN data pull
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
      , cm.campaign_parentid
      , cm.status as campaign_member_status
FROM {{ ref('tier2_salesforce_lead') }} as u
JOIN {{ ref('tier2_salesforce_campaign_and_members') }} as cm
    ON cm.leadid = u.leadid
LEFT JOIN {{ ref('stg_salesforce_campaign') }} as c
    ON c.campaignId = cm.campaign_parentid
WHERE
      u.isconverted = 'false' -- need to ignore leads that were converted to contact
    and (parentCampaign not ilike '%Hubspot%' or parentCampaign is null)
    and (childCampaign not ilike '%Hubspot%' or childCampaign is null)
    and excludeemail = 0
    and parentCampaign!='Sales Sourced - ZoomInfo'
UNION
SELECT
        u.contactId as id
      , u.email
      , u.domainType
      , u.domain
      , u.persona
      , u.accountId
      , cm.createdDateGMT as mqiDateGMT
      , cm.createdDateEST as mqiDateEST
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
      , cm.campaign_sourcedby --campaign_sourcedby is to be used for marketing funnnel after NNN
      , cm.campaign_sourcedby as acquisition_source --acquisition_source is intended to use only for NNN data pull
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
      , cm.campaign_parentid
      , cm. status as campaign_member_status

FROM {{ ref('tier2_salesforce_contact') }} as u
JOIN {{ ref('tier2_salesforce_campaign_and_members') }} as cm
    ON cm.contactId = u.contactId
LEFT JOIN {{ref ('stg_salesforce_campaign')}} as c
    ON c.campaignId = cm.campaign_parentid
where (parentCampaign not ilike '%Hubspot%' or parentCampaign is null)
 and (childCampaign not ilike '%Hubspot%' or childCampaign is null)
 and excludeemail = 0
 and parentCampaign!='Sales Sourced - ZoomInfo'

UNION
SELECT
          null as id
        , u.email
        , u.domaintype
        , u.domain
        , 'Signups - Product' as persona
        , a.accountId as accountId
        , u.createddate as mqiDateGMT
        , null::timestamp as mqiDateEST
        , null::timestamp as mqlDateGMT
        , null::timestamp as mqlDateEST
        , null::boolean as mql
        , null::boolean as sal
        , null::timestamp as salDateGMT
        , null::timestamp as salDateEST
        , null::boolean as sql
        , null::timestamp as sqlDateGMT
        , null::timestamp as sqlDateEST
        , null::boolean as sqo
        , null::timestamp as sqoDate
        , null::boolean as won
        , null::timestamp as opportunityClosedWonDate
        , 'Signups - Product' as campaign_sourcedby --campaign_sourcedby is to be used for marketing funnnel after NNN
        ,  case
                when createdbyclientid = 'Enterprise' then 'Assigned Paid Seat'
                else 'Signups - Product'
          end as acquisition_source   --acquisition_source is intended to use only for NNN data pull
        , 'Signups - Product' as campaignid
        , 'Signups - Product' as parentCampaign
        , 'Signups - Product' as parentCTAtype
        , 'Signups - Product' as parentChannel
        , 'Signups - Product' as parentCTAsubtype
        , 'Signups - Product' as childCampaign
        , 'Signups - Product' as childCTAtype
        , 'Signups - Product' as childChannel
        , 'Signups - Product' as childCTAsubtype
        ,  null as responseStatus
        ,  'Signups - Product' as campaign_parentid
        ,  'Signups - Product' as campaign_member_status

    from {{ ref('kube_vidyard_user_details') }} u
    left join {{ ref('tier2_salesforce_account') }} a
          on a.emaildomain=u.domain
    where excludeemail = 0
      and signupsource != 'Hubspot'



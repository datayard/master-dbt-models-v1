
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
      , case
        when cm.campaignmemberid is not null and u.createddate > cm.createddategmt
            then cm.createddategmt
        else u.createddate end as mqi_date_new
      , case
        when u.leadsource in ('Intercom', 'Product') and u.acquisitationprogram is null
            then 'Product'
        when u.acquisitationprogram is not null then 'Marketing'
        when (u.leadsource not in ('Intercom', 'Product') or u.leadsource is null) and
              u.acquisitationprogram is null and campaignmemberid is not null and
              cm.campaignsourcedby is not null then cm.campaignsourcedby
        when (u.leadsource not in ('Intercom', 'Product') or u.leadsource is null) and
              u.acquisitationprogram is null and campaignmemberid is not null and
              cm.campaignsourcedby is null then 'other'
        else 'exclude'
        end as acquisition_source1

      , case
        when cm.leadid = u.leadid and cm.campaignsourcedby is null and cm.campaignmemberid is not null
            then 'other'
        else cm.campaignsourcedby end as acquisition_source2

      , case
        when cm.leadid = u.leadid and u.createddate > cm.createddategmt then acquisition_source2
        when cm.leadid = u.leadid and u.createddate <= cm.createddategmt and
              cm.campaignmemberid is not null
            and cm.campaignsourcedby is not null
            then cm.campaignsourcedby
        else acquisition_source1 end as acquisition_source

FROM {{ ref('tier2_salesforce_lead') }} as u
LEFT JOIN {{ ref('tier2_salesforce_campaign_and_members') }} as cm
    ON cm.leadid = u.leadid
LEFT JOIN {{ ref('stg_salesforce_campaign')}} as c
    ON c.campaignId = cm.campaign_parentid
WHERE
      u.isconverted = 'false' -- need to ignore leads that were converted to contact
      and (campaign_parentid NOT IN ('7014O000001m7h7QAA', '7014O000001m4XEQAY') or campaign_parentid is null)
      and acquisition_source != 'exclude'

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
      , case
                when cm.campaignmemberid is not null and u.createddate > cm.createddategmt
                then cm.createddategmt
                else u.createddate end as mqi_date_new
      , case
                when u.leadsource in ('Intercom', 'Product') and u.acquisitationprogram is null then 'Product'
                when u.acquisitationprogram is not null then 'Marketing'
                when (u.leadsource not in ('Intercom', 'Product') or u.leadsource is null) and
                u.acquisitationprogram is null and campaignmemberid is not null and
                cm.campaignsourcedby is not null then cm.campaignsourcedby
                when (u.leadsource not in ('Intercom', 'Product') or u.leadsource is null) and
                u.acquisitationprogram is null and campaignmemberid is not null and
                cm.campaignsourcedby is null then 'other'
                else 'exclude'
        end as acquisition_source1

      , case
                when cm.leadid = u.contactid and cm.campaignsourcedby is null and cm.campaignmemberid is not null
                then 'other'
        else cm.campaignsourcedby end as acquisition_source2

      , case
                when cm.leadid = u.contactid and u.createddate > cm.createddategmt then acquisition_source2
                when cm.leadid = u.contactid and u.createddate <= cm.createddategmt and
                cm.campaignmemberid is not null
                and cm.campaignsourcedby is not null
                then cm.campaignsourcedby
        else acquisition_source1 end as acquisition_source


FROM {{ ref('tier2_salesforce_contact') }} as u
LEFT JOIN {{ ref('tier2_salesforce_campaign_and_members') }} as cm
    ON cm.contactId = u.contactId
LEFT JOIN {{ ref('stg_salesforce_campaign') }} as c
    ON c.campaignId = cm.campaign_parentid
-- JOIN {{ ref('tier2_salesforce_account') }} as a
--     ON a.accountId = u.accountId
where acquisition_source != 'exclude'
and (campaign_parentid NOT IN ('7014O000001m7h7QAA', '7014O000001m4XEQAY') or campaign_parentid is null)

UNION
SELECT 
          null as id
        , email
        , domaintype
        , case
   		when u.email like '%vidyard.com' or u.email like '%viewedit.com' then 1
   		else 0
          end as excludeEmail
        , split_part(u.email, '@', 2) as domain
        , null as persona
        , null as accountId
        , null::timestamp as mqiDateGMT
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
        , null as campaign_sourcedby
        , null as campaignid
        , null as parentCampaign
        , null as parentCTAtype
        , null as parentChannel
        , null as parentCTAsubtype
        , null as childCampaign
        , null as childCTAtype
        , null as childChannel
        , null as childCTAsubtype
        , null as responseStatus
        , u.createddate as mqi_date_new
        , null as acquisition_source1
        , null as acquisition_source2
        , case
                when createdbyclientid = 'Enterprise' then 'Assigned Paid Seat'
                else 'Signups - Product' 
          end as acquisition_source 

    from dbt_vidyard_master.kube_vidyard_user_details u
    where excludeemail = 0
      and signupsource != 'Hubspot'


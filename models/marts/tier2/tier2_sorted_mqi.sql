SELECT 
        am.email
      , am.accountId
      , am.id
      , am.domainType
      , am.domain
      , am.excludeemail
      , am.mqiDateGMT
      , am.mqiDateEST
      , am.mqlDateGMT
      , am.mqlDateEST
      , am.mql
      , am.sal
      , am.salDateGMT
      , am.salDateEST
      , am.sql
      , am.sqlDateGMT
      , am.sqlDateEST
      , am.sqo
      , am.sqoDate
      , am.won
      , am.opportunityClosedWonDate
      , am.campaign_sourcedby
      , am.parentcampaignid
      , am.parentCampaign
      , am.parentctatype
      , am.parentchannel
      , am.parentCTAsubtype
      , am.childCampaign
      , am.childCTAtype
      , am.childChannel
      , am.childCTAsubtype
     /* , am.employeeSegment */
      , am.responseStatus
      , row_number() over(partition by am.email order by am.mqi_date) as rn

      , case when am.persona like '%Decision Maker' then 'Decision Maker' 
             when (am.persona like '%Influencer' or am.persona like '%General') then 'Individual Contributor' 
             else 'Other' end as persona
          
      , case when am.parentCTAtype is null then 'Blank' 
             when (parentCTAtype = 'Content Asset' or parentCTAtype =  'Video') then 'Content/Video' 
             when (am.parentCTAtype='Tradeshow' or am.parentCTAtype='Webinar') then 'Event/Webinar' 
             when (am.parentCTAtype='Organic Lead Score' or am.parentCTAtype='Other') then 'Other' 
             else am.parentCTAtype end as parentCTAtype2

      , case when am.childCTAtype is null then 'Blank' 
             when (childCTAtype = 'Content Asset' or childCTAtype =  'Video') then 'Content/Video' 
             when (am.childCTAtype='Tradeshow' or am.childCTAtype='Webinar') then 'Event/Webinar' 
             when (am.childCTAtype='Organic Lead Score' or am.childCTAtype='Other') then 'Other' 
             else am.childCTAtype end as childCTAtype2

      , case when am.parentCTAsubtype = 'User Signup' then row_number() over(partition by am.email order by am.mqi_date) end as user_rn
     
      , case when a.employeeSegment is null then 'UNKNOWN' else a.employeeSegment end as employee_segment
      
      , case when a.accountType = 'Prospect' then a.accountType 
             when a.accountType = 'Customer' then a.accountType 
             when a.accountType = 'Sub-Account' then a.accountType 
             else 'Other' end as type 
      
FROM {{ ref('tier2_all_mqi') }} as am
LEFT JOIN {{ ref('tier2_salesforce_account') }} as a
    ON am.accountId = a.accountId
/* LEFT JOIN {{ ref('stg_salesforce_account') }} as dt
         on dt.emailDomain = split_part(am.email, '@', 2) 
*/
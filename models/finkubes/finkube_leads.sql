with all_mqi as (
    select
      leadid as leadcontactid
      , u.email
      , split_part(u.email, '@', 2) as domain
      , u.domaintype as domaintype
      , case
        when c.campaignsourcedby = 'Sales'
          then 'Sales'
        else 'Other'
      end as campaignsourcecategory
      , c.campaignsourcedby as campaignsource
      , date(c.createdDateest) as mqidate
      , c.campaign_cta as ctatype
      , c.campaign_ctasubtype as ctasubtype
      , date(c.sqodate) as sqodate
      , case
        when sqodate is not null
        and c.sqldateest is null
          then sqodate
        else date(c.sqldateest)
      end as sqldate
      , case
        when sqldate is not null
        and c.saldateest is null
          then sqldate
        else date(c.saldateest)
      end as saldate
      , case
        when saldate is not null
        and c.mqldateest is null
          then saldate
        else date(c.mqldateest)
      end as mqldate
      , '' as region
      , '' as country
       , case when numberofemployees >= 200 then 'Commercial' when numberofemployees < 200 then 'Emerging' else '' end as customertype
    from
      {{ ref('tier2_salesforce_lead') }} u
      join
      {{ ref('tier2_salesforce_campaign_and_members')}} c using (leadid)
    where
      u.isconverted = 'false'

    union all

    select
      contactid as leadcontactid
      , u.email
      , split_part(u.email, '@', 2) as domain
      , u.domaintype
      , case
        when c.campaignsourcedby = 'Sales'
          then c.campaignsourcedby
        else 'Other'
      end as campaignsourcecategory
      , c.campaignsourcedby as campaignsource
      , date(c.createdDateest) as mqi_date
      , c.campaign_cta as ctatype
      , c.campaign_ctasubtype as ctasubtype
      , date(c.sqodate) as sqodate
      , case
        when sqodate is not null
        and c.sqldateest is null
          then sqodate
        else date(c.sqldateest)
      end as sqldate
      , case
        when sqldate is not null
        and c.saldateest is null
          then sqldate
        else date(c.saldateest)
      end as saldate
      , case
        when saldate is not null
        and c.mqldateest is null
          then saldate
        else date(c.mqldateest)
      end as mqldate
        , r.region
        , u.mailingcountry as country
         , nvl(case
        when a.ispersonaccount is true
          then 'Vidyard Pro'
        when a.ispersonaccount is false
        and a.isselfserve is true
          then 'HubSpot Self Serve'
        when a.employeesegment = 'UNKNOWN'
          then 'Emerging'
        else a.employeesegment
      end,'') as customertype
    from
      {{ ref('tier2_salesforce_contact') }} u
      join {{ref('tier2_salesforce_campaign_and_members')}} c using (contactid)
      join {{ref('tier2_salesforce_account')}} a using (accountid)
      join {{ref('tier2_salesforce_opportunity')}} using (accountid)
      left join {{ref('fct_sfdc_country_to_region')}} r on r.country = lower(u.mailingcountry)
  )

  , sorted_mqi as (
    select
      leadcontactid
      , email
      , domain
      , domaintype
      , campaignsourcecategory
      , campaignsource
      , ctatype
      , ctasubtype
      , mqidate
      , mqldate
      , saldate
      , sqldate
      , sqodate
      , region
      , country
      , customertype
      , case
        when ctasubtype = 'User Signup'
          then row_number() over(partition by email order by mqidate)
      end as rn
    from
      all_mqi
    where
      domain is not null
      and domaintype = 'business'
  )
  , duplicate_signup_removed as (
    select
      *
    from
      sorted_mqi
    where
      (
        rn = 1
        or rn is null
      )
      and domain <> 'vidyard.com'
  )
  , new_lead as (
    select
      email
      , min(mqidate) as firstmqidate
    from
      duplicate_signup_removed
    group by
      1
  )
  , new_domain as (
   select
   domain
   , min(mqidate) as firstdomainmqidate
   FROM
    duplicate_signup_removed
    group by 1
  )
  , new_lead_details as (
    select
      n.email
      , n.firstmqidate
      , d.campaignsourcecategory as firstcampaignsourcecategory
      , d.campaignsource as firstcampaignsource
      , d.ctatype as firstctatype
      , d.ctasubtype as firstctasubtype
    from
      new_lead n
      left join duplicate_signup_removed d on
        n.email = d.email
        and n.firstmqidate = d.mqidate
    group by
      1
      , 2
      , 3
      , 4
      , 5
      , 6
  )
  , new_domain_details as (
    SELECT d.email
    , n.firstdomainmqidate
    from new_domain n
    left join duplicate_signup_removed d on
    n.domain = d.domain and n.firstdomainmqidate = d.mqidate
  )
  , summary as (
    select
      leadcontactid
      , email
      , domain
      , domaintype
      , campaignsourcecategory
      , campaignsource
      , case
        when ctatype in (
          'Lead List'
          , 'Other'
        )
          then 'Other'
        when ctatype = 'Organic Lead Score'
          then 'Other'
        when ctatype is null
          then 'Other'
        when ctatype in (
          'Events'
          , 'Tradeshow'
          , 'Webinar'
        )
          then 'Events'
        when ctatype in (
          'Video'
          , 'Content Asset'
        )
          then 'Content Asset'
        else ctatype
      end as ctacategory
      , ctatype
      , ctasubtype
      , region
      , country
      , customertype
      , mqidate
      , to_char(date_trunc('month', mqidate), 'YYYY-MM') as mqimonth
      , mqldate
      , to_char(date_trunc('month', mqldate), 'YYYY-MM') as mqlmonth
      , saldate
      , to_char(date_trunc('month', saldate), 'YYYY-MM') as salmonth
      , sqldate
      , to_char(date_trunc('month', sqldate), 'YYYY-MM') as sqlmonth
      ,sqodate
      , to_char(date_trunc('month', sqodate), 'YYYY-MM') as sqomonth

      , case
        when firstmqidate = mqidate
          then true
        else false
      end as isnetnewemail
      , firstmqidate
      , to_char(date_trunc('month', firstmqidate), 'YYYY-MM') as firstmqimonth
      , case
        when firstdomainmqidate = mqidate
          then true
        else false
      end as isnetnewdomain
      , firstdomainmqidate
      , to_char(date_trunc('month', firstdomainmqidate), 'YYYY-MM') as firstdomainmqimonth

    from
      duplicate_signup_removed d
      left join new_lead_details using (email)
      left join new_domain_details using (email)
  )
select
  *
from
  summary

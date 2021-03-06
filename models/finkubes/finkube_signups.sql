with
  sessions as (
    select
      cast(identifier as varchar(50)) as userid
      , derived_channel as channels
      , sessiontime
      , c.region
    from
      {{ref('tier2_heap')}} h
    left join  {{ref('fct_sfdc_country_to_region')}} c on lower(h.country) = c.country
    where identifier is not null and tracker = 'global_session'
  )
  , user_session_table as (
    select
    o.organizationid
  , case
          when o.createdbyclientid = 'android.vidyard.com'
            then 'Android'
          when o.createdbyclientid = 'govideo-mobile.vidyard.com'
            then 'iOS'
          when o.createdbyclientid = 'app.slack.com'
            then 'Slack'
          when o.createdbyclientid not in ('secure.vidyard.com','auth.viewedit.com','edge-extension.vidyard.com')
            then 'Partner App'
          when o.createdbyclientid = 'edge-extension.vidyard.com'
            then 'Edge'
          when st.channels is null
            then 'Direct'
          else st.channels
        end as channel
  , st.sessiontime
  , st.region
  , row_number() over(partition by organizationid order by st.sessiontime) as rn
  from
  {{ref('tier2_vidyard_user_details')}} o
  left join {{ref('tier2_vidyard_users')}} u using (userid,organizationid)
left join
  sessions st on st.userid = o.userid
    where
      u.orgtype = 'self_serve'
      and o.createdbyclientid != 'Enterprise'
      and u.email not like '%vidyard.com'
      and u.email not like '%viewedit.com' -- exclude hubspot personal signups per zendesk ticket #85501 and #98583
      and o.createdbyclientid not in (
        'app.hubspot.com'
        , 'marketing.hubspot.com'
        , 'marketing.hubspotqa.com'
      )
      and datediff('minute', st.sessiontime, o.createddate) between 0
      and 30
      -- only include sessions 30 minutes prior to signup
  )

, summary as (
select
  o.organizationid

  , case when z.crmid is null then z.accountnumber else left(z.crmid, 15) end as accountid15

  , to_char(date_trunc('month', o.createddate), 'yyyy-mm') as signupmonth
  , date(o.createddate) as signupdate
  , case
    when ust.organizationid is null
    and (case
          when o.createdbyclientid = 'android.vidyard.com'
            then 'Android'
          when o.createdbyclientid = 'govideo-mobile.vidyard.com'
            then 'iOS'
          when o.createdbyclientid = 'app.slack.com'
            then 'Slack'
          when o.createdbyclientid not in ('secure.vidyard.com','auth.viewedit.com','edge-extension.vidyard.com')
            then 'Partner App'
          when o.createdbyclientid = 'edge-extension.vidyard.com'
            then 'Edge'
        end) is not null
      then (case
            when o.createdbyclientid = 'android.vidyard.com'
              then 'Android'
            when o.createdbyclientid = 'govideo-mobile.vidyard.com'
              then 'iOS'
            when o.createdbyclientid = 'app.slack.com'
              then 'Slack'
            when o.createdbyclientid not in ('secure.vidyard.com','auth.viewedit.com','edge-extension.vidyard.com')
              then 'Partner App'
            when o.createdbyclientid = 'edge-extension.vidyard.com'
              then 'Edge'
          end)
    when ust.organizationid is null
    and (case
          when o.createdbyclientid = 'android.vidyard.com'
            then 'Android'
          when o.createdbyclientid = 'govideo-mobile.vidyard.com'
            then 'iOS'
          when o.createdbyclientid = 'app.slack.com'
            then 'Slack'
          when o.createdbyclientid not in ('secure.vidyard.com','auth.viewedit.com','edge-extension.vidyard.com')
            then 'Partner App'
          when o.createdbyclientid = 'edge-extension.vidyard.com'
            then 'Edge'
        end) is null
      then 'Direct'
    when ust.organizationid is not null
    and ust.channel is null
      then 'Direct'
    else ust.channel
  end as subchannel
  , case
    when subchannel in (
      'Android'
      , 'Chrome'
      , 'Edge'
      , 'iOS'
    )
      then 'Marketplace'
    when subchannel in (
      'Player'
      , 'Product'
    )
      then 'Product'
    when subchannel in (
      'Email'
      , 'Referral'
      , 'Slack'
    )
      then 'Referral'
    when subchannel in (
      'Paid'
      , 'Social'
    )
      then 'Paid'
    else subchannel
  end as channel
  , region
  , o.domain
  , o.domaintype
  , u.email

  from
  {{ref('tier2_vidyard_user_details')}} o
  join {{ref('tier2_vidyard_users')}} u using (userid, organizationid)
  left join user_session_table ust using (organizationid)
  LEFT JOIN {{ ref('tier2_zuora') }} z  ON z.vidyardaccountid = o.organizationid
where
  u.orgtype = 'self_serve'
  and o.createdbyclientid != 'Enterprise'
  and u.email not like '%vidyard.com'
  and u.email not like '%viewedit.com' -- exclude hubspot personal signups per zendesk ticket #85501 and #98583
  and o.createdbyclientid not in (
    'app.hubspot.com'
    , 'marketing.hubspot.com'
    , 'marketing.hubspotqa.com'
  )
  and (
    ust.rn = 1 or ust.rn is null
  )
)

, sfdc as (
  select accountid
  from {{ref('tier2_salesforce_account')}} a
)

select organizationid, nvl(accountid,accountid15) as accountid, signupmonth, signupdate, subchannel, channel, region, domain, domaintype, email
from summary s
left join sfdc a on left(a.accountid,15) = s.accountid15
group by 1,2,3,4,5,6,7,8,9,10

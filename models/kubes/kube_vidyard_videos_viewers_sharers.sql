select 
   vus.userId as sharer_id
   , vuv.userId as viewer_id
   , vus.createdDate as sharer_signup_date
   , vuv.createdDate as viewer_signup_date
   , vus.email
   , case when datediff('minute', pv.eventTime, vuv.createdDate) between 0 and 30 then 1 else null end as viral_signups_flag
   , l.name
   , l.title
   , l.company
   , pv.userID as pageview_id
   , pv.eventTime as event_time
   , pv.utmSource as utm_source
   , pv.utmMedium as utm_medium
   , pv.utmCampaign as utm_campaign
   , pv.utmTerm as utm_term
   , split_part(pv.path, '/watch/', 2) as view_path
   , p.createdDate as player_created_date
 
from 
    {{ ref('stg_govideo_production_pageviews') }} pv 
      left join {{ ref('stg_govideo_production_users') }} hu 
        on pv.userID = hu.userID
      left join {{ ref('stg_vidyard_users') }} vuv 
        on vuv.userId = hu.vidyardUserId
      left join {{ ref('stg_vidyard_players') }} p 
        on p.uuID = split_part(pv.path, '/watch/', 2)
      left join {{ ref('stg_vidyard_organizations') }} o 
        on o.organizationID = p.organizationID
      left join {{ ref('stg_vidyard_users') }} vus 
        on vus.userId = o.ownerId
      left join {{ ref('tier2_salesforce_lead') }} l 
        on l.vidyardUserId = vus.userId
 
where
   pv.domain = 'share.vidyard.com'
   and vuv.email not like '%vidyard.com' 
   and vuv.email not like '%viewedit.com'
   and vus.email not like '%vidyard.com'
   and vus.email not like '%viewedit.com'
   and o.orgType = 'self_serve'
   and o.createdByClientID not in ('Enterprise', 'app.hubspot.com', 'marketing.hubspot.com', 'marketing.hubspotqa.com')

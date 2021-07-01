select 
	gs.eventid
  	, u."identity" as vid_userid
    , gs.userid
    , gs.sessionid
    , gs.sessiontime
    , null as eventtime
    , gs.landingpage
    , gs.domain
    , gs.channels
  	, gs.path
    , case
        when gs.landingpage like '%share.vidyard.com%'
          then 'Product'
        when gs.landingpage like '%welcome.vidyard.com/chrome_setup/%'
          then 'Chrome'
        when gs.landingpage like '%jiihcciniecimeajcniapbngjjbonjan%'
          then 'Chrome'
        when gs.channels like 'Product'
          then 'Player'
        else gs.channels
      end as derived_channel
    , 'GS' as tracker
from {{ ref('stg_govideo_production_global_session') }} gs
join {{ ref('stg_govideo_production_users') }} u
	on gs.userid = u.userid and u."identity" is not null
  
union all

select 
	oe.eventid
  	, u."identity" as vid_userid
    , oe.userid
    , null as sessionid
    , null as sessiontime
    , oe.eventtime
    , oe.landingpage
    , oe.domain
    , oe.channels
  	, oe.path
    , null as derived_channel
    , 'OE' as tracker
from {{ ref('stg_govideo_production_opened_extension') }} oe
join {{ ref('stg_govideo_production_users') }} u
	on oe.userid = u.userid and u."identity" is not null
  
union all

select 
	pv.eventid
  	, u."identity" as vid_userid
    , pv.userid
    , null as sessionid
    , null as sessiontime
    , pv.eventtime
    , pv.landingpage
    , pv.domain
    , null as channels
  	, pv.path
    , null as derived_channel
    , 'PV' as tracker
from {{ ref('stg_govideo_production_pageviews') }} pv
join {{ ref('stg_govideo_production_users') }} u
	on pv.userid = u.userid and u."identity" is not null
  
union all

select 
	ps.eventid
  	, u."identity" as vid_userid
    , ps.userid
    , null as sessionid
    , null as sessiontime
    , ps.eventtime
    , ps.landingpage
    , ps.domain
    , ps.channels
  	, ps.path
    , null as derived_channel
    , 'PS' as tracker
from {{ ref('stg_govideo_production_product_sessions') }} ps
join {{ ref('stg_govideo_production_users') }} u
	on ps.userid = u.userid and u."identity" is not null

union all

select 
	ssc.eventid
  	, u."identity" as vid_userid
    , ssc.userid
    , ssc.sessionid
    , ssc.sessiontime
    , null as eventtime
    , ssc.landingpage
    , ssc.domain
    , ssc.channels
  	, ssc.path
    , null as derived_channel
    , 'SSC' as tracker
from {{ ref('stg_govideo_production_sharing_share_combo') }} ssc
join {{ ref('stg_govideo_production_users') }} u
	on ssc.userid = u.userid and u."identity" is not null
  
union all

select 
	vidcompv.eventid
  	, u."identity" as vid_userid
    , vidcompv.userid
    , null as sessionid
    , null as sessiontime
    , vidcompv.eventtime
    , vidcompv.landingpage
    , vidcompv.domain
    , vidcompv.channels
  	, vidcompv.path
    , null as derived_channel
    , 'VCPV' as tracker
from {{ ref('stg_govideo_production_vidyard_com_any_pageview') }} vidcompv
join {{ ref('stg_govideo_production_users') }} u
	on vidcompv.userid = u.userid and u."identity" is not null
  
union all

select 
	vidcomss.eventid
  	, u."identity" as vid_userid
    , vidcomss.userid
    , vidcomss.sessionid
    , vidcomss.sessiontime
    , null as eventtime
    , vidcomss.landingpage
    , vidcomss.domain
    , vidcomss.channels
  	, vidcomss.path
    , null as derived_channel
    , 'VCSS' as tracker
from {{ ref('stg_govideo_production_vidyard_com_sessions') }} vidcomss
join {{ ref('stg_govideo_production_users') }} u
	on vidcomss.userid = u.userid and u."identity" is not null
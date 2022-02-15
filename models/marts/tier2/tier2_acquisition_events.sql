select sessionid,
       'Product Channel' as acquisition_channel,
       'Watermark' as acquisition_event
from {{ ref('stg_govideo_production_viewed_watermark_landing_page') }}
union
select sessionid,
       'Product Channel' as acquisition_channel,
       'Backlink' as acquisition_event
from {{ ref('stg_govideo_production_viewed_backlink_landing_page') }}
union
select sessionid,
       'Product Channel' as acquisition_channel,
       'Right Click - About' as acquisition_event
from {{ ref('stg_govideo_production_viewed_landing_page_after_right_click_player_and_about') }}
union
select sessionid,
       'Product Channel' as acquisition_channel,
       'Pause CTA' as acquiition_event
from {{ ref('stg_govideo_production_signed_up_on_video_pause') }}
union
select sessionid,
       'Product Channel' as acquisition_channel,
       'End CTA' as acquisition_event
from {{ ref('stg_govideo_production_signed_up_on_video_end_screen') }}



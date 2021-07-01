
-- more fields will be added to the table 
select
      hu."identity" as heap_vid_userid
      , (
       case
          when s.landingpage like '%share.vidyard.com%'
            then 'Product'
          when s.landingpage like '%welcome.vidyard.com/chrome_setup/%'
            then 'Chrome'
          when s.landingpage like '%jiihcciniecimeajcniapbngjjbonjan%'
            then 'Chrome'
          when s.channels like 'Product'
            then 'Player'
          else s.channels
        end
      ) as channels
      , s.sessiontime
from
    {{ ref('stg_govideo_production_global_session') }} s
    join {{ ref('stg_govideo_production_users') }} hu on
    hu.userid = s.userid
where
    hu."identity" is not null
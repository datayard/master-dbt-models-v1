SELECT
        gs.eventid
        , u.identifier 
        , u.vidyardUserId
        , gs.userid
        , gs.sessionid
        , gs.sessiontime
        , NULL         AS eventtime
        , gs.landingpage
        , gs.domain
        , gs.channels
        , gs.path
        , CASE
               WHEN gs.landingpage LIKE '%share.vidyard.com%'
                   THEN 'Product'
               WHEN gs.landingpage LIKE '%welcome.vidyard.com/chrome_setup/%'
                   THEN 'Chrome'
               WHEN gs.landingpage LIKE '%jiihcciniecimeajcniapbngjjbonjan%'
                   THEN 'Chrome'
               WHEN gs.channels LIKE 'Product'
                   THEN 'Player'
               ELSE gs.channels
          END AS derived_channel
        , gs.sessiontime AS column_for_distribution
        , 'global_session'  AS tracker
    FROM
        {{ ref('stg_govideo_production_global_session') }} gs
            JOIN {{ ref('stg_govideo_production_users') }} u
                ON gs.userid = u.userid AND u.identifier IS NOT NULL
    where tracker = 'global_session' 

    UNION ALL

    SELECT
        oe.eventid
        , u.identifier 
        , u.vidyardUserId
        , oe.userid
        , null AS sessionid
        , null AS sessiontime
        , oe.eventtime
        , oe.landingpage
        , oe.domain
        , oe.channels
        , oe.path
        , null AS derived_channel
        , oe.eventtime AS column_for_distribution
        , 'opened_extension' AS tracker
    FROM
        {{ ref('stg_govideo_production_opened_extension') }} oe
            JOIN {{ ref('stg_govideo_production_users') }} u
                ON oe.userid = u.userid AND u.identifier IS NOT NULL
    -- this filter will only be applied on an incremental run
    where tracker = 'opened_extension' 

    UNION ALL

    SELECT
        pv.eventid
        , u.identifier 
        , u.vidyardUserId
        , pv.userid
        , NULL AS sessionid
        , NULL AS sessiontime
        , pv.eventtime
        , pv.landingpage
        , pv.domain
        , NULL AS channels
        , pv.path
        , NULL AS derived_channel
        , pv.eventtime AS column_for_distribution
        , 'page_views' AS tracker
    FROM
        {{ ref('stg_govideo_production_pageviews') }} pv
            JOIN {{ ref('stg_govideo_production_users') }} u
                ON pv.userid = u.userid AND u.identifier IS NOT NULL
    where tracker = 'page_views' 

    UNION ALL

    SELECT
        ps.eventid
        , u.identifier 
        , u.vidyardUserId
        , ps.userid
        , NULL AS sessionid
        , NULL AS sessiontime
        , ps.eventtime
        , ps.landingpage
        , ps.domain
        , ps.channels
        , ps.path
        , NULL AS derived_channel
        , ps.eventtime AS column_for_distribution
        , 'product_sessions' AS tracker
    FROM
        {{ ref('stg_govideo_production_product_sessions') }} ps
            JOIN {{ ref('stg_govideo_production_users') }} u
                ON ps.userid = u.userid AND u.identifier IS NOT NULL
    where tracker = 'product_sessions'

    UNION ALL

    SELECT
        ssc.eventid
        , u.identifier 
        , u.vidyardUserId
        , ssc.userid
        , ssc.sessionid
        , ssc.sessiontime
        , ssc.eventtime
        , ssc.landingpage
        , ssc.domain
        , ssc.channels
        , ssc.path
        , NULL AS derived_channel
        , ssc.eventtime AS column_for_distribution
        , 'sharing_share_combo' AS tracker
    FROM
        {{ ref('stg_govideo_production_sharing_share_combo') }} ssc
        JOIN {{ ref('stg_govideo_production_users') }} u
            ON ssc.userid = u.userid AND u.identifier IS NOT NULL

    where tracker = 'sharing_share_combo' 

    UNION ALL

    SELECT
        vidcompv.eventid
        , u.identifier 
        , u.vidyardUserId
        , vidcompv.userid
        , NULL AS sessionid
        , NULL AS sessiontime
        , vidcompv.eventtime
        , vidcompv.landingpage
        , vidcompv.domain
        , vidcompv.channels
        , vidcompv.path
        , NULL AS derived_channel
        , vidcompv.eventtime AS column_for_distribution
        , 'vy_com_page_view' AS tracker
    FROM
        {{ ref('stg_govideo_production_vidyard_com_any_pageview') }} vidcompv
            JOIN {{ ref('stg_govideo_production_users') }} u
                ON vidcompv.userid = u.userid AND u.identifier IS NOT NULL
    where tracker = 'vy_com_page_view'

    UNION ALL

    SELECT
        vidcomss.eventid
        , u.identifier 
        , u.vidyardUserId
        , vidcomss.userid
        , vidcomss.sessionid
        , vidcomss.sessiontime
        , NULL AS eventtime
        , vidcomss.landingpage
        , vidcomss.domain
        , vidcomss.channels
        , vidcomss.path
        , NULL AS derived_channel
        , vidcomss.sessiontime AS column_for_distribution
        , 'vy_com_sessions' AS tracker
    FROM {{ ref('stg_govideo_production_vidyard_com_sessions') }} vidcomss
        JOIN {{ ref('stg_govideo_production_users') }} u
            ON vidcomss.userid = u.userid AND u.identifier IS NOT NULL
    where tracker = 'vy_com_sessions' 

    UNION ALL

    SELECT
        pv.eventid
        , u.identifier 
        , u.vidyardUserId
        , pv.userid
        , NULL AS sessionid
        , NULL AS sessiontime
        , pv.eventtime
        , pv.landingpage
        , pv.domain
        , NULL AS channels
        , pv.path
        , NULL AS derived_channel
        , pv.eventtime AS column_for_distribution
        , 'video_creation' AS tracker
    FROM
        {{ ref('stg_govideo_production_video_creation_started_to_create_or_upload_a_video_combo') }} pv
            JOIN {{ ref('stg_govideo_production_users') }} u
                ON pv.userid = u.userid AND u.identifier IS NOT NULL
    where tracker = 'video_creation'

    UNION ALL

    SELECT
        pv.eventid
        , u.identifier 
        , u.vidyardUserId
        , pv.userid
        , NULL AS sessionid
        , NULL AS sessiontime
        , pv.eventtime
        , pv.landingpage
        , pv.domain
        , NULL AS channels
        , pv.path
        , NULL AS derived_channel
        , pv.eventtime AS column_for_distribution
        , 'video_upload' AS tracker
    FROM
        {{ ref('stg_govideo_production_video_recorded_or_uploaded') }} pv
            JOIN {{ ref('stg_govideo_production_users') }} u
                ON pv.userid = u.userid AND u.identifier IS NOT NULL
    where tracker = 'video_upload'

{{
    config(
        materialized='incremental'
    )
}}

SELECT
        gs.eventid
        , u.identifier 
        , u.vidyardUserId
        , gs.userid
        , gs.sessionid
        , gs.sessiontime
        , NULL AS eventtime
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
        , 'global_session'  AS tracker
    FROM
        {{ ref('stg_govideo_production_global_session') }} gs
            JOIN {{ ref('stg_govideo_production_users') }} u
                ON gs.userid = u.userid AND u.identifier IS NOT NULL
      {% if is_incremental() %}
    -- this filter will only be applied on an incremental run
    WHERE gs.sessiontime > (select max(sessiontime) from {{ this }} where tracker = 'global_session' )
    {% endif %}

    UNION ALL

    SELECT
        oe.eventid
        , u.identifier 
        , u.vidyardUserId
        , oe.userid
        , oe.sessionid
        , oe.sessiontime
        , oe.eventtime
        , oe.landingpage
        , oe.domain
        , oe.channels
        , oe.path
        , null AS derived_channel
        , 'opened_extension' AS tracker
    FROM
        {{ ref('stg_govideo_production_opened_extension') }} oe
            JOIN {{ ref('stg_govideo_production_users') }} u
                ON oe.userid = u.userid AND u.identifier IS NOT NULL
    -- this filter will only be applied on an incremental run
        {% if is_incremental() %}
    -- this filter will only be applied on an incremental run
    WHERE oe.eventtime > (select max(eventtime) from {{ this }} where tracker = 'opened_extension' )
    {% endif %}

    UNION ALL

    SELECT
        pv.eventid
        , u.identifier 
        , u.vidyardUserId
        , pv.userid
        , pv.sessionid
        , pv.sessiontime
        , pv.eventtime
        , pv.landingpage
        , pv.domain
        , NULL AS channels
        , pv.path
        , NULL AS derived_channel
        , 'page_views' AS tracker
    FROM
        {{ ref('stg_govideo_production_pageviews') }} pv
            JOIN {{ ref('stg_govideo_production_users') }} u
                ON pv.userid = u.userid AND u.identifier IS NOT NULL
     {% if is_incremental() %}
    -- this filter will only be applied on an incremental run
    WHERE pv.eventtime > (select max(eventtime) from {{ this }} where tracker = 'page_views' )
    {% endif %}

    UNION ALL

    SELECT
        ps.eventid
        , u.identifier 
        , u.vidyardUserId
        , ps.userid
        , ps.sessionid
        , ps.sessiontime
        , ps.eventtime
        , ps.landingpage
        , ps.domain
        , ps.channels
        , ps.path
        , NULL AS derived_channel
        , 'product_sessions' AS tracker
    FROM
        {{ ref('stg_govideo_production_product_sessions') }} ps
            JOIN {{ ref('stg_govideo_production_users') }} u
                ON ps.userid = u.userid AND u.identifier IS NOT NULL
    {% if is_incremental() %}
    -- this filter will only be applied on an incremental run
    WHERE ps.eventtime > (select max(eventtime) from {{ this }} where tracker = 'product_sessions' )
    {% endif %}

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
        , 'sharing_share_combo' AS tracker
    FROM
        {{ ref('stg_govideo_production_sharing_share_combo') }} ssc
        JOIN {{ ref('stg_govideo_production_users') }} u
            ON ssc.userid = u.userid AND u.identifier IS NOT NULL

    {% if is_incremental() %}
    -- this filter will only be applied on an incremental run
    WHERE ssc.eventtime > (select max(eventtime) from {{ this }} where tracker = 'sharing_share_combo' )
    {% endif %}

    UNION ALL

    SELECT
        vidcompv.eventid
        , u.identifier 
        , u.vidyardUserId
        , vidcompv.userid
        , vidcompv.sessionid
        , vidcompv.sessiontime
        , vidcompv.eventtime
        , vidcompv.landingpage
        , vidcompv.domain
        , vidcompv.channels
        , vidcompv.path
        , NULL AS derived_channel
        , 'vy_com_page_view' AS tracker
    FROM
        {{ ref('stg_govideo_production_vidyard_com_any_pageview') }} vidcompv
            JOIN {{ ref('stg_govideo_production_users') }} u
                ON vidcompv.userid = u.userid AND u.identifier IS NOT NULL
    {% if is_incremental() %}
    -- this filter will only be applied on an incremental run
    WHERE vidcompv.eventtime > (select max(eventtime) from {{ this }} where tracker = 'vy_com_page_view' )
    {% endif %}

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
        , 'vy_com_sessions' AS tracker
    FROM {{ ref('stg_govideo_production_vidyard_com_sessions') }} vidcomss
        JOIN {{ ref('stg_govideo_production_users') }} u
            ON vidcomss.userid = u.userid AND u.identifier IS NOT NULL
    {% if is_incremental() %}
    -- this filter will only be applied on an incremental run
    WHERE vidcomss.sessiontime > (select max(sessiontime) from {{ this }} where tracker = 'vy_com_sessions' )
    {% endif %}

    UNION ALL

    SELECT
        pv.eventid
        , u.identifier 
        , u.vidyardUserId
        , pv.userid
        , pv.sessionid
        , pv.sessiontime
        , pv.eventtime
        , pv.landingpage
        , pv.domain
        , NULL AS channels
        , pv.path
        , NULL AS derived_channel
        , 'video_creation' AS tracker
    FROM
        {{ ref('stg_govideo_production_video_creation_started_to_create_or_upload_a_video_combo') }} pv
            JOIN {{ ref('stg_govideo_production_users') }} u
                ON pv.userid = u.userid AND u.identifier IS NOT NULL
    {% if is_incremental() %}
    -- this filter will only be applied on an incremental run
    WHERE pv.eventtime > (select max(eventtime) from {{ this }} where tracker = 'video_creation' )
    {% endif %}

    UNION ALL

    SELECT
        pv.eventid
        , u.identifier 
        , u.vidyardUserId
        , pv.userid
        , pv.sessionid
        , pv.sessiontime
        , pv.eventtime
        , pv.landingpage
        , pv.domain
        , NULL AS channels
        , pv.path
        , NULL AS derived_channel
        , 'video_upload' AS tracker
    FROM
        {{ ref('stg_govideo_production_video_recorded_or_uploaded') }} pv
            JOIN {{ ref('stg_govideo_production_users') }} u
                ON pv.userid = u.userid AND u.identifier IS NOT NULL
    {% if is_incremental() %}
    -- this filter will only be applied on an incremental run
    WHERE pv.eventtime > (select max(eventtime) from {{ this }} where tracker = 'video_upload' )
    {% endif %}

    UNION ALL

    SELECT
        ac.eventID
        , u.identifier 
        , u.vidyardUserId
        , ac.userID
        , ac.sessionID
        , ac.sessionTime
        , ac.eventTime
        , ac.landingPage
        , ac.domain
        , ac.channels
        , ac.path
        , NULL AS derived_channel
        , 'admin_combo' AS tracker
    FROM
        {{ ref('stg_govideo_production_admin_combo') }} ac
        JOIN {{ ref('stg_govideo_production_users') }} u
                    ON ac.userid = u.userid AND u.identifier IS NOT NULL
        {% if is_incremental() %}
        -- this filter will only be applied on an incremental run
        WHERE ac.eventtime > (select max(eventtime) from {{ this }} where tracker = 'admin_combo' )
        {% endif %}

    UNION ALL

    SELECT
        iac.eventID
        , u.identifier 
        , u.vidyardUserId
        , iac.userID
        , iac.sessionID
        , iac.sessionTime
        , iac.eventTime
        , iac.landingPage
        , iac.domain
        , iac.channels
        , iac.path
        , NULL AS derived_channel
        , 'insights_analytics_combo' AS tracker
    FROM
        {{ ref('stg_govideo_production_insights_analytics_combo') }} iac
        JOIN {{ ref('stg_govideo_production_users') }} u
                    ON iac.userid = u.userid AND u.identifier IS NOT NULL
        {% if is_incremental() %}
        -- this filter will only be applied on an incremental run
        WHERE iac.eventtime > (select max(eventtime) from {{ this }} where tracker = 'insights_analytics_combo' )
        {% endif %}
    UNION ALL

    SELECT
        mc.eventID
        , u.identifier 
        , u.vidyardUserId
        , mc.userID
        , mc.sessionID
        , mc.sessionTime
        , mc.eventTime
        , mc.landingPage
        , mc.domain
        , mc.channels
        , mc.path
        , NULL AS derived_channel
        , 'manage_combo' AS tracker
    FROM
        {{ ref('stg_govideo_production_manage_combo') }} mc
        JOIN {{ ref('stg_govideo_production_users') }} u
                    ON mc.userid = u.userid AND u.identifier IS NOT NULL
        {% if is_incremental() %}
        -- this filter will only be applied on an incremental run
        WHERE mc.eventtime > (select max(eventtime) from {{ this }} where tracker = 'manage_combo' )
        {% endif %}        
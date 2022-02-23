{{
    config(
        pre_hook= [
                    "{% if is_incremental() %} DELETE FROM {{ this }} WHERE sessiontime >= DATEADD(day, -1, current_date) {% endif %}",
                    "{% if is_incremental() %} DELETE FROM {{ this }} WHERE eventtime >= DATEADD(day, -1, current_date) {% endif %}"
                ],
        materialized='incremental'
    )
}}

SELECT
        gs.eventid
        , u.identifier
        , u.vidyardUserId
        , u.generalUseCase
        , u.specificUseCase
        , gs.userid
        , NULL as clientId
        , gs.sessionid
        , gs.sessiontime
        , NULL AS eventtime
        , gs.landingpage
        , gs.domain
        , gs.channels
        , gs.path
        , gs.country
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
        , NULL AS utmcampaign
        , NULL AS utmsource
        , NULL AS utmterm
        , NULL AS utmMedium
        , NULL AS new_visit_indicator
        , gs.gregion as region
        , 'global_session'  AS tracker
    FROM
        {{ ref('stg_govideo_production_global_session') }} gs
            LEFT JOIN {{ ref('stg_govideo_production_users') }} u
                ON gs.userid = u.userid
        {% if is_incremental() %}
            -- this filter will only be applied on an incremental run
            WHERE gs.sessiontime > (select max(sessiontime) from {{ this }} where tracker = 'global_session' )
                and gs.sessiontime < DATEADD(day, 1, current_date)

        {% elif 'dbt_cloud_pr_' in this.schema %}

            WHERE gs.sessiontime > DATEADD(day, -3, current_date)
                and gs.sessiontime < DATEADD(day, 1, current_date)

        {% endif %}

    UNION ALL

    SELECT
        oe.eventid
        , u.identifier
        , u.vidyardUserId
        , u.generalUseCase
        , u.specificUseCase
        , oe.userid
        , NULL as clientId
        , oe.sessionid
        , oe.sessiontime
        , oe.eventtime
        , oe.landingpage
        , oe.domain
        , oe.channels
        , oe.path
        , oe.country
        , null AS derived_channel
        , NULL AS utmcampaign
        , NULL AS utmsource
        , NULL AS utmterm
        , NULL AS utmMedium
        , NULL AS new_visit_indicator
        , oe.gregion as region
        , 'opened_extension' AS tracker
    FROM
        {{ ref('stg_govideo_production_opened_extension') }} oe
            JOIN {{ ref('stg_govideo_production_users') }} u
                ON oe.userid = u.userid AND u.identifier IS NOT NULL
    
        {% if is_incremental() %}

            -- this filter will only be applied on an incremental run
            WHERE oe.eventtime > (select max(eventtime) from {{ this }} where tracker = 'opened_extension' )
                and oe.eventtime < DATEADD(day, 1, current_date)

        {% elif 'dbt_cloud_pr_' in this.schema %}

            WHERE oe.eventtime > DATEADD(day, -3, current_date)
                and oe.eventtime < DATEADD(day, 1, current_date)
                
        {% endif %}

    UNION ALL

    SELECT
        pv.eventid
        , u.identifier
        , u.vidyardUserId
        , u.generalUseCase
        , u.specificUseCase
        , pv.userid
        , NULL as clientId
        , pv.sessionid
        , pv.sessiontime
        , pv.eventtime
        , pv.landingpage
        , pv.domain
        , NULL AS channels
        , pv.path
        , pv.country
        , NULL AS derived_channel
        , NULL AS utmcampaign
        , NULL AS utmsource
        , NULL AS utmterm
        , NULL AS utmMedium
        , NULL AS new_visit_indicator
        , pv.gregion as region
        , 'page_views' AS tracker
    FROM
        {{ ref('stg_govideo_production_pageviews') }} pv
            LEFT JOIN {{ ref('stg_govideo_production_users') }} u
                ON pv.userid = u.userid
        
        {% if is_incremental() %}

            -- this filter will only be applied on an incremental run
            WHERE pv.eventtime > (select max(eventtime) from {{ this }} where tracker = 'page_views' )
                and pv.eventtime < DATEADD(day, 1, current_date)

        {% elif 'dbt_cloud_pr_' in this.schema %}

            WHERE pv.eventtime > DATEADD(day, -3, current_date)
                and pv.eventtime < DATEADD(day, 1, current_date)
                
        {% endif %}

    UNION ALL

    SELECT
        ps.eventid
        , u.identifier
        , u.vidyardUserId
        , u.generalUseCase
        , u.specificUseCase
        , ps.userid
        , NULL as clientId
        , ps.sessionid
        , ps.sessiontime
        , ps.eventtime
        , ps.landingpage
        , ps.domain
        , ps.channels
        , ps.path
        , ps.country
        , NULL AS derived_channel
        , NULL AS utmcampaign
        , NULL AS utmsource
        , NULL AS utmterm
        , NULL AS utmMedium
        , NULL AS new_visit_indicator
        , ps.gregion as region
        , 'product_sessions' AS tracker
    FROM
        {{ ref('stg_govideo_production_product_sessions') }} ps
            JOIN {{ ref('stg_govideo_production_users') }} u
                ON ps.userid = u.userid AND u.identifier IS NOT NULL

        {% if is_incremental() %}

            -- this filter will only be applied on an incremental run
            WHERE ps.eventtime > (select max(eventtime) from {{ this }} where tracker = 'product_sessions' )
                and ps.eventtime < DATEADD(day, 1, current_date)

        {% elif 'dbt_cloud_pr_' in this.schema %}

            WHERE ps.eventtime > DATEADD(day, -3, current_date)
                and ps.eventtime < DATEADD(day, 1, current_date)
                
        {% endif %}

    UNION ALL

    SELECT
        ssc.eventid
        , u.identifier
        , u.vidyardUserId
        , u.generalUseCase
        , u.specificUseCase
        , ssc.userid
        , ssc.clientId
        , ssc.sessionid
        , ssc.sessiontime
        , ssc.eventtime
        , ssc.landingpage
        , ssc.domain
        , ssc.channels
        , ssc.path
        , ssc.country
        , NULL AS derived_channel
        , NULL AS utmcampaign
        , NULL AS utmsource
        , NULL AS utmterm
        , NULL AS utmMedium
        , NULL AS new_visit_indicator
        , ssc.gregion as region
        , 'sharing_share_combo' AS tracker
    FROM
        {{ ref('stg_govideo_production_sharing_share_combo') }} ssc
        JOIN {{ ref('stg_govideo_production_users') }} u
            ON ssc.userid = u.userid AND u.identifier IS NOT NULL

        {% if is_incremental() %}
    
            -- this filter will only be applied on an incremental run
            WHERE ssc.eventtime > (select max(eventtime) from {{ this }} where tracker = 'sharing_share_combo' )
                and ssc.eventtime < DATEADD(day, 1, current_date)

        {% elif 'dbt_cloud_pr_' in this.schema %}

            WHERE ssc.eventtime > DATEADD(day, -3, current_date)
                and ssc.eventtime < DATEADD(day, 1, current_date)
                
        {% endif %}

    UNION ALL

    SELECT
        vidcompv.eventid
        , u.identifier
        , u.vidyardUserId
        , u.generalUseCase
        , u.specificUseCase
        , vidcompv.userid
        , NULL as clientId
        , vidcompv.sessionid
        , vidcompv.sessiontime
        , vidcompv.eventtime
        , vidcompv.landingpage
        , vidcompv.domain
        , vidcompv.channels
        , vidcompv.path
        , vidcompv.country
        , NULL AS derived_channel
        , vidcompv.utmcampaign
        , vidcompv.utmsource
        , vidcompv.utmterm
        , vidcompv.utmMedium
        , NULL AS new_visit_indicator
        , vidcompv.gregion as region
        , 'vy_com_page_view' AS tracker
    FROM
        {{ ref('stg_govideo_production_vidyard_com_any_pageview') }} vidcompv
            LEFT JOIN {{ ref('stg_govideo_production_users') }} u
                ON vidcompv.userid = u.userid

        {% if is_incremental() %}
            -- this filter will only be applied on an incremental run
            WHERE vidcompv.eventtime > (select max(eventtime) from {{ this }} where tracker = 'vy_com_page_view' )
                and vidcompv.eventtime < DATEADD(day, 1, current_date)

        {% elif 'dbt_cloud_pr_' in this.schema %}

            WHERE vidcompv.eventtime > DATEADD(day, -3, current_date)
                and vidcompv.eventtime < DATEADD(day, 1, current_date)
                
        {% endif %}

    UNION ALL

    SELECT
        vidcomss.eventid
        , u.identifier
        , u.vidyardUserId
        , u.generalUseCase
        , u.specificUseCase
        , vidcomss.userid
        , NULL as clientId
        , vidcomss.sessionid
        , vidcomss.sessiontime
        , NULL AS eventtime
        , vidcomss.landingpage
        , vidcomss.domain
        , vidcomss.channels
        , vidcomss.path
        , vidcomss.country
        , NULL AS derived_channel
        , NULL AS utmcampaign
        , NULL AS utmsource
        , NULL AS utmterm
        , NULL AS utmMedium
        , case
            when row_number() over(partition by vidcomss.userid order by vidcomss.sessiontime) = 1 then true
            else false
          end as new_visit_indicator
        , vidcomss.gregion as region
        , 'vy_com_sessions' AS tracker
    FROM {{ ref('stg_govideo_production_vidyard_com_sessions') }} vidcomss
        LEFT JOIN {{ ref('stg_govideo_production_users') }} u
            ON vidcomss.userid = u.userid
    
        {% if is_incremental() %}
            -- this filter will only be applied on an incremental run
            WHERE vidcomss.sessiontime > (select max(sessiontime) from {{ this }} where tracker = 'vy_com_sessions' )
                and vidcomss.sessiontime < DATEADD(day, 1, current_date)

        {% elif 'dbt_cloud_pr_' in this.schema %}

            WHERE vidcomss.sessiontime > DATEADD(day, -3, current_date)
                and vidcomss.sessiontime < DATEADD(day, 1, current_date)
                
        {% endif %}

    UNION ALL

    SELECT
        pv.eventid
        , u.identifier
        , u.vidyardUserId
        , u.generalUseCase
        , u.specificUseCase
        , pv.userid
        , NULL as clientId
        , pv.sessionid
        , pv.sessiontime
        , pv.eventtime
        , pv.landingpage
        , pv.domain
        , NULL AS channels
        , pv.path
        , pv.country
        , NULL AS derived_channel
        , NULL AS utmcampaign
        , NULL AS utmsource
        , NULL AS utmterm
        , NULL AS utmMedium
        , NULL AS new_visit_indicator
        , pv.gregion as region
        , 'video_creation' AS tracker
    FROM
        {{ ref('stg_govideo_production_video_creation_started_to_create_or_upload_a_video_combo') }} pv
            JOIN {{ ref('stg_govideo_production_users') }} u
                ON pv.userid = u.userid AND u.identifier IS NOT NULL
    
        {% if is_incremental() %}
            
            -- this filter will only be applied on an incremental run
            WHERE pv.eventtime > (select max(eventtime) from {{ this }} where tracker = 'video_creation' )
                and pv.eventtime < DATEADD(day, 1, current_date)

        {% elif 'dbt_cloud_pr_' in this.schema %}

            WHERE pv.eventtime > DATEADD(day, -3, current_date)
                and pv.eventtime < DATEADD(day, 1, current_date)
                
        {% endif %}

    UNION ALL

    SELECT
        pv.eventid
        , u.identifier
        , u.vidyardUserId
        , u.generalUseCase
        , u.specificUseCase
        , pv.userid
        , NULL as clientId
        , pv.sessionid
        , pv.sessiontime
        , pv.eventtime
        , pv.landingpage
        , pv.domain
        , NULL AS channels
        , pv.path
        , pv.country
        , NULL AS derived_channel
        , NULL AS utmcampaign
        , NULL AS utmsource
        , NULL AS utmterm
        , NULL AS utmMedium
        , NULL AS new_visit_indicator
        , pv.gregion as region
        , 'video_upload' AS tracker
    FROM
        {{ ref('stg_govideo_production_video_recorded_or_uploaded') }} pv
            JOIN {{ ref('stg_govideo_production_users') }} u
                ON pv.userid = u.userid AND u.identifier IS NOT NULL
    
        {% if is_incremental() %}
    
            -- this filter will only be applied on an incremental run
            WHERE pv.eventtime > (select max(eventtime) from {{ this }} where tracker = 'video_upload' )
                and pv.eventtime < DATEADD(day, 1, current_date)

        {% elif 'dbt_cloud_pr_' in this.schema %}

            WHERE pv.eventtime > DATEADD(day, -3, current_date)
                and pv.eventtime < DATEADD(day, 1, current_date)
                
        {% endif %}

    UNION ALL

    SELECT
        ac.eventID
        , u.identifier
        , u.vidyardUserId
        , u.generalUseCase
        , u.specificUseCase
        , ac.userID
        , NULL as clientId
        , ac.sessionID
        , ac.sessionTime
        , ac.eventTime
        , ac.landingPage
        , ac.domain
        , ac.channels
        , ac.path
        , ac.country
        , NULL AS derived_channel
        , NULL AS utmcampaign
        , NULL AS utmsource
        , NULL AS utmterm
        , NULL AS utmMedium
        , NULL AS new_visit_indicator
        , ac.gregion as region
        , 'admin_combo' AS tracker
    FROM
        {{ ref('stg_govideo_production_admin_combo') }} ac
        JOIN {{ ref('stg_govideo_production_users') }} u
                    ON ac.userid = u.userid AND u.identifier IS NOT NULL
        
        {% if is_incremental() %}
            
            -- this filter will only be applied on an incremental run
            WHERE ac.eventtime > (select max(eventtime) from {{ this }} where tracker = 'admin_combo' )
                and ac.eventtime < DATEADD(day, 1, current_date)

        {% elif 'dbt_cloud_pr_' in this.schema %}

            WHERE ac.eventtime > DATEADD(day, -3, current_date)
                and ac.eventtime < DATEADD(day, 1, current_date)
                
        {% endif %}

    UNION ALL

    SELECT
        iac.eventID
        , u.identifier
        , u.vidyardUserId
        , u.generalUseCase
        , u.specificUseCase
        , iac.userID
        , NULL as clientId
        , iac.sessionID
        , iac.sessionTime
        , iac.eventTime
        , iac.landingPage
        , iac.domain
        , iac.channels
        , iac.path
        , iac.country
        , NULL AS derived_channel
        , NULL AS utmcampaign
        , NULL AS utmsource
        , NULL AS utmterm
        , NULL AS utmMedium
        , NULL AS new_visit_indicator
        , iac.gregion as region
        , 'insights_analytics_combo' AS tracker
    FROM
        {{ ref('stg_govideo_production_insights_analytics_combo') }} iac
        JOIN {{ ref('stg_govideo_production_users') }} u
                    ON iac.userid = u.userid AND u.identifier IS NOT NULL

        {% if is_incremental() %}
            -- this filter will only be applied on an incremental run
            WHERE iac.eventtime > (select max(eventtime) from {{ this }} where tracker = 'insights_analytics_combo' )
                and iac.eventtime < DATEADD(day, 1, current_date)

        {% elif 'dbt_cloud_pr_' in this.schema %}

            WHERE iac.eventtime > DATEADD(day, -3, current_date)
                and iac.eventtime < DATEADD(day, 1, current_date)
                
        {% endif %}
    UNION ALL

    SELECT
        mc.eventID
        , u.identifier
        , u.vidyardUserId
        , u.generalUseCase
        , u.specificUseCase
        , mc.userID
        , NULL as clientId
        , mc.sessionID
        , mc.sessionTime
        , mc.eventTime
        , mc.landingPage
        , mc.domain
        , mc.channels
        , mc.path
        , mc.country
        , NULL AS derived_channel
        , NULL AS utmcampaign
        , NULL AS utmsource
        , NULL AS utmterm
        , NULL AS utmMedium
        , NULL AS new_visit_indicator
        , mc.gregion as region
        , 'manage_combo' AS tracker
    FROM
        {{ ref('stg_govideo_production_manage_combo') }} mc
        JOIN {{ ref('stg_govideo_production_users') }} u
                    ON mc.userid = u.userid AND u.identifier IS NOT NULL
        
        {% if is_incremental() %}
        
            -- this filter will only be applied on an incremental run
            WHERE mc.eventtime > (select max(eventtime) from {{ this }} where tracker = 'manage_combo' )
                and mc.eventtime < DATEADD(day, 1, current_date)

        {% elif 'dbt_cloud_pr_' in this.schema %}

            WHERE mc.eventtime > DATEADD(day, -3, current_date)
                and mc.eventtime < DATEADD(day, 1, current_date)
                
        {% endif %}

    UNION ALL

    SELECT
        cc.eventID
        , u.identifier
        , u.vidyardUserId
        , u.generalUseCase
        , u.specificUseCase
        , cc.userID
        , cc.clientId
        , cc.sessionID
        , cc.sessionTime
        , cc.eventTime
        , cc.landingPage
        , cc.domain
        , cc.channels
        , cc.path
        , cc.country
        , NULL AS derived_channel
        , NULL AS utmcampaign
        , NULL AS utmsource
        , NULL AS utmterm
        , NULL AS utmMedium
        , NULL AS new_visit_indicator
        , cc.gregion as region
        , 'video_creation_create_combo' AS tracker
    FROM
        {{ ref('stg_govideo_production_video_creation_create_combo') }} cc
        JOIN {{ ref('stg_govideo_production_users') }} u
                    ON cc.userid = u.userid AND u.identifier IS NOT NULL
        
        {% if is_incremental() %}
        
            -- this filter will only be applied on an incremental run
            WHERE cc.eventtime > (select max(eventtime) from {{ this }} where tracker = 'video_creation_create_combo' )
                and cc.eventtime < DATEADD(day, 1, current_date)

        {% elif 'dbt_cloud_pr_' in this.schema %}

            WHERE cc.eventtime > DATEADD(day, -3, current_date)
                and cc.eventtime < DATEADD(day, 1, current_date)
                
        {% endif %}

    UNION ALL

    SELECT
        vpa.eventID
        , u.identifier
        , u.vidyardUserId
        , u.generalUseCase
        , u.specificUseCase
        , vpa.userID
        , vpa.clientId
        , vpa.sessionID
        , vpa.sessionTime
        , vpa.eventTime
        , vpa.landingPage
        , vpa.domain
        , NULL AS channels
        , vpa.path
        , vpa.country
        , NULL AS derived_channel
        , NULL AS utmcampaign
        , NULL AS utmsource
        , NULL AS utmterm
        , NULL AS utmMedium
        , NULL AS new_visit_indicator
        , vpa.gregion as region
        , 'video_from_partner_app' AS tracker
    FROM
        {{ ref('stg_sharing_inserted_video_from_partner_app') }} vpa
        JOIN {{ ref('stg_govideo_production_users') }} u
                    ON vpa.userid = u.userid AND u.identifier IS NOT NULL
        
        {% if is_incremental() %}
        
            -- this filter will only be applied on an incremental run
            WHERE vpa.eventtime > (select max(eventtime) from {{ this }} where tracker = 'video_from_partner_app' )
                and vpa.eventtime < DATEADD(day, 1, current_date)

        {% elif 'dbt_cloud_pr_' in this.schema %}

            WHERE vpa.eventtime > DATEADD(day, -3, current_date)
                and vpa.eventtime < DATEADD(day, 1, current_date)
                
        {% endif %}

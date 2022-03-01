SELECT
    share_combo.event_id as eventID,
    share_combo.user_id as userID,
    share_combo.clientid as clientId,
    share_combo.session_id as sessionID,
    share_combo.time as eventTime,
    share_combo.session_time as sessionTime,
    --share_combo.continent as continent,
    share_combo.country as country,
    share_combo.region as region,
    share_combo.city as city,
    share_combo.platform as platform,
    share_combo.device as device,
    share_combo.device_type as deviceType,
    share_combo.browser as browser,
    --share_combo.browser_type as browserType,
    --share_combo.vidyard_platform as vidyardPlatform,
    share_combo.referrer as referrer,
    share_combo.landing_page as landingPage,
    share_combo.landing_page_query as landingPageQuery,
    share_combo.query as query,
    share_combo.domain as domain,
    share_combo.path as path,
    share_combo.title as title,
    share_combo.channels as channels,
    isnull(cr.region, 'Other') as gregion
FROM
    {{ source ('govideo_production' , 'sharing_share_combo')}} as share_combo
    left join {{ source('ops_utility_tables', 'country_names_with_region') }} cr
                on share_combo.country = cr.country_name
WHERE
    share_combo.time < DATEADD(day, 1, current_date)
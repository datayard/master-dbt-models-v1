SELECT 
        global_session.event_id as eventID,
        global_session.user_id as userID,
        global_session.session_id as sessionID,
        global_session.session_time as sessionTime,
        --global_session.continent as continent,
        global_session.country as country,
        global_session.region as region,
        global_session.city as city,
        global_session.platform as platform,
        global_session.device_type as deviceType,
        global_session.browser as browser,
        --global_session.browser_type as browserType,
        --global_session.vidyard_platform as vidyardPlatform,
        global_session.referrer as referrer,
        global_session.landing_page as landingPage,
        global_session.landing_page_query as landingPageQuery,
        global_session.query as query,
        global_session.domain as domain,
        global_session.path as path,
        global_session.title as title,
        global_session.channels as channels,
        global_session.utm_source as utmSource,
        global_session.utm_medium as utmMedium,
        global_session.utm_campaign as utmCampaign,
        global_session.utm_term as utmTerm,
        global_session.utm_content as utmContent
FROM
        {{ source('govideo_production' , 'global_session')}} as global_session
WHERE
        TRUE
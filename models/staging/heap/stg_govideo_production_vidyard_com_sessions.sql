SELECT
        vidyard_com_sessions.event_id as eventID,
        vidyard_com_sessions.user_id as userID,
        vidyard_com_sessions.session_id as sessionID,
        vidyard_com_sessions.session_time as sessionTime,
        --vidyard_com_sessions.continent as continent,
        vidyard_com_sessions.country as country,
        vidyard_com_sessions.region as region,
        vidyard_com_sessions.city as city,
        vidyard_com_sessions.platform as platform,
        vidyard_com_sessions.device_type as deviceType,
        vidyard_com_sessions.browser as browser,
        --vidyard_com_sessions.browser_type as browserType,
        --vidyard_com_sessions.vidyard_platform as vidyardPlatform,
        vidyard_com_sessions.referrer as referrer,
        vidyard_com_sessions.landing_page as landingPage,
        vidyard_com_sessions.landing_page_query as landingPageQuery,
        vidyard_com_sessions.query as query,
        vidyard_com_sessions.domain as domain,
        vidyard_com_sessions.path as path,
        vidyard_com_sessions.title as title,
        vidyard_com_sessions.channels as channels,
        vidyard_com_sessions.utm_source as utmSource,
        vidyard_com_sessions.utm_medium as utmMedium,
        vidyard_com_sessions.utm_campaign as utmCampaign,
        vidyard_com_sessions.utm_term as utmTerm,
        vidyard_com_sessions.utm_content as utmContent

FROM
        {{ source ('govideo_production' , 'vidyard_com_sessions')}} as vidyard_com_sessions

WHERE
        vidyard_com_sessions.session_time < DATEADD(day, 1, current_date)
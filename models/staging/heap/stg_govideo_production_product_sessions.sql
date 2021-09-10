SELECT
        product_sessions.event_id as eventID,
        product_sessions.session_id as sessionId,
        product_sessions.session_time as sessiontime,
        product_sessions.user_id as userID,
        product_sessions.time as eventTime,
        --product_sessions.continent as continent,
        product_sessions.country as country,
        product_sessions.region as region,
        product_sessions.city as city,
        product_sessions.platform as platform,
        product_sessions.device as device,
        product_sessions.device_type as deviceType,
        product_sessions.browser as browser,
        --product_sessions.browser_type as browserType,
        --product_sessions.vidyard_platform as vidyardPlatform,
        product_sessions.referrer as referrer,
        product_sessions.landing_page as landingPage,
        product_sessions.landing_page_query as landingPageQuery,
        product_sessions.query as query,
        product_sessions.domain as domain,
        product_sessions.path as path,
        product_sessions.title as title,
        product_sessions.channels as channels
FROM
        {{ source ('govideo_production', 'product_sessions')}} as product_sessions
WHERE
        TRUE
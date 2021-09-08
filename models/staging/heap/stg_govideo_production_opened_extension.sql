SELECT
        opened_extension.event_id as eventID,
        opened_extension.user_id as userID,
        opened_extension.session_id as sessionid,
        opened_extension.session_time as sessiontime,
        opened_extension.time as eventTime,
        --opened_extension.continent as continent,
        opened_extension.country as country,
        opened_extension.region as region,
        opened_extension.city as city,
        opened_extension.platform as platform,
        opened_extension.device_type as deviceType,
        opened_extension.browser as browser,
        --opened_extension.browser_type as browserType,
        --opened_extension.vidyard_platform as vidyardPlatform,
        opened_extension.referrer as referrer,
        opened_extension.landing_page as landingPage,
        opened_extension.landing_page_query as landingPageQuery,
        opened_extension.query as query,
        opened_extension.domain as domain,
        opened_extension.path as path,
        opened_extension.title as title,
        opened_extension.channels as channels
FROM
        {{ source('govideo_production' ,'opened_extension') }} as opened_extension
WHERE
        TRUE

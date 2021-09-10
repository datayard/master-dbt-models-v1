SELECT
        vidyard_com_any_pageview.event_id as eventID,
        vidyard_com_any_pageview.session_id as sessionId,
        vidyard_com_any_pageview.session_time as sessionTime,
        vidyard_com_any_pageview.user_id as userID,
        vidyard_com_any_pageview.time as eventTime,
        --vidyard_com_any_pageview.continent as continent,
        vidyard_com_any_pageview.country as country,
        vidyard_com_any_pageview.region as region,
        vidyard_com_any_pageview.city as city,
        vidyard_com_any_pageview.platform as platform,
        vidyard_com_any_pageview.device_type as deviceType,
        vidyard_com_any_pageview.browser as browser,
        --vidyard_com_any_pageview.browser_type as browserType,
        --vidyard_com_any_pageview.vidyard_platform as vidyardPlatform,
        vidyard_com_any_pageview.referrer as referrer,
        vidyard_com_any_pageview.landing_page as landingPage,
        vidyard_com_any_pageview.landing_page_query as landingPageQuery,
        vidyard_com_any_pageview.query as query,
        vidyard_com_any_pageview.domain as domain,
        vidyard_com_any_pageview.path as path,
        vidyard_com_any_pageview.title as title,
        vidyard_com_any_pageview.channels as channels,
        vidyard_com_any_pageview.utm_source as utmSource,
        vidyard_com_any_pageview.utm_medium as utmMedium,
        vidyard_com_any_pageview.utm_campaign as utmCampaign,
        vidyard_com_any_pageview.utm_term as utmTerm,
        vidyard_com_any_pageview.utm_content as utmContent
FROM
        {{ source ('govideo_production' , 'vidyard_com_any_pageview')}} as vidyard_com_any_pageview

WHERE
        TRUE
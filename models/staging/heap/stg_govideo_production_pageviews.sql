SELECT
        pageviews.session_id as sessionid,
        pageviews.session_time as sessiontime,
        pageviews.event_id as eventID,
        pageviews.user_id as userID,
        pageviews.time as eventTime,
        pageviews.country as country,
        pageviews.region as region,
        pageviews.city as city,
        pageviews.platform as platform,
        pageviews.device as device,
        pageviews.device_type as deviceType,
        pageviews.browser as browser,
        pageviews.referrer as referrer,
        pageviews.landing_page as landingPage,
        pageviews.landing_page_query as landingPageQuery,
        pageviews.query as query,
        pageviews.domain as domain,
        pageviews.path as path,
        pageviews.title as title,
        pageviews.utm_source as utmSource,
        pageviews.utm_medium as utmMedium,
        pageviews.utm_campaign as utmCampaign,
        pageviews.utm_term as utmTerm,
        pageviews.utm_content as utmContent
FROM
        {{ source ('govideo_production' , 'pageviews')}} as pageviews
WHERE
        pageviews.time < DATEADD(day, 1, current_date)
SELECT
        about_cta.event_id as eventID,
        about_cta.session_id as sessionId,
        about_cta.session_time as sessionTime,
        about_cta.user_id as userID,
        about_cta.time as eventTime,
        about_cta.country as country,
        about_cta.region as region,
        about_cta.city as city,
        about_cta.platform as platform,
        about_cta.device_type as deviceType,
        about_cta.browser as browser,
        about_cta.referrer as referrer,
        about_cta.landing_page as landingPage,
        about_cta.landing_page_query as landingPageQuery,
        about_cta.query as query,
        about_cta.domain as domain,
        about_cta.path as path,
        isnull(cr.region, 'Other') as gregion
FROM
        {{ source ( 'govideo_production' , 'acquisition_viewed_landing_page_after_right_clicking_player_and_clicking_about') }} as about_cta
        left join {{ source('ops_utility_tables', 'country_names_with_region') }} cr
                on about_cta.country = cr.country_name
WHERE
        about_cta.time < DATEADD(day, 1, current_date)
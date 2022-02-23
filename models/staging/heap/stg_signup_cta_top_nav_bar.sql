SELECT
        signup_header.event_id as eventID,
        signup_header.session_id as sessionId,
        signup_header.session_time as sessionTime,
        signup_header.user_id as userID,
        signup_header.time as eventTime,
        signup_header.country as country,
        signup_header.region as region,
        signup_header.city as city,
        signup_header.platform as platform,
        signup_header.device_type as deviceType,
        signup_header.browser as browser,
        signup_header.referrer as referrer,
        signup_header.landing_page as landingPage,
        signup_header.landing_page_query as landingPageQuery,
        signup_header.query as query,
        signup_header.domain as domain,
        signup_header.path as path,
        isnull(cr.region, ‘Other’) as gregion
FROM
        {{ source ( 'govideo_production' , 'acquisition_clicked_share_page_signup_cta_top_nav_bar_') }} as signup_header
        left join {{ source('ops_utility_tables', 'country_names_with_region') }} cr
                on signup_header.country = cr.country_name
WHERE
        signup_header.time < DATEADD(day, 1, current_date)
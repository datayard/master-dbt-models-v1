SELECT
        inserted_video_from_partner_app.event_id as eventID,
        inserted_video_from_partner_app.session_id as sessionId,
        inserted_video_from_partner_app.session_time as sessionTime,
        inserted_video_from_partner_app.user_id as userID,
        inserted_video_from_partner_app.time as eventTime,
        inserted_video_from_partner_app.country as country,
        inserted_video_from_partner_app.region as region,
        inserted_video_from_partner_app.city as city,
        inserted_video_from_partner_app.platform as platform,
        inserted_video_from_partner_app.device_type as deviceType,
        inserted_video_from_partner_app.browser as browser,
        inserted_video_from_partner_app.referrer as referrer,
        inserted_video_from_partner_app.landing_page as landingPage,
        inserted_video_from_partner_app.landing_page_query as landingPageQuery,
        inserted_video_from_partner_app.query as query,
        inserted_video_from_partner_app.domain as domain,
        inserted_video_from_partner_app.path as path,
        isnull(cr.region, 'Other') as gregion
FROM
        {{ source ( 'govideo_production' , 'sharing_inserted_video_from_partner_app') }} as inserted_video_from_partner_app
        left join {{ source('ops_utility_tables', 'country_names_with_region') }} cr
                on inserted_video_from_partner_app.country = cr.country_name
WHERE
        inserted_video_from_partner_app.time < DATEADD(day, 1, current_date)
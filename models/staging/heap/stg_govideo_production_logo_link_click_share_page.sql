SELECT
        logo_link_click.event_id as eventID,
        logo_link_click.session_id as sessionId,
        logo_link_click.session_time as sessionTime,
        logo_link_click.user_id as userID,
        logo_link_click.time as eventTime,
        logo_link_click.country as country,
        logo_link_click.region as region,
        logo_link_click.city as city,
        logo_link_click.platform as platform,
        logo_link_click.device_type as deviceType,
        logo_link_click.browser as browser,
        logo_link_click.referrer as referrer,
        logo_link_click.landing_page as landingPage,
        logo_link_click.landing_page_query as landingPageQuery,
        logo_link_click.query as query,
        logo_link_click.domain as domain,
        logo_link_click.path as path,
        isnull(cr.region, 'Other') as gregion
FROM
        {{ source ( 'govideo_production' , 'acquisition_viewed_landing_page_after_logo_link_click_share_page_') }} as logo_link_click
        left join {{ source('ops_utility_tables', 'country_names_with_region') }} cr
                on logo_link_click.country = cr.country_name
WHERE
        logo_link_click.time < DATEADD(day, 1, current_date)
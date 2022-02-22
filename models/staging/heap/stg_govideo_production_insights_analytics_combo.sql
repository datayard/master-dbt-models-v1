SELECT
  insights_analytics_combo.event_id as eventID,
  insights_analytics_combo.user_id as userID,
  insights_analytics_combo.session_id as sessionID,
  insights_analytics_combo.time as eventTime,
  insights_analytics_combo.session_time as sessionTime,
  --insights_analytics_combo.continent as continent,
  insights_analytics_combo.country as country,
  insights_analytics_combo.region as region,
  insights_analytics_combo.city as city,
  insights_analytics_combo.platform as platform,
  insights_analytics_combo.device as device,
  insights_analytics_combo.device_type as deviceType,
  insights_analytics_combo.browser as browser,
  --insights_analytics_combo.browser_type as browserType,
  --insights_analytics_combo.vidyard_platform as vidyardPlatform,
  insights_analytics_combo.referrer as referrer,
  insights_analytics_combo.landing_page as landingPage,
  insights_analytics_combo.landing_page_query as landingPageQuery,
  insights_analytics_combo.query as query,
  insights_analytics_combo.domain as domain,
  insights_analytics_combo.path as path,
  insights_analytics_combo.title as title,
  insights_analytics_combo.channels as channels,
  isnull(cr.region, 'Other') as gregion
FROM
  {{ source ('govideo_production' , 'insights_analytics_combo')}} as insights_analytics_combo
  left join {{ source('ops_utility_tables', 'country_names_with_region') }} cr
                on insights_analytics_combo.country = cr.country_name       
WHERE
  insights_analytics_combo.time < DATEADD(day, 1, current_date)
SELECT
  admin_combo.event_id as eventID,
  admin_combo.user_id as userID,
  admin_combo.session_id as sessionID,
  admin_combo.time as eventTime,
  admin_combo.session_time as sessionTime,
  --admin.continent as continent,
  admin_combo.country as country,
  admin_combo.region as region,
  admin_combo.city as city,
  admin_combo.platform as platform,
  admin_combo.device as device,
  admin_combo.device_type as deviceType,
  admin_combo.browser as browser,
  --admin_combo.browser_type as browserType,
  --admin_combo.vidyard_platform as vidyardPlatform,
  admin_combo.referrer as referrer,
  admin_combo.landing_page as landingPage,
  admin_combo.landing_page_query as landingPageQuery,
  admin_combo.query as query,
  admin_combo.domain as domain,
  admin_combo.path as path,
  admin_combo.title as title,
  admin_combo.channels as channels,
  isnull(cr.region, 'Other') as gregion
FROM
  {{ source ('govideo_production' , 'admin_combo') }} as admin_combo
  left join {{ source('ops_utility_tables', 'country_names_with_region') }} cr
    on admin_combo.country = cr.country_name
WHERE
  admin_combo.time < DATEADD(day, 1, current_date)
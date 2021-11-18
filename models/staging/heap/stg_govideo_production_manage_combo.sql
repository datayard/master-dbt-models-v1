SELECT
  manage_combo.event_id as eventID,
  manage_combo.user_id as userID,
  manage_combo.session_id as sessionID,
  manage_combo.time as eventTime,
  manage_combo.session_time as sessionTime,
  --manage_combo.continent as continent,
  manage_combo.country as country,
  manage_combo.region as region,
  manage_combo.city as city,
  manage_combo.platform as platform,
  manage_combo.device as device,
  manage_combo.device_type as deviceType,
  manage_combo.browser as browser,
  --manage_combo.browser_type as browserType,
  --manage_combo.vidyard_platform as vidyardPlatform,
  manage_combo.referrer as referrer,
  manage_combo.landing_page as landingPage,
  manage_combo.landing_page_query as landingPageQuery,
  manage_combo.query as query,
  manage_combo.domain as domain,
  manage_combo.path as path,
  manage_combo.title as title,
  manage_combo.channels as channels
FROM
        {{ source('govideo_production' ,'manage_combo') }} as manage_combo
WHERE
        manage_combo.time < DATEADD(day, 1, current_date)

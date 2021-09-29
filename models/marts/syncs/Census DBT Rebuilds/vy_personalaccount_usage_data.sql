/* {{ config(materialized='table', tags=['publish']) }} */

WITH 
  players as (
    SELECT
      organizationID as org_id,
      COUNT(distinct vy_players.playerID) as players_created,
      MAX(vy_players.createdDate) as last_player_created_date
    FROM
      {{ref('stg_vidyard_players')}} as vy_players
    GROUP BY
      org_id
    ),
  videos as (
    SELECT
      organizationID as org_id,
      COUNT(distinct vy_videos.videoID) as videos_created,
      MAX(vy_videos.createdDate) as last_video_created_date
    FROM
      {{ref('stg_vidyard_videos')}} as vy_videos
    GROUP BY
      org_id
    ),
  chrome_sessions as ( --adding comment for review
    SELECT
      MAX(chrome_extension.eventTime) as last_chromeext_session,
      chrome_extension.userID as chrome_heap_user_id
    FROM
      {{ref('stg_govideo_production_opened_extension')}} as chrome_extension
    GROUP BY
      chrome_heap_user_id
    )

SELECT DISTINCT
  master_users.email as email,
  master_users.vy_user_id as vy_user,
  MAX(prod_sessions.eventTime) as last_product_session,
  CASE
    WHEN 
      account_data.activated ='t'
      AND last_product_session >= dateadd(day,-30,CURRENT_DATE)
      THEN 't'
    WHEN 
      account_data.activated ='t'
      AND last_product_session <= dateadd(day,-30,CURRENT_DATE)
      THEN 'f'
    ELSE null
  END as is_MAU,
  account_data.vy_personalaccount_id,
  players.players_created,
  players.last_player_created_date,
  videos.videos_created,
  videos.last_video_created_date,
  org_metrics.viewsCount,
  org_metrics.videosWithViews,
  chrome_sessions.last_chromeext_session,
  embed_limits.active_embeds,
  embed_limits.embed_limit,
  embed_limits.remaining_embeds,
  org_clients.videos_from_client  + CAST(' in ' AS varchar) + org_clients.client_name as videos_in_most_used_client
FROM
  {{ ref('user_id_master') }} master_users
LEFT JOIN
  {{ref('stg_govideo_production_product_sessions')}} as prod_sessions
ON
  master_users.heap_id = prod_sessions.userID
JOIN
  {{ ref('vy_personalaccount_object_data') }} account_data
ON
  account_data.vy_user_id = master_users.vy_user_id
LEFT JOIN
  chrome_sessions
ON
  master_users.heap_id = chrome_sessions.chrome_heap_user_id
LEFT JOIN
  players
ON
  players.org_id = account_data.vy_personalaccount_id
LEFT JOIN
  videos
ON
  videos.org_id = account_data.vy_personalaccount_id
LEFT JOIN
  {{ref('stg_vidyard_org_metrics')}} as org_metrics
ON
  org_metrics.organizationID = account_data.vy_personalaccount_id
LEFT JOIN
  {{ref ('most_used_clients')}} as org_clients
ON
  org_clients.organizationID = account_data.vy_personalaccount_id
LEFT JOIN
  {{ref('calculated_embed_limits')}} as embed_limits
ON
  master_users.vy_user_id = embed_limits.user_id
GROUP BY
  master_users.email,
  master_users.vy_user_id,
  account_data.vy_personalaccount_id,
  players_created,
  account_data.activated,
  last_player_created_date,
  videos_created,
  last_video_created_date,
  org_metrics.viewsCount,
  org_metrics.videosWithViews,
  last_chromeext_session,
  org_clients.client_name,
  org_clients.videos_from_client,
  embed_limits.active_embeds,
  embed_limits.embed_limit,
  embed_limits.remaining_embeds
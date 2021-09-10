SELECT DISTINCT
  mkto_lead.id as mkto_lead_id,
  mkto_lead.email,
  usage_data.vy_user,
  usage_data.vy_personalaccount_id,
  usage_data.is_mau,
  usage_data.players_created,
  usage_data.last_player_created_date,
  usage_data.videos_created,
  usage_data.last_video_created_date,
  usage_data.views_count,
  usage_data.videos_with_views,
  usage_data.last_product_session,
  usage_data.videos_in_most_used_client,
  usage_data.embed_limit,
  usage_data.remaining_embeds,
  usage_data.last_chromeext_session
FROM
  census.vy_personalaccount_usage_data as usage_data
JOIN
  census.user_id_master as master_user
ON
  usage_data.vy_user = master_user.vy_user_id
JOIN
  marketo_production.lead as mkto_lead
ON
  mkto_lead.id = master_user.mkto_lead_id
WHERE 
  master_user.mkto_lead_id IS NOT null
--Excluding users without a Mkto_lead it to avoid creating new ones 
{% macro grant_permission_to_looker() %}

    ----------------------SALESFORCE-----------------------

GRANT USAGE ON SCHEMA salesforce_production TO looker;
GRANT SELECT ON 
	salesforce_production.account, 
    salesforce_production.campaign, 
    salesforce_production.contact, 
    salesforce_production.lead, 
    salesforce_production.opportunity, 
    salesforce_production.user 
TO looker;
        
----------------------VIDYARD-----------------------    

GRANT USAGE ON SCHEMA public TO looker;
GRANT SELECT ON 
  public.vidyard_active_features,
  public.vidyard_allotment_limits,
  public.vidyard_allotment_types,
  public.vidyard_event_joins,
  public.vidyard_events,
  public.vidyard_features,
  public.vidyard_hubs,
  public.vidyard_nps_surveys,
  public.vidyard_org_metrics,
  public.vidyard_organizations,
  public.vidyard_players,
  public.vidyard_team_memberships,
  public.vidyard_teams,
  public.vidyard_user_groups,
  public.vidyard_users,
  public.vidyard_videos
TO looker;

----------------------ZUORA-------------------------
        
GRANT USAGE ON SCHEMA zuora TO looker;
GRANT SELECT ON 
  zuora.account,
  zuora.contact,
  zuora.product,
  zuora.product_rate_plan,
  zuora.rate_plan,
  zuora.subscription
TO looker;

----------------GOVIDEO_PRODUCTION------------------

GRANT USAGE ON SCHEMA govideo_production TO looker;
GRANT SELECT ON 
  govideo_production.global_session,
  govideo_production.opened_extension,
  govideo_production.pageviews,
  govideo_production.product_sessions,
  govideo_production.users,
  govideo_production.video_creation_started_to_create_or_upload_a_video_combo,
  govideo_production.video_recorded_or_uploaded,
  govideo_production.vidyard_com_any_pageview,
  govideo_production.vidyard_com_sessions
TO looker;

----------------DBT_VIDYARD_MASTER------------------

GRANT USAGE ON SCHEMA dbt_vidyard_master TO looker;
GRANT SELECT ON 
  dbt_vidyard_master.stg_salesforce_account,
  dbt_vidyard_master.stg_salesforce_campaign,
  dbt_vidyard_master.stg_salesforce_contact,
  dbt_vidyard_master.stg_salesforce_lead,
  dbt_vidyard_master.stg_salesforce_opportunity,
  dbt_vidyard_master.stg_salesforce_campaignmember,
  
  dbt_vidyard_master.stg_vidyard_active_features,
  dbt_vidyard_master.stg_vidyard_allotment_limits,
  dbt_vidyard_master.stg_vidyard_allotment_types,
  dbt_vidyard_master.stg_vidyard_event_joins,
  dbt_vidyard_master.stg_vidyard_events,
  dbt_vidyard_master.stg_vidyard_features,
  dbt_vidyard_master.stg_vidyard_hubs,
  dbt_vidyard_master.stg_vidyard_nps_surveys,
  dbt_vidyard_master.stg_vidyard_org_metrics,
  dbt_vidyard_master.stg_vidyard_organizations,
  dbt_vidyard_master.stg_vidyard_players,
  dbt_vidyard_master.stg_vidyard_team_memberships,
  dbt_vidyard_master.stg_vidyard_teams,
  dbt_vidyard_master.stg_vidyard_user_groups,
  dbt_vidyard_master.stg_vidyard_users,
  dbt_vidyard_master.stg_vidyard_videos,
  
  dbt_vidyard_master.stg_zuora_account,
  dbt_vidyard_master.stg_zuora_contact,
  dbt_vidyard_master.stg_zuora_product,
  dbt_vidyard_master.stg_zuora_product_rate_plan,
  dbt_vidyard_master.stg_zuora_rate_plan,
  dbt_vidyard_master.stg_zuora_subscription,
  
  dbt_vidyard_master.stg_govideo_production_global_session,
  dbt_vidyard_master.stg_govideo_production_opened_extension,
  dbt_vidyard_master.stg_govideo_production_pageviews,
  dbt_vidyard_master.stg_govideo_production_product_sessions,
  dbt_vidyard_master.stg_govideo_production_users,
  dbt_vidyard_master.stg_govideo_production_video_creation_started_to_create_or_upload_a_video_combo,
  dbt_vidyard_master.stg_govideo_production_video_recorded_or_uploaded,
  dbt_vidyard_master.stg_govideo_production_vidyard_com_any_pageview,
  dbt_vidyard_master.stg_govideo_production_vidyard_com_sessions
  
TO looker;


{% endmacro %}


{% macro grant_permission_to_dbt() %}

    ----------------------SALESFORCE-----------------------

    GRANT USAGE ON SCHEMA salesforce_production TO vidyarddbt;
    GRANT SELECT ON 
        salesforce_production.account, 
        salesforce_production.campaign, 
        salesforce_production.contact, 
        salesforce_production.lead, 
        salesforce_production.opportunity, 
        salesforce_production.user 
    TO vidyarddbt;
            
    ----------------------VIDYARD-----------------------    

    GRANT USAGE ON SCHEMA public TO vidyarddbt;
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
    TO vidyarddbt;

    ----------------------ZUORA-------------------------
            
    GRANT USAGE ON SCHEMA zuora TO vidyarddbt;
    GRANT SELECT ON 
        zuora.account,
        zuora.contact,
        zuora.product,
        zuora.product_rate_plan,
        zuora.rate_plan,
        zuora.subscription
    TO vidyarddbt;

    ----------------DBT_VIDYARD_MASTER------------------

    GRANT USAGE ON SCHEMA dbt_vidyard_master TO vidyarddbt;
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
        dbt_vidyard_master.stg_zuora_subscription
    TO vidyarddbt;


{% endmacro %}
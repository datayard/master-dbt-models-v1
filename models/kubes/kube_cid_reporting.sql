with 
    vidyard_users_duplicate as (
        select 
            userid
            , organizationid
            , name
            , email
            , domainType
            , combined_usecase
            , classification
            , createddate
            , row_number() over(partition by userid order by createddate asc) as rn
        from
            {{ ref('kube_vidyard_user_details')}}
    ),

    vidyard_users_unique as(
        select 
            userid
            , organizationid
            , name
            , email
            , domainType
            , combined_usecase
            , classification
            , createddate
        from 
            vidyard_users_duplicate
        where 
            rn = 1
    ),

    role_duplicate as (
        select
            userid
            , isadmin
            , row_number() over(partition by userid order by createddate asc) as rn
        from 
            {{ ref('tier2_vidyard_team_memberships')}}
        where 
            isadmin = 'true'
    ),

    role_unique as (
        select 
            userid
            , isadmin
        from 
            role_duplicate
        where 
            rn = 1 
    ),

    salesforce_duplicate as (
        select
            vidyardUserId
            , accountid
            , primaryUseCase
            , customerTier
            , accountIndustry
            , numberOfEmployees
            , employeeSegment
            , crmPlatform
            , marketingAutomationPlatform
            , salesProspectingTool
            , row_number() over(partition by vidyardUserId order by createdDate asc) as rn
        from 
            {{ ref('tier2_salesforce_account')}}
    ),

    salesforce_unique as (
        select
            vidyardUserId
            , accountid
            , primaryUseCase
            , customerTier
            , accountIndustry
            , numberOfEmployees
            , employeeSegment
            , crmPlatform
            , marketingAutomationPlatform
            , salesProspectingTool
        from 
            salesforce_duplicate
        where
            rn = 1
    ),

    admin_combo as (
        select 
            distinct vidyardUserId
            , count(distinct eventid) as PB_admin
        from 
            {{ ref('tier2_heap')}}
        where 
            tracker = 'admin_combo'
            and datediff(day,eventtime,getdate()) between 0 and 60 
        group by 
            1
        order by 
            2 
            desc
    ),

    analytics_combo as (
        select 
            distinct vidyardUserId
            , count(distinct eventid) as PB_analytics
        from 
            {{ ref('tier2_heap')}}
        where 
            tracker = 'insights_analytics_combo'
            and datediff(day,eventtime,getdate()) between 0 and 60 
        group by 
            1
        order by 
            2 
            desc
    ),

    share_combo as (
        select 
            distinct vidyardUserId
            , count(distinct eventid) as PB_share
        from 
            {{ ref('tier2_heap')}}
        where 
            tracker = 'sharing_share_combo'
            and datediff(day,eventtime,getdate()) between 0 and 60 
        group by 
            1
        order by 
            2 
            desc
    ),

    manage_combo as (
        select 
            distinct vidyardUserId
            , count(distinct eventid) as PB_manage
        from 
            {{ ref('tier2_heap')}}
        where 
            tracker = 'manage_combo'
            and datediff(day,eventtime,getdate()) between 0 and 60 
        group by 
            1
        order by 
            2 
            desc
    ),

    create_combo as (
        select 
            distinct vidyardUserId
            , count(distinct eventid) as PB_create
        from 
            {{ ref('tier2_heap')}}
        where 
            tracker = 'video_creation_create_combo'
            and datediff(day,eventtime,getdate()) between 0 and 60 
        group by 
            1
        order by 
            2 
            desc
    )

select 
    vuu.userid as userid
    , vuu.name as org_name
    , vuu.email as email
    , vuu.domainType as domaintype
    , vuu.combined_usecase as heap_usecase
    , vuu.classification as price_tier
    , vuu.createddate as user_createddate
    , ru.isadmin as isadmin
    , su.accountid as salesforce_accountid
    , su.primaryUseCase as salesforce_usecase
    , su.customerTier as customer_tier
    , su.accountIndustry as account_industry
    , su.numberOfEmployees as employee_count
    , su.employeeSegment as sales_segment
    , su.crmPlatform as crm_platform
    , su.salesProspectingTool as sales_prospecting_tool
    , su.marketingAutomationPlatform as marketing_automation_platform
    , adc.PB_admin as PBC_admin
    , anc.PB_analytics as PBC_analytics
    , sc.PB_share as PBC_share
    , mc.PB_manage as PBC_manage
    , cc.PB_create as PBC_create

from 
    vidyard_users_unique as vuu 

        left join role_unique as ru 
            on vuu.userid = ru.userid

        left join salesforce_unique as su 
            on vuu.userid = su.vidyardUserId

        left join admin_combo as adc
            on vuu.userid = adc.vidyardUserId

        left join analytics_combo as anc 
            on vuu.userid = anc.vidyardUserId

        left join share_combo as sc 
            on vuu.userid = sc.vidyardUserId
        
        left join manage_combo as mc 
            on vuu.userid = mc.vidyardUserId

        left join create_combo as cc 
            on vuu.userid = cc.vidyardUserId
with lead_contact as (
    select
        distinct u.createddate               as mqi_date
        ,u.email                             as email
        ,u.domaintype                        as domaintype
        ,case
              when u.leadsource in ('Intercom', 'Product') and u.acquisitationprogram is null
                  then 'Product'
              when u.acquisitationprogram is not null then 'Marketing'
        end                                  as acquisition_source
        ,u.leadid                            as leadcontactid
        ,'Lead'                              as userType
        ,u.acquisitationprogram              as acquisitation_program
        ,u.status                            as status
        ,u.leadid as id
    from {{ ref('tier2_salesforce_lead') }} as u
    where u.isconverted = 'false'
    union
    select
        distinct u.createddate                as mqi_date
        , u.email                             as email
        , u.domaintype                        as domaintype
        , case
              when u.leadsource in ('Intercom', 'Product') and u.acquisitationprogram is null
                  then 'Product' 
              when u.acquisitationprogram is not null then 'Marketing'
        end                                  as acquisition_source
        , u.contactid                        as leadcontactid
        , 'Contact'                          as userType
        , u.acquisitationprogram             as acquisitation_program
        , u.contactstatus                    as status
        ,u.contactid as id
    from {{ ref('tier2_salesforce_contact') }} as u
)
,campaignmembers_leadscontacts as (
         select
             distinct cm.createddategmt       as mqi_date
            ,u.email                          as email
            ,u.domaintype                     as domaintype
            ,case
                  when cm.campaignsourcedby is null then 'Other'else cm.campaignsourcedby
                end                           as acquisition_source
            ,cm.campaign_parentid             as campaign_parentid
            ,cm.campaignid                    as campaignid
            ,cm.campaignmemberid              as campaignmember_id
         from {{ ref('tier2_salesforce_lead') }} as u
         join {{ ref('tier2_salesforce_campaign_and_members') }} as cm on
             cm.leadid = u.leadid
         where u.isconverted = 'false'
         and (campaign_parentid NOT IN ('7014O000001m7h7QAA', '7014O000001m4XEQAY') or campaign_parentid is null)
         union
         select
             distinct cm.createddategmt          as mqi_date
             , u.email                           as email
             , u.domaintype                      as domaintype
             , case
                   when cm.campaignsourcedby is null then 'Other' else cm.campaignsourcedby
                 end                            as acquisition_source
            , cm.campaign_parentid              as campaign_parentid
            , cm.campaignid                     as campaignid
            , cm.campaignmemberid               as campaignmember_id
         from {{ ref('tier2_salesforce_contact') }} as u
         join {{ ref('tier2_salesforce_campaign_and_members') }} as cm on
             cm.contactid = u.contactid
         where (campaign_parentid NOT IN ('7014O000001m7h7QAA', '7014O000001m4XEQAY') or campaign_parentid is null)
     )
     ,nnns as (
         select
                mqi_date
              , email
              , domaintype
              , acquisition_source
         from lead_contact
         where acquisition_source is not null

         union

         select mqi_date
              , email
              , domaintype
              , acquisition_source
         from campaignmembers_leadscontacts

         union

         select
             distinct createddate         as mqi_date
             , email
             , domaintype
             ,case when createdbyclientid ='Enterprise' then 'Assigned Paid Seat' else
              'Signups - Product'  end      as acquisition_source
         from dbt_vidyard_master.kube_vidyard_user_details
         where excludeemail = 0
         and signupsource != 'Hubspot'
     )
     ,ordered_nnns as (
         select n.*
              , row_number() over (partition by n.email order by n.mqi_date asc) as rn
         from nnns n
     )
         select mqi_date
             , email
             , domaintype
             , acquisition_source
        from ordered_nnns as n
        where n.rn = 1
SELECT
    --USER
    vut2.userid
    , email
    , email_to_exclude
    , vut2.domain
    , domain_type

    --ORGANIZATION
    , vut2.organizationid
    , ownerid
    , vut2.accountid as org_accountid
    , orgtype
    , parentid
    , org_name
    , vut2.createddate as org_createddate
    , vut2.updateddate as org_updateddate
    , vut2.createdbyclientid as org_createdbyclientid
    , paying
    , signup_source

    --ORG_METRICS
    , firstview
    , firstviewvideoid
    , totalseconds
    , videoswithviews
    , viewscount

    --GROUP & TEAM
    , groupid
    , teamid

    --CALCULATED FIELDS
    , has_access_to_enterprise_account
    , has_personal_account
    , linked_to_parent_account
    , user_type
    --, account_type

    --ZUORA
  /*, subscriptionid
    , subscriptionstartdate
    , subscriptionenddate
    , zuoraaccountid
    , zuoracrmid
    , rateplanid
    , productrateplanid
    , productid
    , net_mrr
    , subscriptionstatus
    , soldtocontactid
  */
    --HEAP
    , eventid
    --, vid_userid
    , ht2.userid AS heap_userid
    , sessionid
    , sessiontime
    , eventtime
    , landingpage
    , ht2.domain as heap_domain
    , channels
    , path
    , derived_channel
    , column_for_distribution
    , tracker

FROM --{{ ref('kube_vidyard_zuora_users') }} kbvzu
    {{ ref('tier2_vidyard_users') }} vut2
    LEFT JOIN {{ ref('tier2_heap') }} ht2
        ON  ht2.vy_userid = vut2.userid         
            AND ht2.is_vy_userid_integer = 1
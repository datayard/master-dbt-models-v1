SELECT
    --USER
    kbvzu.userid
    , email
    , email_to_exclude
    , kbvzu.domain
    , domain_type

    --ORGANIZATION
    , kbvzu.organizationid
    , ownerid
    , org_accountid
    , orgtype
    , parentid
    , org_name
    , org_createddate
    , org_updateddate
    , org_createdbyclientid
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
    , account_type

    --ZUORA
    , subscriptionid
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

FROM {{ ref('kube_vidyard_zuora_users') }} kbvzu
        LEFT JOIN {{ ref('tier2_heap') }} ht2
            ON  ht2.vy_userid = kbvzu.userid 
                AND ht2.is_vy_userid_integer = 1
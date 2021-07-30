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
    , account_type --PARAMETER COMPUTED FROM ZUORA INFO

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

    --VIDYARD USER ENTITY
    , entity
    --, userid
    --, organizationid
    , entityid
    , childentityid
    , vyent.createddate
    , vyent.updateddate
    , type
    , vyent.createdbyclientid
    , uuid
    , origin
    , derived_origin
    , source
    , status
    , issecure
    , milliseconds
    , inviteaccepted
    , userscore
    , usercomment
    , filled
    , allowcontact
    , cancelled

FROM {{ ref('kube_vidyard_zuora_users') }} kbvzu
            LEFT JOIN {{ ref('tier2_vidyard_user_entities') }} vyent
                    ON vyent.userid = kbvzu.userid AND vyent.organizationid = kbvzu.organizationid
SELECT
    --USER
    vut2.userid
    , email
    , excludeEmail
    , vut2.domain
    , domainType

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
    --, account_type --PARAMETER COMPUTED FROM ZUORA INFO

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
    , derivedOrigin
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

FROM --{{ ref('kube_vidyard_zuora_users') }} kbvzu
    {{ ref('tier2_vidyard_users') }} vut2
    LEFT JOIN {{ ref('tier2_vidyard_user_entities') }} vyent
        ON vyent.userid = vut2.userid AND vyent.organizationid = vut2.organizationid
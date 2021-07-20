SELECT 
         --USER
        kbvzu.userid
        , email
        , vidyard_email
        , viewedit_email
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

         --Entity
         , entity
         , entityid
         , childentityid
         , createddate
         , updateddate
         , type
         , createdbyclientid
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
         , eventid
         , vid_userid
         , ht2.userid AS heapuserid
         , sessionid
         , sessiontime
         , eventtime
         , landingpage
         , domain
         , channels
         , path
         , derived_channel
         , column_for_distribution
         , tracker
         
    FROM {{ ref('kube_vidyard_user_entities') }} kbvzu
             JOIN {{ ref('tier2_heap') }} ht2
                  ON ht2.vid_userid = kbvzu.userid
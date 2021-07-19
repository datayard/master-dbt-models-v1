SELECT
    --USER
    vut2.userid
    , email
    , vidyard_email
    , viewedit_email
    , domain_type

    --ORGANIZATION
    , vut2.organizationid
    , ownerid
    , vut2.accountid as org_accountid
    , orgtype
    , parentid
    , org_name
    , vut2.createddate AS org_createddate
    , vut2.updateddate AS org_updateddate
    , vut2.createdbyclientid AS org_createdbyclientid
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
    , vut2.user_type

    --ZUORA
    , zt2.subscriptionid
    , zt2.subscriptionstartdate
    , zt2.subscriptionenddate
    , zt2.accountid as zuoraaccountid
    , zt2.crmid as zuoracrmid
    , zt2.rateplanid
    , zt2.productrateplanid
    , zt2.productid
    , zt2.net_mrr
    , zt2.status as subscriptionstatus
    , zt2.soldtocontactid

    --CALCULATED FIELD
    , CASE
        --FREE
        WHEN vut2.userid = vut2.ownerid
            AND vut2.organizationid = vut2.accountid
            AND vut2.orgtype = 'self_serve'
            AND zt2.vidyardid is null
            THEN 'Free'

        --PRO - ACTIVE
        WHEN vut2.userid = vut2.ownerid
            AND vut2.organizationid = vut2.accountid
            AND vut2.orgtype = 'self_serve'
            AND zt2.sku = 'SS-010'
            AND zt2.status = 'Active'
            AND zt2.vidyardid is not null
            THEN 'Pro'

        --PRO - CANCELLED BUT END_DATE > CUR_DATE
        WHEN vut2.userid = vut2.ownerid
            AND vut2.organizationid = vut2.accountid
            AND vut2.orgtype = 'self_serve'
            AND zt2.sku = 'SS-010'
            AND zt2.status = 'Cancelled'
            AND zt2.termenddate >= getdate()
            AND zt2.vidyardid is not null
            THEN 'Pro'

        -- Self Serve Enterprise
        WHEN vut2.userid = vut2.ownerid
            AND vut2.organizationid != vut2.accountid
            AND vut2.orgtype = 'self_serve'
            AND vut2.has_access_to_enterprise_account = 1
            AND zt2.vidyardid is not null
            --TODO: LOGIC TO INCLUDE SELF SERVE COMPANY IDS GOES HERE
            THEN 'Self Serve Enterprise'

        -- Enterprise
        WHEN vut2.userid = vut2.ownerid
            AND vut2.organizationid != vut2.accountid
            AND vut2.orgtype = 'self_serve'
            AND vut2.has_access_to_enterprise_account = 1
            AND zt2.vidyardid is null
            THEN 'Enterprise'

        -- TODO - 8K+ ACCOUNTS FALL INTO THIS - NEEDS CHECK
        ELSE null

    END AS account_type
FROM
    {{ ref('tier2_vidyard_users') }} vut2
    LEFT JOIN {{ ref('tier2_zuora') }} zt2
        ON zt2.vidyardid = vut2.organizationid
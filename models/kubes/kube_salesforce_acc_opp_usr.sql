SELECT 
    -- ACCOUNT FIELDS
    accountid
    , csmownerid
    , vidyarduserid
    , createddate
    , dateofchurn
    , accountname
    , ispersonaccount
    , accounttype
    , customertier
    , churnreason
    , churnreasondetails

    --OPPORTUNITY FIELDS
    , opportunityid
    , stagename
    , opportunitytype
    , campaignid
    , newarr
    , newacv
    , oneTimeCharge
    , lastYearARR
    , renewalAmount
    , renewalwonarr
    , renewallostarr
    , reoccurringmrr

    -- USER FIELDS
    , userid
    , fullname
FROM 
    {{ ref('tier2_salesforce_account') }} as a
    JOIN {{ ref('tier2_salesforce_opportunity') }} as o ON o.accountid = a.accountid
    LEFT JOIN {{ ref('tier2_salesforce_user') }} as u ON u.userid = o.ownerid
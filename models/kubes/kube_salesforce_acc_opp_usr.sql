SELECT 
    -- ACCOUNT FIELDS
    a.accountid
    , a.csmownerid
    , a.vidyarduserid
    , a.createddate
    , a.dateofchurn
    , a.accountname
    , a.ispersonaccount
    , a.accounttype
    , a.customertier
    , a.churnreason
    , a.churnreasondetails
    , a.arr

    --OPPORTUNITY FIELDS
    , o.opportunityid
    , o.stagename
    , o.opportunitytype
    , o.campaignid
    , o.newarr
    , o.newacv
    , o.oneTimeCharge
    , o.lastYearARR
    , o.renewalAmount
    , o.renewalwonarr
    , o.renewallostarr
    , o.reoccurringmrr
    , o.renewalduedate
    , o.closedate
    , o.iswon
    , o.businessunit

    -- USER FIELDS
    , u.userid
    , u.fullname
FROM 
    {{ ref('tier2_salesforce_account') }} as a
    JOIN {{ ref('tier2_salesforce_opportunity') }} as o ON o.accountid = a.accountid
    LEFT JOIN {{ ref('tier2_salesforce_user') }} as u ON u.userid = o.ownerid
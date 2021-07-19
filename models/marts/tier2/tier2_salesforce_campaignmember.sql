SELECT
           campaignMemberId
           , createdDate
           , isDeleted
           , campaignId
           , type
           , leadId
           , contactId
           , status
           , name
           , email
           , campaignSourcedBy
           , mql
           , mqlDate
           , sal
           , salDate
           , sql
           , sqlDate
           , sqo
           , sqoDate
           , sqoLost
           , sqoLostDate
           , won
           , opportunityClosedWonDate
           , opportunityId
           , statusReason
           , currentStatusReason

    FROM 
        {{ ref('stg_salesforce_campaignmember') }}
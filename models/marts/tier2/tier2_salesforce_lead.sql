SELECT leadId
         , isDeleted
         , accountId
         , vidyardUserId
         , isConverted
         , convertedContactId
         , createdDate
         , convertedDate
         , leadOwnerId
         , lastName
         , firstName
         , name
         , email
         , title
         , role
         , company
         , industry
         , leadSource
         , leadType
         , status
         , statusReason
         , reasonUnqualified
         , ballerScore
         , acquisitationprogram
         , domainType
         , persona
         , numberofemployees
         , domain
         , excludeemail
        

    FROM
        {{ ref('stg_salesforce_lead') }}

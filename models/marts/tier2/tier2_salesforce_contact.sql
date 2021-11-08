SELECT contactId
         , isDeleted
         , accountId
         , vidyardUserId
         , createdDate
         , accountOwnerId
         , ownerId
         , lastName
         , firstName
         , name
         , email
         , title
         , role
         , jobRole
         , department
         , accountLeadType
         , leadSource
         , ballerScore
         , contactStatus
         , statusReason
         , mainContact
         , marketingAutomationPlatform
         , crm
         , mailingStreet
         , mailingCity
         , mailingState
         , mailingPostalCode
         , mailingCountry
         , domainType
         , persona
         ,acquisitationprogram
    FROM 
        {{ ref('stg_salesforce_contact') }}
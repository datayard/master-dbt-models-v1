SELECT accountId
         , accountName
         , isDeleted
         , accountType
         , parentAccountId
         , accountPhone
         , accountIndustry
         , employeeSegment
         , accountSegment
         , numberOfEmployees
         , annualRevenue
         , ownerId
         , customerTier
         , emailDomain
         , website
         , vidyardAccountId
         , originalContractDate
         , csmOwnerId
         , isPersonAccount
         , isSelfServe
         , billingStreet
         , billingCity
         , billingState
         , billingPostalCode
         , billingCountry
         , isPartner
         , dateOfChurn
         , churnReason
         , churnReasonDetails
         , partnerType
         , vidyardAccountProducts
         , createdDate
         , accountTerritory
         , accountRegion
         , nonContract
         , vidyardUserId
         , crmPlatform
         , marketingAutomationPlatform
         , abmTier
         , onlineVideoPlatform
         , primaryUseCase
         , qaStatus
         , engagioStatus
         , arr
         , partnerManager
         , subType
         , partnerTier
         , partnerDesignation
         , partnerStackStatus
         , partnerStackID
         , salesProspectingTool  
         , notes 
         , daysSinceLastActivityAccount     
    FROM 
        {{ ref('stg_salesforce_account') }}
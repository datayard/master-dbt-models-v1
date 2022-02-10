SELECT opportunityId
         , accountId
         , ownerId
         , opportunityName
         , stageName
         , closeDate
         , opportunityType
         , isClosed
         , isWon
         , campaignId
         , createdDate as createddatewithtimezone
         , cast(createdDate as date)
         , enteredPipelineDate
         , deadReason
         , closedWonDate
         , deadDate
         , renewalDueDate
         , multiYear1stDueDate
         , multiYear2ndDueDate
         , contactChampionId
         , businessUnit
         , opportunityAttribution
         , newacv
         , newARR
         , oneTimeCharge
         , lastYearARR
         , renewalAmount
         , renewalwonarr
         , renewallostarr
         , reoccurringmrr
         , csmOwnerId
         , contractStartDate
         , contractEndDate
         , previousContractStartDate
         , previousContractEndDate
         , churnReason
         , competitor
         , forecastCategory
         , redOpportunity
         , Assurance
         , useCase
         , partnerAccountID
         , partnerBuyingRelationship
         , partnerPercentage
         , partnerStackID
         , partnerStackLeadID
         , partnerStackPartnerID
         , partnerRep    
         , daysInStage1
         , daysInStage2
         , daysInStage3
         , daysInStage4
         , daysInStage5
         , daysInStage6
         , daysInStageCustom   
         , preQualificationDate
         , initiationDate
         , valuePropositionDate
         , solutionEvaluationDate
         , DecisionDate
         , negotiationDate 
         , probability 
         , originatingContactId                  
    FROM 
        {{ ref('stg_salesforce_opportunity') }}
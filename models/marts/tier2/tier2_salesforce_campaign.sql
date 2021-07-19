SELECT campaignId
         , isDeleted
         , name
         , parentId
         , type
         , status
         , startDate
         , endDate
         , expectedRevenue
         , budgetedCost
         , actualCost
         , isActive
         , description
         , createdDate
         , mediaType
         , inboundVsOutbound
         , cta
         , ctaSubtype
         , channelPicklist
         , channelDetail
         , channelMediaType
         , campaignSourcedBy
         , responseType
    FROM 
        {{ ref('stg_salesforce_campaign') }}
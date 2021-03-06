SELECT 
    sfdc_campaign.id as campaignId
   , sfdc_campaign.isdeleted as isDeleted
   , sfdc_campaign.name as campaignName
   , sfdc_campaign.parentid as parentId
   , sfdc_campaign.type as type
   , sfdc_campaign.status as status
   , sfdc_campaign.startdate as startDate
   , sfdc_campaign.enddate as endDate
   , sfdc_campaign.expectedrevenue as expectedRevenue
   , sfdc_campaign.budgetedcost as budgetedCost
   , sfdc_campaign.actualcost as actualCost
   , sfdc_campaign.isactive as isActive
   , sfdc_campaign.description as description
	, sfdc_campaign.createddate as createdDateGMT
	, convert_timezone('EST', sfdc_campaign.createddate::timestamp) as createdDateEST
   , sfdc_campaign.media_type__c as mediaType
   , sfdc_campaign.inbound_vs_outbound__c as inboundVsOutbound
   , sfdc_campaign.cta__c as cta
   , sfdc_campaign.cta_subtype__c as ctaSubtype
   , sfdc_campaign.channel_picklist__c as channelPicklist
   , sfdc_campaign.channel_detail__c as channelDetail
   , sfdc_campaign.channel_media_type__c as channelMediaType
   , sfdc_campaign.campaign_sourced_by__c as campaignSourcedBy
   , sfdc_campaign.response_type__c as responseType 
 FROM 
    {{ source('salesforce_production', 'campaign') }}  as sfdc_campaign
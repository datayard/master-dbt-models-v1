SELECT
    ah.id as adId,
    ah.campaign_id as campaignId,
    ah.creative_id as creativeId,
    ah.created_time as createdTime,
    ah.name as adName,
    ah.ad_set_id as adSetId,
    ah.ad_source_id as adSourceId

FROM 
	{{ source('facebook_ads', 'ad_history') }} as ah
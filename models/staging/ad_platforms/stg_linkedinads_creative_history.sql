SELECT
    ch.id as creativeId,
    ch.campaign_id as campaignId,
    ch.created_time as createdTime,
    ch.status 

FROM 
	{{ source('linkedin_ads', 'creative_history') }} as ch
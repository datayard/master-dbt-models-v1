SELECT
    ch.id as campaignId,
    ch.name as campaignName,
    ch.type as campaignType,
    ch.tracking_template as trackingTemplate

FROM 
	{{ source('bingads', 'campaign_history') }} as ch
SELECT
    ch.id as campaignId,
    ch.name as campaignName,
    ch.type as campaignType,
    ch.tracking_template as tracking_template

FROM 
	{{ source('bingads', 'campaign_history') }} as ch
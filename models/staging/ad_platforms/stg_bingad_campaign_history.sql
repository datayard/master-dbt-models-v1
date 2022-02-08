SELECT
    ch.id as campaignId,
    ch.name as campaignName,
    ch.type as campaignType,
    ch.tracking_template as trackingTemplate,
    ch.type as test_column
FROM 
	{{ source('bingads', 'campaign_history') }} as ch
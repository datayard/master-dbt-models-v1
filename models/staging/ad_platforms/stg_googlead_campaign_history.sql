SELECT
    ch.id as campaignId,
    ch.name as campaignName,
    ch.start_date as startDate,
    ch.end_date as endDate,
    ch.campaign_trial_type as trailType,
    ch.base_campaign_id as basecampaignId,
    ch.advertising_channel_type as channelType,
    ch.tracking_url_template as trackingTemplate
    
FROM 
	{{ source('google_adwords', 'campaign_history') }} as ch
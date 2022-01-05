SELECT
    ch.id as campaignId,
    ch.name as campaignName,
    ch.start_date as startDate,
    ch.end_date as endDate,
    ch.campaign_trial_type as trailType
    
FROM 
	{{ source('google_adwords', 'campaign_history') }} as ch
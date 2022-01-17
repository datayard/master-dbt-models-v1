SELECT
    laac.creative_id as creativeId,
    laac.day as date,
    laac.impressions as impressions,
    laac.clicks as clicks,
    laac.cost_in_usd as costInUSD,
    laac.follows,
    laac.likes,
    laac.opens,
    laac.shares,
    laac.total_engagements
    

FROM 
	{{ source('linkedin_ads', 'ad_analytics_by_creative') }} as laac
SELECT
    dp.campaign_id as campaignId,
    dp.impressions as dailyImpressions,
    dp.clicks as dailyClicks,
    dp.spend as dailySpend,
    dp.conversions as dailyConversions,
    dp.network,
    dp.custom_parameters,
    dp.date

FROM 
	{{ source('bingads', 'campaign_performance_daily_report') }} as dp
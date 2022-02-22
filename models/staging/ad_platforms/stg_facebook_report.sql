SELECT
    fr.ad_id as adId,
    fr.date,
    fr.impressions as dailyImpressions,
    fr.inline_link_clicks as dailyInlineClicks,
    fr.reach as dailyReach,
    fr.cpc as dailyCpc,
    fr.spend as dailySpend,
    fr.ad_name,
    fr.adset_name,
    fr.campaign_name

FROM 
	{{ source('facebook_ads', 'facebook_report') }} as fr
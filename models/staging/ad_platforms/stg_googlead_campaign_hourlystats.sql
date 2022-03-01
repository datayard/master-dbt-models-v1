SELECT
    campaignstat.date as date,
    campaignstat.quarter as quarter,
    campaignstat.ad_network_type_1 as networkType1,
    campaignstat.month_of_year as month_of_year,
    campaignstat.ad_network_type_2 as networkType2,
    campaignstat.hour_of_day as hour_of_day,
    campaignstat.cost as cost,
    campaignstat.month as month,
    campaignstat.campaign_id as campaignID,
    campaignstat.day_of_week as day_of_week,
    campaignstat.impressions as impressions,
    campaignstat.device as device,
    campaignstat.year as year,
    campaignstat.interactions as interactions,
    campaignstat.clicks as clicks,
    campaignstat.week as week,
    campaignstat.conversions,
    campaignstat.interactions


FROM 
	{{ source('google_adwords', 'campaign_hourly_stats') }} as campaignstat
WHERE
    campaignstat.date >= '2020-12-01'
SELECT
    aghourlystat.date as date,
    aghourlystat.quarter as quarter,
    aghourlystat.ad_network_type_1 as networkType1,
    aghourlystat.month_of_year as month_of_year,
    aghourlystat.ad_network_type_2 as networkType2,
    aghourlystat.hour_of_day as hour_of_day,
    aghourlystat.cost as cost,
    aghourlystat.month as month,
    aghourlystat.click_type as clickType,
    aghourlystat.campaign_id as campaignID,
    aghourlystat.day_of_week as day_of_week,
    aghourlystat.impressions as impressions,
    aghourlystat.device as device,
    aghourlystat.conversions as conversions,
    aghourlystat.year as year,
    aghourlystat.ad_group_id as adgroupID,
    aghourlystat.clicks as clicks,
    aghourlystat.week as week

FROM 
	{{ source('google_adwords', 'ad_group_hourly_stats') }} as aghourlystat
WHERE
    aghourlystat.date >= '2020-01-01'
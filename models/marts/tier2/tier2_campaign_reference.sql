with 
     original as (
          
          select
               campaignadid
               , campaignadfullname
               , adplatform
               , customparameter
               , trackingurl
          from {{ ref('stg_campaign_reference')}}

          union all

          select 
               cast(t1.campaignid as char(256)) as campaignadid
               , t1.campaignname as campaignadfullname
               , 'bing' as adplatform
               , t2.parametervalue as customparameter
               , t1.trackingtemplate as trackingurl
              
          from 
              (
               (select distinct * from {{ ref('stg_bingad_campaign_history')}}) as t1
                    left join 
                         (select distinct * from {{ ref('stg_bingad_campaign_custom_paramter_history')}}) as t2
                              on t1.campaignid = t2.campaignid
              )
      ),

     utms_available as (
          
          select 
               campaignadid
               , campaignadfullname
               , adplatform
               , customparameter
               , trackingurl

               , case 
                    when customparameter like '%{_campaign}%' 
                         then substring(customparameter, position('{_campaign}='in customparameter)+len('{_campaign}='))
                    when adplatform like '%bing%'
                         then customparameter
                    end as left_trimed_customparameter --temporary column, meaningless

               , case 
                    when trackingurl like '%utm_campaign%' 
                         then substring(trackingurl, position('utm_campaign='in trackingurl)+len('utm_campaign=')) 
                    end as left_trimed_campaignurl --temporary column, meaningless

               , case 
                    when trackingurl like '%utm_source%' 
                         then substring(trackingurl, position('utm_source='in trackingurl)+len('utm_source=')) 
                    end as left_trimed_sourceurl --temporary column, meaningless

               , case 
                    when trackingurl like '%utm_medium%' 
                         then substring(trackingurl, position('utm_medium='in trackingurl)+len('utm_medium=')) 
                    end as left_trimed_mediumurl --temporary column, meaningless

               , case 
                    when left_trimed_customparameter like '%;%' 
                         then left(left_trimed_customparameter, charindex(';', left_trimed_customparameter)-1)
                    when left_trimed_customparameter not like '%;%' 
                         then left_trimed_customparameter
                    when left_trimed_campaignurl like '%&%' 
                         then left(left_trimed_campaignurl, charindex('&', left_trimed_campaignurl)-1)
                    else left_trimed_campaignurl end as utm_campaign
  
               , case 
                    when left_trimed_sourceurl like '%&%' 
                         then left(left_trimed_sourceurl, charindex('&', left_trimed_sourceurl)-1)
                    else left_trimed_sourceurl end as utm_source

               , case 
                    when left_trimed_mediumurl like '%&%' 
                         then left(left_trimed_mediumurl, charindex('&', left_trimed_mediumurl)-1)
                    else left_trimed_mediumurl end as utm_medium
          
          from original
          -- where len(campaignadid)>4
     ),

     full_reference as (
          
          select
               campaignadid
               , campaignadfullname
               , adplatform
               , case when utm_campaign is null then campaignadfullname else utm_campaign end as utm_campaign
               , case when utm_source is null and adplatform = 'google' then 'google' 
                    when utm_source is null and adplatform = 'facebook' then 'facebook'
                    when utm_source is null and adplatform = 'linkedin' then 'linkedin'
                    else utm_source end as utm_source
               , case when utm_medium is null and adplatform = 'google' and lower(campaignadfullname) like '%youtube%' then 'google-youtube' 
                    when utm_medium is null then 'unknown'
                    else utm_medium end as utm_medium
          
          from 
               utms_available
     ),

     campaignad_performance as (
               
          select 
               gs.date as date
               , cast(gs.campaignID as char(256)) as campaignAdId 
               , sum(gs.cost)*0.8 as spend
               , sum(gs.impressions) as impressions
               , sum(gs.clicks) as clicks
               -- , gs.conversions

          from {{ ref('stg_googlead_campaign_hourlystats')}} as gs
          group by 1,2

          union all

          select
               bs.date as date
               , cast(bs.campaignId as char(256)) as campaignAdId 
               , sum(bs.dailySpend) as spend
               , sum(bs.dailyImpressions) as impressions
               , sum(bs.dailyClicks) as clicks
               -- , bs.dailyConversions

          from {{ ref('stg_bingad_campaign_daily_performance')}} as bs
          group by 1,2

          union all 

          select
               fs.date as date
               , fs.adId as campaignAdId
               , sum(dailySpend) as spend
               , sum(dailyImpressions) as impressions
               , sum(cast(round(dailySpend*1/nullif(dailyCpc,0)) as bigint)) as clicks

          from {{ ref('stg_facebook_report')}} as fs
          group by 1,2
            

          union all


          select
               lc.date as date
               , cast(lc.creativeId as char(256)) as campaignAdId
               , sum(costInUSD) as spend
               , sum(impressions) as impressions
               , sum(clicks) as clicks

          from {{ ref('stg_linkedinads_creative')}} as lc
          group by 1,2
     )

-- campaign_performance_summary --
          
     select 
          cp.date
          , fr.adplatform
          , cp.campaignAdId
          , fr.campaignadfullname
          , fr.utm_campaign
          , fr.utm_source
          , fr.utm_medium
          , sum(cp.spend) as totalDailySpend
          , sum(cp.impressions) as totalDailyImpressions
          , sum(cp.clicks) as totalDailyClicks
     
     from campaignad_performance as cp
          join full_reference fr
              on cp.campaignAdId = fr.campaignadid
     group by
          1,
          2,
          3,
          4,
          5,
          6,
          7
     

          
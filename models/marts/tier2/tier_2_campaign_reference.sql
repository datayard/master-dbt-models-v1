with 
    temp as (
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
      )

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
from temp
where len(campaignadid)>4
order by adplatform
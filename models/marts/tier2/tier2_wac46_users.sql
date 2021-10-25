with users_summary_table as (
    SELECT dt.userid                                                     AS userid,
           dt.organizationid                                             AS organizationid,
           v.childentityid                                               AS videoid,
           date(date_trunc('week', dt.createddate))                      AS org_created_week,
           date(date_trunc('week', v.createddate))                       AS video_created_week,
           DATEDIFF(week, org_created_week, video_created_week)          AS week_diff,
           dense_rank() over(partition by dt.organizationid order by video_created_week asc) as time_sort
    FROM {{ref ('kube_vidyard_user_details') }} AS dt
             JOIN {{ ref('tier2_vidyard_videos') }} AS v
                  ON dt.userid = v.userid
    WHERE dt.excludeemail = 0
      and v.origin != 'sample'
      and week_diff between 0 and 5
),
t_summary as (
    select  organizationid,
            count(distinct week_diff) as distinct_weeks
    from users_summary_table
    group by 1
    having count(distinct week_diff) >=4
)
select distinct
  t.video_created_week
  ,  s.organizationid
from users_summary_table t
join t_summary s
  on t.organizationid = s.organizationid
where
   t.time_sort=4

with first_mqi as (
    select
      sm.email
      , sm.mqi_date
      , sm.parentCampaign
      , sm.parentCTAtype
      , sm.parentChannel
      , sm.childCampaign
      , sm.childCTAtype
      , sm.childChannel
      , sm.campaign_sourcedby
    from
       {{ ref('tier2_sorted_mqi') }} sm
    where sm.rn = 1
  )
  , second_mqi as (
    select
      sm.email
      , sm.mqi_date
    from
       {{ ref('tier2_sorted_mqi') }} sm
    where sm.rn = 2
      and (sm.user_rn != 2 or sm.user_rn is null)
  )
  , all_mqi as (
    select
      sm.email
      , sm.mqi_date
    from
       {{ ref('tier2_sorted_mqi') }} sm
    where (sm.user_rn != 2 or sm.user_rn is null)
  )
  , first_mql as (
    select
      sm.email
      , min(sm.mqlDateEST) as mqlDate
    from
       {{ ref('tier2_sorted_mqi') }} sm
    where
      sm.mql = 'true'
    group by
      1
  )
  , first_sal as (
    select
      sm.email
      , min(sm.salDateEST) as saldate
    from
      {{ ref('tier2_sorted_mqi') }} sm
    where
      sm.sal = 'true'
    group by
      1
  )
  , first_sql as (
    select
      sm.email
      , min(sm.sqlDateEST) as sqlDate
    from
      {{ ref('tier2_sorted_mqi') }} sm
    where
      sm.sql = 'true'
    group by
      1
  )
  , first_sqo as (
    select
      sm.email
      , min(sm.sqoDate) as sqoDate
    from
      {{ ref('tier2_sorted_mqi') }} sm
    where
      sm.sqo = 'true'
    group by
      1
  )
  , first_won as (
    select
      sm.email
      , min(sm.opportunityClosedWonDate) as wonDate
    from
      {{ ref('tier2_sorted_mqi') }} sm
    where
      sm.won = 'true'
    group by
      1
  ),

   x as (

select
<<<<<<< HEAD
    sm.*,
    fm.mqi_date as firstMQIdate,
    fm.email as firstMQIemail,
=======
    fm.*,
>>>>>>> c59ed18fb9d22c97ef5cea3391174381f285fb97
    rm.mqi_date as secondMQIdate,
    rm.email as secondMQIemail,
    am.email as allmqiemail,
    fmql.mqldate as firstmqldate,
    fmql.email as firstmqlemail,
    fsal.saldate as firstsaldate,
    fsal.email as firstsalemail,
    fsql.sqldate as firstsqldate,
    fsql.email as firstsqlemail,
    fsqo.sqodate as firstsqodate,
<<<<<<< HEAD
    fsqo.email as firstsqoemail,
    fwon.wondate as firstwondate,
    fwon.email as firstwonemail
from {{ ref('tier2_sorted_mqi') }} sm
left join first_mqi fm on sm.email = fm.email
left join second_mqi rm on sm.email = rm.email
left join all_mqi am on sm.email = am.email
left join first_mql fmql on sm.email = fmql.email
left join first_sal fsal on sm.email = fsal.email
left join first_sql fsql on sm.email = fsql.email
left join first_sqo fsqo on sm.email = fsqo.email
left join first_won fwon on sm.email = fwon.email
=======
    fwon.wondate as firstwondate
from first_mqi fm 
left join second_mqi rm on fm.email = rm.email
left join first_mql fmql on fm.email = fmql.email
left join first_sal fsal on fm.email = fsal.email
left join first_sql fsql on fm.email = fsql.email
left join first_sqo fsqo on fm.email = fsqo.email
left join first_won fwon on fm.email = fwon.email)

select email from x group by email having count(*) > 1
>>>>>>> c59ed18fb9d22c97ef5cea3391174381f285fb97

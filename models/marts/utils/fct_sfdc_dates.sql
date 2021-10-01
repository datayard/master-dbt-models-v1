with
 dates as (
    select
      (
        getdate()::date +1000 - n)::date as date
    from
      (
        select
          row_number() over(order by 1) - 1 as n
        from
          {{ ref('tier2_salesforce_contact') }}
        limit 11000
      )
  )
  -- Future dates is used to generate dates that occur in the future (beyond the current month).
  , future_dates as (
    select distinct
      dateadd('months', 3, d.date) as date
    from
      dates d
    where
      dateadd('months', 3, d.date) > d.date
    order by
      1 desc
  )
  , joined_dates as (
    select
      d.date as date
    from
      dates d
    union
    select
      f.date as date
    from
      future_dates f
  )
select
  date(date)
  , case
    when date_part('month', d.date) in (
      '5'
      , '6'
      , '7'
    )
      then  convert(char(4), date_part('year', d.date) + 1)  + '-' + 'Q1'
    when date_part('month', d.date) in (
      '8'
      , '9'
      , '10'
    )
      then  convert(char(4), date_part('year', d.date) + 1) + '-' + 'Q2'
    when date_part('month', d.date) in (
      '11'
      , '12'
    )
      then  convert(char(4), date_part('year', d.date) + 1) + '-' + 'Q3'
    when date_part('month', d.date) in (
      '1'
    )
      then convert(char(4), date_part('year', d.date) + 0) + '-' + 'Q3'
    when date_part('month', d.date) in (
      '2'
      , '3'
      , '4'
    )
      then convert(char(4), date_part('year', d.date) + 0)  + '-' + 'Q4'
  end as fiscal_period
  , case
    when date_part('month', d.date) in (
      '5'
      , '6'
      , '7'
    )
      then convert(char(4), date_part('year', d.date) + 1)
    when date_part('month', d.date) in (
      '8'
      , '9'
      , '10'
    )
      then convert(char(4), date_part('year', d.date) + 1)
    when date_part('month', d.date) in (
      '11'
      , '12'
    )
      then convert(char(4), date_part('year', d.date) + 1)
    when date_part('month', d.date) in (
      '1'
    )
      then convert(char(4), date_part('year', d.date) + 0)
    when date_part('month', d.date) in (
      '2'
      , '3'
      , '4'
    )
      then convert(char(4), date_part('year', d.date) + 0)
  end as fiscalyear
  , case
    when date_part('month', d.date) in (
      '5'
      , '6'
      , '7'
    )
      then 'Q1'
    when date_part('month', d.date) in (
      '8'
      , '9'
      , '10'
    )
      then 'Q2'
    when date_part('month', d.date) in (
      '11'
      , '12'
    )
      then 'Q3'
    when date_part('month', d.date) in (
      '1'
    )
      then 'Q3'
    when date_part('month', d.date) in (
      '2'
      , '3'
      , '4'
    )
      then 'Q4'
  end as fiscalquarter
, right(fiscalquarter,1)*1 + left(fiscalyear,4) * 4 as fiscalquartervalue
  , case
    when date_part('month', d.date) <= 4
      then date_part('month', d.date) + 8
    else date_part('month', d.date) -4
  end as fiscalmonth
  , case
    when date_part('month', d.date) <= 4
      then cast(date_part('year', d.date) as text) || '-' || case
      when date_part('month', d.date) + 8 < 10
        then '0'
      else ''
    end || cast(date_part('month', d.date) + 8 as text)
    else cast(date_part('year', d.date) + 1 as text) || '-' || case
      when date_part('month', d.date) -4 < 10
        then '0'
      else ''
    end || cast(date_part('month', d.date) -4 as text)
  end as fiscalyearmonth
  , date_part('year', d.date) as year
  , date_part('month', d.date) as month


--  , cast(date_part('year', d.date) as text) || '-' || case
--    when date_part('month', d.date) < 10
--      then '0'
--    else ''
--  end || cast(date_part('month', d.date) as text) as year_month
  , to_char(date_trunc('month',d.date),'YYYY-MM') as yearmonth

  , date_part('year', d.date) * 12 + date_part('month', d.date) as yearmonthvalue
  , date_part('day', d.date) as dateday
from
  joined_dates d
order by
  1 desc

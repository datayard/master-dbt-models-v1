/*{{ config(materialized='table', tags=['publish']) }}*/

WITH
  t1 as (
    SELECT
      vy_users.userId as user_id,
      count(distinct vy_embeds.embedID) as active_embeds,
      max(
        case
          when 
            vy_limits.allotmentTypeID = 3
          then 
            vy_limits.limit
          when
            (
              vy_orgs.accountId not in 
                (
                  select 
                    account_id 
                  from 
                    public.vidyard_invoiced_customers
                )
            and vy_orgs.organizationID = vy_orgs.accountId
          )
            then 5
          else null
        end
      )
      as embed_limit,
      -- These next two fields are just to check if a limit exists/doesn't exist for free/enterprise accounts
      vy_orgs.organizationID,
      vy_orgs.accountId
    from
      {{ref('stg_vidyard_organizations')}} as vy_orgs
    join 
      {{ref('stg_vidyard_user_groups')}} as vy_usergroups 
    on
      vy_usergroups.organizationID = vy_orgs.organizationID
    join 
      {{ref('stg_vidyard_users')}} as vy_users 
    on
      vy_users.userId = vy_usergroups.userID
    left join 
      {{ref('stg_vidyard_active_embeds')}} as vy_embeds
    on
      vy_embeds.accountID = vy_orgs.accountId
    left join 
      {{ref('stg_vidyard_allotment_limits')}} as vy_limits 
    on
      vy_limits.accountID = vy_orgs.accountId
    where
      vy_users.email not like '%vidyard.com'
      and vy_users.email not like '%viewedit.com'
      and vy_orgs.orgType = 'self_serve'
      and vy_orgs.createdDate >= '2016-10-11'
    group by
      1,
      4,
      5
    order by
      3 desc
  )
select
  user_id,
  active_embeds,
  embed_limit,
  embed_limit - active_embeds as remaining_embeds
from
  t1
where
  embed_limit is not null
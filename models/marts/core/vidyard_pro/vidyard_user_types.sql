select 
	distinct
	o.organizationid
    , o.accountid
    , o.ownerid
    , o.parentid
    , o.orgtype 
    , u.userid
    , case
    		when coalesce(tm.teamid) is not null  then 2
            else null
       end as has_teamid
    , case
    		when coalesce(tm.teammembershipid) is not null  then 2
            else null
       end as has_membershipid
    , case
    		when coalesce(ug.groupid) is not null  then 2
            else null
       end as has_groupid    
    , o.name
    , case
    	when o.ownerid = u.userid and o.orgtype = 'self_serve' then 1
        else 0
       end as has_personal_account
    , case
    	when (o.ownerid = u.userid and o.orgtype = 'self_serve' and o.organizationid != o.accountid) then 4
        else 0
       end as linked_to_parent_account
    , case
    	when (has_personal_account + linked_to_parent_account + case when coalesce(has_teamid, has_membershipid, has_groupid) is null then 0 else coalesce(has_teamid, has_membershipid, has_groupid) end) = 0 then 'Orphan'
        when (has_personal_account + linked_to_parent_account + case when coalesce(has_teamid, has_membershipid, has_groupid) is null then 0 else coalesce(has_teamid, has_membershipid, has_groupid) end) = 1 then 'Stand Alone'
        when (has_personal_account + linked_to_parent_account + case when coalesce(has_teamid, has_membershipid, has_groupid) is null then 0 else coalesce(has_teamid, has_membershipid, has_groupid) end) = 2 then 'Enterprise Only'
        when (has_personal_account + linked_to_parent_account + case when coalesce(has_teamid, has_membershipid, has_groupid) is null then 0 else coalesce(has_teamid, has_membershipid, has_groupid) end) = 3 then 'Hybrid'
        when (has_personal_account + linked_to_parent_account + case when coalesce(has_teamid, has_membershipid, has_groupid) is null then 0 else coalesce(has_teamid, has_membershipid, has_groupid) end) = 7 then 'Enterprise Personal'
      	else '--'
      end as user_type
from {{ ref('stg_vidyard_organizations') }} o
join {{ ref('stg_vidyard_users') }} u
	on o.ownerid = u.userid
left join {{ ref('stg_vidyard_user_groups') }} ug
    on ug.userid = u.userid
left join {{ ref('stg_vidyard_team_memberships') }} tm
    on tm.userid = u.userid
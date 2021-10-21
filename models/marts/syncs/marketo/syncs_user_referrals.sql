SELECT
	Distinct vy_referrals.referrerid,
	vy_referrals.signed_up_referals,
	vy_referrals.sent_referal,
  master_user.mkto_lead_id,
  master_user.email
FROM
    {{ ref('fct_total_user_referrals') }} as vy_referrals
JOIN
  {{ref('user_id_master')}} as master_user
ON
  vy_referrals.referrerid = master_user.vy_user_id
WHERE 
  master_user.mkto_lead_id IS NOT null

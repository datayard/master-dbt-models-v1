SELECT
	vy_referals.referrerid,
	COUNT (case when vy_referals.referralstatus LIKE 'signed_up' THEN 1 ELSE NULL end) as signed_up_referals,
	COUNT (case when vy_referals.referralstatus LIKE 'sent_referral' THEN 1 ELSE NULL end) as sent_referal,
	COUNT (case when vy_referals.referralstatus LIKE 'inactive' THEN 1 ELSE NULL end) as inactive_referal,
	COUNT (case when vy_referals.referralstatus LIKE 'clicked_link' THEN 1 ELSE NULL end) as clicked_link
FROM
    {{ ref('stg_vidyard_referrals') }} as vy_referals
WHERE
	true
GROUP BY
	vy_referals.referrerid
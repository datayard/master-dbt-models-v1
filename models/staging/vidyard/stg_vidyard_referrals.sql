SELECT 
	vidyard_referrals.id as referralID,
	vidyard_referrals.referrer_user_id as referrerID,
	vidyard_referrals.referee_user_id as referreID,
	vidyard_referrals.status as referralStatus,
	vidyard_referrals.incentive as incentive,
	vidyard_referrals.updated_at as updatedAt
 FROM 
	{{ source('public', 'vidyard_referrals') }} as vidyard_referrals
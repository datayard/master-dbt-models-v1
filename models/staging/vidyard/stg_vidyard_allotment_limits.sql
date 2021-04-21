SELECT 
	vidyard_allotment_limits.account_id as accountID,
	vidyard_allotment_limits.id as allotmentLimitID,
	vidyard_allotment_limits.limit as limit,
	vidyard_allotment_limits.grace_limit as graceLimit,
	vidyard_allotment_limits.allotment_type_id as allotmentTypeID,
	vidyard_allotment_limits.enforced as enforced
 FROM 
	{{ source('public', 'vidyard_allotment_limits') }} as vidyard_allotment_limits
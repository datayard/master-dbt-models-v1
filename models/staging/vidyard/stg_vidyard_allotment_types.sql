SELECT 
	vidyard_allotment_types.id as allotmentTypeID,
	vidyard_allotment_types.name as name,
	vidyard_allotment_types.description as description,
	vidyard_allotment_types.default_limit as defaultLimit,
	vidyard_allotment_types.default_grace_limit as defaultGraceLimit,
	vidyard_allotment_types.default_enforced as defaultEnforced
 FROM 
	{{ source('public', 'vidyard_allotment_types') }} as vidyard_allotment_types
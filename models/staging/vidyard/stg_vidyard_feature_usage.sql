SELECT 
	vidyard_feature_usages.id as featureusageid,
	vidyard_feature_usages.organization_id as organizationId,
	vidyard_features.feature_id as featureId
 FROM 
	{{ source('public', 'vidyard_feature_usages') }} as vidyard_feature_usages
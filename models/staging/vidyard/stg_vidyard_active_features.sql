SELECT 
	vidyard_active_features.id as activeFeatureID,
	vidyard_active_features.organization_id as organizationID,
	vidyard_active_features.feature_id as featureID,
	vidyard_active_features.created_at as createdDate,
	vidyard_active_features.updated_at as updatedDate
 FROM 
	{{ source('public', 'vidyard_active_features') }} as vidyard_active_features
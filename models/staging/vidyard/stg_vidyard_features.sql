SELECT 
	vidyard_features.id as featureId,
	vidyard_features.friendly_name as friendlyName,
	vidyard_features.include_by_default as includeByDefault,
	vidyard_features.category as category,
	vidyard_features.unique_name as uniqueName
 FROM 
	{{ source('public', 'vidyard_features') }} as vidyard_features
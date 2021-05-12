SELECT 
        vidyard_hubs.id as hubID,
        vidyard_hubs.organization_id as organizationID,
        vidyard_hubs.account_id as accountID,
        vidyard_hubs.cname as name,
        vidyard_hubs.subdomain as subDomain,
        vidyard_hubs.hub_type as hubType,
        vidyard_hubs.manage_hub_users as manageHubUsers,
        vidyard_hubs.route_type as routeType,
        vidyard_hubs.enabled_seo as enabledSEO,
        vidyard_hubs.created_at as createdDate,
        vidyard_hubs.updated_at as updatedDate,
        vidyard_hubs.index_layout_id as indexLayoutID,
        vidyard_hubs.show_layout_id as showLayoutID,
        vidyard_hubs.category_layout_id as categoryLayoutID,
        vidyard_hubs.search_layout_id as searchLayoutID
FROM
    {{ source ('public', 'vidyard_hubs')}} as vidyard_hubs
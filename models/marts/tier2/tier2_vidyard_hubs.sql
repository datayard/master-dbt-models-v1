SELECT
        hubID,
        organizationID,
        accountID,
        name,
        subDomain,
        hubType,
        manageHubUsers,
        routeType,
        enabledSEO,
        createdDate,
        updatedDate,
        indexLayoutID,
        showLayoutID,
        categoryLayoutID,
        searchLayoutID
FROM
    {{ ref('stg_vidyard_hubs') }}
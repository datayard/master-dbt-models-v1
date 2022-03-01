SELECT
    cr.id as campaignAdId,
    cr.full_name as campaignAdFullName,
    cr.source as adPlatform,
    cr.custom_parameter as customParameter,
    cr.tracking_url as trackingURL
    
FROM
    {{ source('ops_utility_tables','campaign_reference') }} as cr
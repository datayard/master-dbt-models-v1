SELECT
        embedID,
        accountID
FROM
    {{ ref('stg_vidyard_active_embeds') }}
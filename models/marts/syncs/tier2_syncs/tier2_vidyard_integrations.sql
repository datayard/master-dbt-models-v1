SELECT
    orgs.account_id,
    listagg (distinct tokens.token_type,';') as useCase
FROM
    {{ ref('stg_vidyard_api_tokens') }} as tokens
JOIN
    {{ ref('stg_vidyard_organizations') }} as orgs
ON
    tokens.organization_id = orgs.id
WHERE
    tokens.is_valid = true
GROUP BY
  orgs.account_id
LIMIT
    100
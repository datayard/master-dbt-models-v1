SELECT
    usrdetails.domain,
    COUNT (CASE WHEN usrdetails.personal_account_type like 'free' then 1 end) as free,
    COUNT (CASE WHEN usrdetails.personal_account_type like 'pro' then 1 end) as pro,
    COUNT (CASE WHEN usrdetails.personal_account_type like 'enterprise' then 1 end) as enterprise,
    COUNT (CASE WHEN usrdetails.personal_account_type like 'enterprise self serve' then 1 end) AS enterprise_self_serve
FROM
    {{ ref('tier2_vidyard_user_details') }} usrdetails
WHERE
    usrdetails.domaintype = 'business'
    AND (usrdetails.createddate  >= DATEADD(MONTH, -6, current_date)
    OR usrdetails.lastSessionDate >= DATEADD(MONTH, -6, current_date))
GROUP BY
    usrdetails.domain
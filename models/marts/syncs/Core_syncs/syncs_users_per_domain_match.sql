WITH maus as (
    SELECT
        COUNT (distinct mau.userid) as maus,
        mau.domain
    FROM
        dbt_vidyard_master.tier2_mau as mau
    WHERE
        mau = 1
    GROUP BY
        mau.domain
),
    WEU as (
        SELECT
            COUNT (distinct weu.userid) as weus,
            weu.domain
        FROM
            dbt_vidyard_master.tier2_weu as weu
        WHERE
            true
        GROUP BY
            weu.domain
    )

SELECT
    usrdetails.domain,
    maus.maus,
    WEU.weus,
    COUNT (CASE WHEN usrdetails.classification like 'free' then 1 end) as free,
    COUNT (CASE WHEN usrdetails.classification like 'free' AND (usrdetails.createddate >= DATEADD(MONTH, -6, current_date) OR usrdetails.lastSessionDate >= DATEADD(MONTH, -6, current_date)) then 1 end) as free_6months,
    COUNT (CASE WHEN usrdetails.classification like 'pro' then 1 end) as pro,
    COUNT (CASE WHEN usrdetails.classification like 'pro' AND (usrdetails.createddate >= DATEADD(MONTH, -6, current_date) OR usrdetails.lastSessionDate >= DATEADD(MONTH, -6, current_date)) then 1 end) as pro_6months,
    COUNT (CASE WHEN usrdetails.classification like 'enterprise' then 1 end) as enterprise,
    COUNT (CASE WHEN usrdetails.classification like 'enterprise' AND (usrdetails.createddate >= DATEADD(MONTH, -6, current_date) OR usrdetails.lastSessionDate >= DATEADD(MONTH, -6, current_date)) then 1 end) as enterprise_6months,
    COUNT (CASE WHEN usrdetails.classification like 'enterprise self serve' then 1 end) as enterprise_self_serve,
    COUNT (CASE WHEN usrdetails.classification like 'enterprise self serve' AND (usrdetails.createddate >= DATEADD(MONTH, -6, current_date) OR usrdetails.lastSessionDate >= DATEADD(MONTH, -6, current_date)) then 1 end) as enterprise_ss_6months
FROM
    {{ ref('tier2_vidyard_user_details') }} usrdetails
    --dbt_vidyard_master.tier2_vidyard_user_details as usrdetails
LEFT JOIN
    maus
ON
    maus.domain = usrdetails.domain
LEFT JOIN
    WEU
ON
    WEU.domain = usrdetails.domain
WHERE
    usrdetails.domaintype like 'business'
GROUP BY
    usrdetails.domain,
    maus.maus,
    WEU.weus
ORDER BY
    maus.maus ASC






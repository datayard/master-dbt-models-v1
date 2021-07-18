WITH pro_acocunts as (
    SELECT
        'Self-serve' as accountype,
        COUNT (distinct Pro_accounts.activeproaccounts) as accounts
    FROM
         {{ ref('fct_active_pro_subscriptions') }} as Pro_accounts
    WHERE
          Pro_accounts.name NOT LIKE 'Free Forever'
    GROUP BY
        accountype
    ),

    customer_accounts as (
    SELECT
        account.employeesegment as accountype,
        COUNT (distinct account.accountid) as accounts
    FROM
        {{ ref('stg_salesforce_account') }} as account
    WHERE
        account.accounttype LIKE 'Customer'
        AND account.ispersonaccount = false
    GROUP BY
        accountype
    )
SELECT
       accountype,
       accounts
FROM
    (
        SELECT accountype,
               accounts
        FROM pro_acocunts
        UNION ALL
        SELECT accountype,
               accounts
        FROM customer_accounts
    )
GROUP BY
    accountype,
    accounts

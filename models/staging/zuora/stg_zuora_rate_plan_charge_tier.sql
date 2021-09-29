SELECT
        rpct.id as rateplanchargetierid
        , rpct.rate_plan_charge_id as rateplanchargeid
        , rpct.discount_percentage as discountpercentage
FROM
        {{ source ('zuora', 'rate_plan_charge_tier')}} as rpct

WHERE
        TRUE
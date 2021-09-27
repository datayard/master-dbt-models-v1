SELECT 
        rpc.rate_plan_id as rateplanid
        , rpc.mrr as mrr
        , rpc.charge_model as chargemodel
FROM
        {{ source ('zuora', 'rate_plan_charge')}} as rpc

WHERE
        TRUE
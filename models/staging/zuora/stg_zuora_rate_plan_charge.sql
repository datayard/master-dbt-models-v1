SELECT
        rpc.id as rateplanchargeid
        , rpc.rate_plan_id as rateplanid
        , rpc.mrr as mrr
        , rpc.charge_model as chargemodel
        , rpc.effective_start_date as effectivestartdate
        , rpc.effective_end_date as effectiveenddate
FROM
        {{ source ('zuora', 'rate_plan_charge')}} as rpc

WHERE
        TRUE

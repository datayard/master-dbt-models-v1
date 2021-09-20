SELECT
        accountid
        , allotmentlimitid
        , "limit"
        , gracelimit
        , enforced
        , at.allotmenttypeid
        , name
        , description
        , defaultlimit
        , defaultgracelimit
        , defaultenforced
    FROM
         {{ ref('stg_vidyard_allotment_limits') }} al
         JOIN {{ ref('stg_vidyard_allotment_types') }} at
            ON al.allotmenttypeid = at.allotmenttypeid
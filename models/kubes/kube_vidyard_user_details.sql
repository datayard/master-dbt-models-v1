WITH 
    first_session_table AS (
        SELECT
            vu.organizationid
            , ht.derived_channel 
            , ht.sessiontime
            , ROW_NUMBER() OVER(PARTITION BY vu.organizationid ORDER BY ht.sessiontime) AS rn
        FROM 
            {{ ref('tier2_vidyard_user_details') }} vu
            JOIN {{ ref('tier2_heap') }} ht
                ON  ht.vidyardUserId = vu.userid         
        WHERE 
            ht.tracker = 'global_session'     
            --only include sessions 30 minutes prior to signup
            AND DATEDIFF('minute', ht.sessiontime, vu.createddate) BETWEEN 0 AND 30
)
,
    use_case_data AS (
        SELECT * FROM 
        (
        SELECT
            vu.userid
            , vu.organizationid
            , ht.combined_usecase
            , ROW_NUMBER() OVER(PARTITION BY vu.organizationid ORDER BY CASE WHEN ht.combined_usecase IS NULL THEN 99 ELSE 1 END ASC) AS rn
        FROM 
            {{ ref('tier2_vidyard_user_details') }} vu
            JOIN {{ ref('stg_govideo_production_users') }} ht
                ON  ht.vidyardUserId = vu.userid 
        ) a
        where rn=1  and combined_usecase is not null      

)
SELECT 
    vu.userid
    , vu.organizationid
    , vu.personal_account_type
    , vu.enterprise_access
    , vu.classification
    , vu.excludeEmail
    , vu.domain
    , vu.domainType
    , vu.name
    , vu.createddate
    , vu.updateddate
    , vu.createdbyclientid
    , case 
        when vu.signup_source is not null then vu.signup_source
        when fst.derived_channel is null then 'Direct'
        else fst.derived_channel 
    end as signupsource    
    , vu.firstviewdate
    , vu.firstviewvideoid
    , vu.totalseconds
    , vu.videoswithviews
    , vu.viewscount
    , vu.activatedFlag
    , uc.combined_usecase

FROM 
    {{ ref('tier2_vidyard_user_details') }} vu
    LEFT JOIN first_session_table fst
        ON  vu.organizationid = fst.organizationid     
    LEFT JOIN use_case_data uc
        ON vu.organizationid = uc.organizationid        
WHERE 
    (fst.rn = 1 or fst.rn is null)
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
    used_chrome_extension AS (
        SELECT DISTINCT
            vu.organizationid
        FROM 
            {{ ref('tier2_vidyard_user_details') }} vu
            JOIN {{ ref('tier2_heap') }} ht
                ON  ht.vidyardUserId = vu.userid         
        WHERE 
            ht.tracker = 'opened_extension'     
)
,
    created_video AS (
        SELECT DISTINCT
            vu.organizationid
        FROM 
            {{ ref('tier2_vidyard_user_details') }} vu
            JOIN {{ ref('tier2_vidyard_user_entities') }} en
                ON  en.userid = vu.userid         
        WHERE 
            en.entity = 'video'
            and en.origin != 'sample'
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
    , vu.email
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
    , coalesce(om.generalUseCase,uc.combined_usecase) as combined_usecase
    , case 
        when ext.organizationid is not null then 1
        else 0
    end as usedchromeextensionflag
    ,  case 
        when cv.organizationid is not null then 1
        else 0
    end as createdvideoflag


FROM 
    {{ ref('tier2_vidyard_user_details') }} vu
    LEFT JOIN first_session_table fst
        ON  vu.organizationid = fst.organizationid    
    LEFT JOIN {{ ref('stg_vidyard_onboarding_metadata') }} om     
        ON vu.userid = om.userid
    LEFT JOIN use_case_data uc
        ON vu.organizationid = uc.organizationid 
    LEFT JOIN used_chrome_extension ext
        ON vu.organizationid = ext.organizationid 
    LEFT JOIN created_video cv
        ON vu.organizationid = cv.organizationid
WHERE 
    (fst.rn = 1 or fst.rn is null)
    -- and vu.classification NOT IN ('anomaly','orphan','unknown')
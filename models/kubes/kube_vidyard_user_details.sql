WITH 
    first_session_table AS (
        SELECT
            vu.organizationid
            , ht.sessionid
            , ht.derived_channel 
            , ht.sessiontime
            , ae.acquisition_channel
            , ae.acquisition_event
            , ROW_NUMBER() OVER(PARTITION BY vu.organizationid ORDER BY ht.sessiontime) AS rn
        FROM 
            {{ ref('tier2_vidyard_user_details') }} vu
            JOIN {{ ref('tier2_heap') }} ht
                ON  ht.vidyardUserId = vu.userid
            JOIN {{ ref('tier2_acquisition_events') }} ae
                ON ae.sessionid = ht.sessionid
        WHERE 
            ht.tracker = 'global_session'     
            --only include sessions 30 minutes prior to signup
            AND DATEDIFF('minute', ht.sessiontime, vu.createddate) BETWEEN 0 AND 30
)
,
   last_product_session AS (
        SELECT
            vu.organizationid
            , max(ht.sessiontime) AS last_session
        FROM
            {{ ref('tier2_vidyard_user_details') }} vu
            JOIN {{ ref('tier2_heap') }} ht
                ON  ht.vidyardUserId = vu.userid
        WHERE
            ht.tracker = 'product_sessions'
        GROUP BY 1

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
            JOIN {{ ref('tier2_vidyard_videos') }} en
                ON  en.userid = vu.userid         
        WHERE 
            en.origin != 'sample'
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
            JOIN {{ ref('tier2_heap_users') }} ht
                ON  ht.vidyardUserId = vu.userid 
        ) a
        where rn=1  and combined_usecase is not null      

)
  , specific_use_case_data as (
        SELECT * FROM
        (
        SELECT
            vu.userid
            , vu.organizationid
            , ht.specificUseCase
            , ROW_NUMBER() OVER(PARTITION BY vu.organizationid ORDER BY CASE WHEN ht.specificUseCase IS NULL THEN 99 ELSE 1 END ASC) AS rn
        FROM
            {{ ref('tier2_vidyard_user_details') }} vu
            JOIN {{ ref('tier2_heap_users') }} ht
                ON  ht.vidyardUserId = vu.userid
        ) a
        where rn=1  and specificUseCase is not null
    )

SELECT 
    vu.userid
    , vu.organizationid
    , vu.parentid
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
    , coalesce(om.specificUseCase,suc.specificUseCase) as specific_usecase
    , case 
        when ext.organizationid is not null then 1
        else 0
    end as usedchromeextensionflag
    ,  case 
        when cv.organizationid is not null then 1
        else 0
    end as createdvideoflag
    , row_number() over(partition by vu.domain order by vu.createddate asc) as rn
    , lps.last_session as lastsession
    , fst.acquisition_channel
    , fst.acquistiion_event


FROM 
    {{ ref('tier2_vidyard_user_details') }} vu
    LEFT JOIN first_session_table fst
        ON  vu.organizationid = fst.organizationid
    LEFT JOIN last_product_session lps
        ON vu.organizationid = lps.organizationid
    LEFT JOIN {{ ref('stg_vidyard_onboarding_metadata') }} om     
        ON vu.userid = om.userid
    LEFT JOIN use_case_data uc
        ON vu.organizationid = uc.organizationid 
    LEFT JOIN specific_use_case_data suc
        ON vu.organizationid = suc.organizationid
    LEFT JOIN used_chrome_extension ext
        ON vu.organizationid = ext.organizationid 
    LEFT JOIN created_video cv
        ON vu.organizationid = cv.organizationid
WHERE 
    (fst.rn = 1 or fst.rn is null)
    -- and vu.classification NOT IN ('anomaly','orphan','unknown')
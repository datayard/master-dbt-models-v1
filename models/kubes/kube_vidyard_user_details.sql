SELECT

    vu.userid
    , vu.organizationid
    , vu.personal_account_type
    , vu.enterprise_access
    , vu.classification
    , vu.email_to_exclude
    , vu.domain
    , vu.domain_type
    , vu.name
    , vu.createddate
    , vu.updateddate
    , vu.createdbyclientid
    , case 
        when vu.signup_source is not null then vu.signup_source
        when ht.derived_channel is null then 'Direct'
        else ht.derived_channel 
    end as signup_source    
    , vu.firstviewdate
    , vu.firstviewvideoid
    , vu.totalseconds
    , vu.videoswithviews
    , vu.viewscount

FROM 
    {{ ref('tier2_vidyard_user_details') }} vu
    LEFT JOIN {{ ref('tier2_heap') }} ht
        ON  ht.vy_userid = vu.userid         
        AND ht.is_vy_userid_integer = 1
WHERE ht.tracker = 'global_session'        
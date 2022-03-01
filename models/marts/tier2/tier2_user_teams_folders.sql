SELECT
	u.userid
	, o.ownerid
	, o.organizationid
	, o.createddate 
	, o.accountid
	, o.parentid
	, o.orgtype
	, o.name
	, o.updateddate
	, o.createdbyclientid
	, o.signup_source
	, u.email
	, u.domaintype
	, u.domain
	, u.excludeemail
	, CASE
		WHEN o.orgtype = 'self_serve' and o.organizationid = o.accountid
			THEN 'personal'
		WHEN o.orgtype IS NULL and o.organizationid != o.accountid
			THEN 'shared'
		WHEN o.orgtype IS NULL and o.organizationid = o.accountid
			THEN 'parent folder'
		WHEN o.orgtype = 'self_serve' and o.organizationid != o.accountid
			THEN 'personal enterprise'
		WHEN o.orgtype IS NULL and o.organizationid IS NULL
			THEN 'orphan'
		ELSE NULL
		END AS folder_type
   	, om.firstviewdate
    , om.firstviewvideoid
    , om.totalseconds
    , om.videoswithviews
    , om.viewscount
    , om.activatedFlag		
FROM {{ ref ('stg_vidyard_organizations') }} as o
JOIN {{ ref('stg_vidyard_users') }} as u
	ON o.ownerid = u.userid
LEFT JOIN {{ ref('stg_vidyard_org_metrics') }} as om
	ON o.organizationid = om.organizationid	
WHERE
	orgtype = 'self_serve'
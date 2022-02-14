WITH 
	most_used_clients as(
		SELECT
		  v.organizationid,
		  v.userid,
		  t2vud.email,
		  v.createdbyclientid as "most_frequent_client",
		  COUNT (v.createdbyclientid) as "videos_from_client",
		  row_number() over(
		    partition by v.organizationid ORDER BY "videos_from_client" DESC)
		FROM
		  --dbt_vidyard_master.stg_vidyard_videos as v
          {{ ref('stg_vidyard_videos') }} v
		LEFT JOIN
            --dbt_vidyard_master.tier2_vidyard_user_details as t2vud
            {{ ref('tier2_vidyard_user_details') }} t2vud
		ON
		  v.organizationid = t2vud.organizationid
		GROUP BY
		  v.organizationid,
		  v.userid,
		  t2vud.email,
		  v.createdbyclientid
)

SELECT
	muc.organizationid,
	muc.userid,
	muc.most_frequent_client,
	muc.videos_from_client,
	CASE
		WHEN muc.most_frequent_client LIKE 'secure.vidyard.com'
    		then 'Vidyard Platform'
   		WHEN muc.most_frequent_client LIKE 'app.salesloft.com'
    		then 'Salesloft'
    	WHEN muc.most_frequent_client LIKE 'auth.viewedit.com'
    		then 'GoVideo Extension'
    	WHEN muc.most_frequent_client LIKE 'mixmax.com'
    		then 'Mixmax'
    	WHEN muc.most_frequent_client LIKE 'app.hubspot.com'
    		then 'Hubspot App'
    	WHEN muc.most_frequent_client LIKE 'sales.hubspot.com'
    		then 'Hubspot sales'
    	WHEN muc.most_frequent_client LIKE 'support.hubspot.com'
    		then 'Hubspot support'
    	WHEN muc.most_frequent_client LIKE 'outlook.vidyard.com'
    		then 'Outlook'
    	WHEN muc.most_frequent_client LIKE 'govideo-mobile.vidyard.com'
    		then 'GoVideo Mobile'
    	WHEN muc.most_frequent_client LIKE 'quip.com'
    		then 'Quip'
    	WHEN muc.most_frequent_client LIKE 'drift.com'
    		then 'Drift'
    	WHEN muc.most_frequent_client LIKE 'allbound.com'
    		then 'Allbound'
    	WHEN muc.most_frequent_client LIKE 'prod.outreach.io'
    		then 'Outreach'
  ELSE muc.most_frequent_client
END AS client_name,
  muc.videos_from_client  + CAST(' in ' AS varchar) + muc.most_frequent_client as videos_in_most_used_client
FROM
	most_used_clients as muc
WHERE
	muc.row_number = '1'
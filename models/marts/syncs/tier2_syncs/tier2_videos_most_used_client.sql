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
	muc.videos_from_client  + CAST(' in ' AS varchar) + muc.most_frequent_client as videos_in_most_used_client
FROM
	most_used_clients as muc
WHERE
	muc.row_number = '1'
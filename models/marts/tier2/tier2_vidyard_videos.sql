SELECT
   vud2.userid
   , v.organizationid
   , p.playerid AS entityid
   , v.videoid AS childentityid
   , v.createddate AS createddate
   , v.updateddate AS updateddate
   , v.videotype AS type
   , v.createdbyclientid
   , v.origin
   , v.status
   , v.issecure
   , v.milliseconds
   , v.derivedOrigin
   , v.source
FROM
	{{ ref('stg_vidyard_videos')}} as v
   	LEFT JOIN {{ ref('stg_vidyard_players')}} as p
      ON p.organizationid = v.organizationid AND p.ownerid = v.userid
   	LEFT JOIN  {{ ref('tier2_vidyard_user_details') }} vud2
      ON  v.organizationid = vud2.organizationid AND v.userid = vud2.userid
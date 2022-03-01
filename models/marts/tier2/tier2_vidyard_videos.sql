SELECT
   vud2.userid
   , vud2.domain
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
   , p.uuid
   , case when v.origin != 'sample' then dense_rank() over(partition by vud2.userid order by v.videoid) end as milestone_rk

FROM
	{{ ref('stg_vidyard_videos')}} as v
      LEFT JOIN {{ ref('stg_vidyard_chapters')}} as c
      ON v.videoid = c.videoid
   	LEFT JOIN {{ ref('stg_vidyard_players')}} as p
      ON c.playerid = p.playerid
      --ON p.organizationid = v.organizationid AND p.ownerid = v.userid
   	LEFT JOIN  {{ ref('tier2_vidyard_user_details') }} vud2
      ON p.ownerid = vud2.userid
      --ON  v.organizationid = vud2.organizationid AND v.userid = vud2.userid
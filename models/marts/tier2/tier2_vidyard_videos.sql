SELECT
  	'video' AS entity                              
	, vud2.userid                                  
    , vud2.organizationid                          
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
	{{ ref('tier2_vidyard_user_details') }} vud2
  	JOIN {{ ref('stg_vidyard_players') }} p
  		ON p.organizationid = vud2.organizationid AND p.ownerid = vud2.userid
  	JOIN {{ ref('stg_vidyard_chapters') }} c
  		ON c.playerid = p.playerid
  	JOIN {{ ref('stg_vidyard_videos') }} v
  		ON v.videoid = c.videoid AND v.organizationid = vud2.organizationid AND v.userid = vud2.userid

-------------------------------------------------------------
--					PLAYERS
-------------------------------------------------------------
SELECT
  	'player' AS entity                              --COL01
	, vut2.userid                                   --COL02
    , vut2.organizationid                           --COL03
  	, p.playerid AS entityid                        --COL04
  	, NULL AS childentityid                         --COL05
  	, p.createddate                                 --COL06
  	, p.updateddate                                 --COL07
  	, p.playertype AS type                          --COL08
  	, p.createdbyclientid                           --COL09
  	, p.uuid                                        --COL10
  	, NULL AS origin                                --COL11
  	, NULL AS status                                --COL12
  	, NULL AS issecure                              --COL13
    , NULL AS milliseconds                          --COL14
    , NULL AS inviteaccepted                        --COL15
    , NULL AS userscore                             --COL16
    , NULL AS usercomment                           --COL17
    , NULL AS filled                                --COL18
    , NULL AS allowcontact                          --COL19
    , NULL AS cancelled                             --COL20

	, NULL AS derivedOrigin                         --COL21
	, NULL AS source		                        --COL22
	, NULL AS isadmin								--COL23
FROM
	{{ ref('tier2_vidyard_users') }} vut2
  	JOIN {{ ref('stg_vidyard_players') }} p
  		ON p.organizationid = vut2.organizationid AND p.ownerid = vut2.userid

UNION ALL

-------------------------------------------------------------
--					VIDEOS
-------------------------------------------------------------
SELECT
  	'video' AS entity                              --COL01
	, vut2.userid                                  --COL02
    , vut2.organizationid                          --COL03
  	, p.playerid AS entityid                       --COL04
  	, v.videoid AS childentityid                   --COL05
  	, v.createddate AS createddate                   --COL06
  	, v.updateddate AS updateddate                   --COL07
  	, v.videotype AS type                          --COL08
  	, v.createdbyclientid                          --COL09
    , NULL AS uuid                                 --COL10
  	, v.origin                                     --COL11
  	, v.status                                     --COL12
  	, v.issecure                                   --COL13
  	, v.milliseconds                               --COL14
    , NULL AS inviteaccepted                       --COL15
    , NULL AS userscore                            --COL16
    , NULL AS usercomment                          --COL17
    , NULL AS filled                               --COL18
    , NULL AS allowcontact                         --COL19
    , NULL AS cancelled                            --COL20

	, v.derivedOrigin                         	   --COL21
	, v.source		                               --COL22
	, NULL AS isadmin								--COL23
FROM
	{{ ref('tier2_vidyard_users') }} vut2
  	JOIN {{ ref('stg_vidyard_players') }} p
  		ON p.organizationid = vut2.organizationid AND p.ownerid = vut2.userid
  	JOIN {{ ref('stg_vidyard_chapters') }} c
  		ON c.playerid = p.playerid
  	JOIN {{ ref('stg_vidyard_videos') }} v
  		ON v.videoid = c.videoid AND v.organizationid = vut2.organizationid AND v.userid = vut2.userid

UNION ALL

-------------------------------------------------------------
--					USER GROUPS
-------------------------------------------------------------
SELECT
  	'user group' AS entity                         --COL01
	, vut2.userid                                  --COL02
    , vut2.organizationid                          --COL03
  	, ug.groupid AS entityid                       --COL04
  	, null AS childentityid                        --COL05
  	, ug.createddate                               --COL06
  	, ug.updateddate                               --COL07
  	, NULL AS type                                 --COL08
  	, NULL AS createdbyclientid                    --COL09
  	, NULL AS uuid                                 --COL10
  	, NULL AS origin                               --COL11
    , NULL AS status                               --COL12
    , NULL AS issecure                             --COL13
    , NULL AS milliseconds                         --COL14
  	, ug.inviteaccepted                            --COL15
    , NULL AS userscore                            --COL16
    , NULL AS usercomment                          --COL17
    , NULL AS filled                               --COL18
    , NULL AS allowcontact                         --COL19
    , NULL AS cancelled                            --COL20

	, NULL AS derivedOrigin                       --COL21
	, NULL AS source		                       --COL22
	, NULL AS isadmin								--COL23
FROM
	{{ ref('tier2_vidyard_users') }} vut2
  	JOIN {{ ref('stg_vidyard_user_groups') }} ug
  		ON ug.userid = vut2.userid AND ug.organizationid = vut2.organizationid

UNION ALL

-------------------------------------------------------------
--					TEAM MEMBERSHIPS
-------------------------------------------------------------
SELECT
  	'team' AS entity                               --COL01
	, vut2.userid                                  --COL02
    , t.accountid AS organizationid                --COL03
  	, t.teamid AS entityid                         --COL04
  	, tm.teammembershipid AS childentityid         --COL05
  	, tm.createddate as createddate                  --COL06
  	, tm.updateddate as updateddate                  --COL07
  	, vut2.orgtype AS type                         --COL08
  	, NULL AS createdbyclientid                    --COL09
  	, NULL AS uuid                                 --COL10
  	, NULL AS origin                               --COL11
    , NULL AS status                               --COL12
    , NULL AS issecure                             --COL13
    , NULL AS milliseconds                         --COL14
  	, tm.inviteaccepted                            --COL15
    , NULL AS userscore                            --COL16
    , NULL AS usercomment                          --COL17
    , NULL AS filled                               --COL18
    , NULL AS allowcontact                         --COL19
    , NULL AS cancelled                            --COL20

	, NULL AS derivedOrigin                       --COL21
	, NULL AS source		                       --COL22
	, t.isadmin		        					   --COL23
FROM
	{{ ref('tier2_vidyard_users') }} vut2
  	JOIN {{ ref('stg_vidyard_team_memberships') }} tm
  		ON tm.userid = vut2.userid
  	JOIN {{ ref('stg_vidyard_teams') }} t
  		ON t.teamid = tm.teamid and t.accountid = vut2.accountid 
		  
UNION ALL

-------------------------------------------------------------
--					NPS SURVEY
-------------------------------------------------------------
SELECT
  	'survey' AS entity                             --COL01
	, vut2.userid                                  --COL02
    , vut2.organizationid                          --COL03
  	, svy.npssurveyid AS entityid                  --COL04
  	, NULL AS childentityid                        --COL05
  	, svy.createddate as createddate                 --COL06
  	, svy.updateddate as updateddate                 --COL07
  	, svy.surveytype AS type                       --COL08
  	, NULL AS createdbyclientid                    --COL09
  	, NULL AS uuid                                 --COL10
  	, NULL AS origin                               --COL11
    , NULL AS status                               --COL12
    , NULL AS issecure                             --COL13
    , NULL AS milliseconds                         --COL14
  	, NULL AS inviteaccepted                       --COL15
    , userscore                                    --COL16
    , usercomment                                  --COL17
    , filled                                       --COL18
    , allowcontact                                 --COL19
    , cancelled                                    --COL20

	, NULL AS derivedOrigin                       --COL21
	, NULL AS source		                       --COL22
	, NULL AS isadmin							   --COL23
FROM
	{{ ref('tier2_vidyard_users') }} vut2
    JOIN {{ ref('stg_vidyard_nps_surveys') }} svy
        ON svy.userid = vut2.userid and svy.organizationid = vut2.organizationid
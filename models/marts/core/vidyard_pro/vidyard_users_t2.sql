select
  	o.organizationID
	, email
	, name
	, orgtype
	, parentID
	, o.createdByClientID
	, o.createdDate
	, o.ownerId
	, o.accountId
	, u.userid
	, v.videoid
	, om.viewscount
	, om.videoswithviews
	, om.firstview
  	, added_to_team_time
    , invitation_accepted_time
  	--check vidyard email
    , case 
	    when u.email like '%vidyard.com' then 1
	    else 0
        end as vidyard_email
        --check viewedit email
        , case 
            when u.email like '%viewedit.com' then 1
            else 0
        end as viewedit_email
        --exclude hubspot personal signups per zendesk ticket #85501 and #98583
        , case
            when o.createdbyclientid in ('app.hubspot.com','marketing.hubspot.com','marketing.hubspotqa.com') then 1
            else 0
        end as hubspot_personal_signup
        , case
            when split_part(u.email, '@', 2) like '%gmail.com%'
                or split_part(u.email, '@', 2) like '%yahoo%'
                or split_part(u.email, '@', 2) like '%hotmail%'
                or split_part(u.email, '@', 2) like '%icloud%'
                or split_part(u.email, '@', 2) like '%outlook%'
                or split_part(u.email, '@', 2) like '%googlemail.com%'
                or split_part(u.email, '@', 2) like '%privaterelay.appleid.com%'
                or split_part(u.email, '@', 2) like '%mail.ru%'
                or split_part(u.email, '@', 2) like '%aol.com'
                or split_part(u.email, '@', 2) like '%live.c%'
                or split_part(u.email, '@', 2) like '%live.se'
                or split_part(u.email, '@', 2) like '%synthetics.dtdg.co%'
                or split_part(u.email, '@', 2) like '%me.com'
                or split_part(u.email, '@', 2) like '%msn.com'
                or split_part(u.email, '@', 2) like '%yandex.ru%'
                or split_part(u.email, '@', 2) like '%comcast.net%'
                or split_part(u.email, '@', 2) like '%tsu.ac.th%'
                or split_part(u.email, '@', 2) like '%qq.com%'
                or split_part(u.email, '@', 2) like '%protonmail.com%'
                or split_part(u.email, '@', 2) like '%ggh.goe.go.kr%'
                or split_part(u.email, '@', 2) like '%portoseguro.com.br%'
                or split_part(u.email, '@', 2) like '%mail.com%'
                or split_part(u.email, '@', 2) like '%att.net'
                or split_part(u.email, '@', 2) like '%sbcglobal.net%'
                or split_part(u.email, '@', 2) like '%bk.ru'
                or split_part(u.email, '@', 2) like '%libero.it'
                or split_part(u.email, '@', 2) like '%gmx.de'
                or split_part(u.email, '@', 2) like '%web.de'
                or split_part(u.email, '@', 2) like '%wp.pl'
                or split_part(u.email, '@', 2) like '%getnada.com'
            then 'personal'
            when split_part(u.email, '@', 2) like '%.edu%'
                or split_part(u.email, '@', 2) like '%edu.%'
                or split_part(u.email, '@', 2) like '%education%'
                or split_part(u.email, '@', 2) like '%student%'
                or split_part(u.email, '@', 2) like '%school%'
                or split_part(u.email, '@', 2) like '%k12%'
                or split_part(u.email, '@', 2) like '%mcpsmd.net'
                or split_part(u.email, '@', 2) like '%wrdsb%'
                or split_part(u.email, '@', 2) like '%gotvdsb%'
                or split_part(u.email, '@', 2) like '%cscprovidence.ca%'
                or split_part(u.email, '@', 2) like '%sherrard.us%'
                or split_part(u.email, '@', 2) like '%academy%'
                or split_part(u.email, '@', 2) like '%daytonpublic%'
                or split_part(u.email, '@', 2) like '%msdlt.org%'
                or split_part(u.email, '@', 2) like '%georgiacyber.org%'
                or split_part(u.email, '@', 2) like '%georgiacyber.org%'
                or split_part(u.email, '@', 2) like '%spu.ac.th%'
                or split_part(u.email, '@', 2) like '%ehancock.org%'
                or split_part(u.email, '@', 2) like '%springbranch%'
                or split_part(u.email, '@', 2) like '%esc18.net%'
                or split_part(u.email, '@', 2) like '%district%'
                or split_part(u.email, '@', 2) like '%helixcharter.net%'
                or split_part(u.email, '@', 2) like '%dpsnc.net%'
                or split_part(u.email, '@', 2) like '%nv.ccsd.net%'
                or split_part(u.email, '@', 2) like '%skola.karlskrona.se%'
                or split_part(u.email, '@', 2) like '%spumail.net%'
                or split_part(u.email, '@', 2) like '%cspgno.ca%'
                or split_part(u.email, '@', 2) like '%hdsb.ca%'
                or split_part(u.email, '@', 2) like '%hillsborohawks.net%'
                or split_part(u.email, '@', 2) like '%mesquiteisd.org%'
                or split_part(u.email, '@', 2) like '%hillsborohawks.net%'
                or split_part(u.email, '@', 2) like '%scholar.democracyprep%'
                or split_part(u.email, '@', 2) like '%scsnc.org%'
                or split_part(u.email, '@', 2) like '%epsb.ca%'
                or split_part(u.email, '@', 2) like '%tonacsd.org%'
                or split_part(u.email, '@', 2) like 'forneyisd.net%'
                or split_part(u.email, '@', 2) like '%lehsd.org%'
                or split_part(u.email, '@', 2) like '%oygardenskule.no%'
                or split_part(u.email, '@', 2) like '%jvanier.ca%'
                or split_part(u.email, '@', 2) like '%pdsb.net%'
                or split_part(u.email, '@', 2) like '%lsr7.net%'
                or split_part(u.email, '@', 2) like '%aldine-isd.org%'
                or split_part(u.email, '@', 2) like '%pensacolachs.org%'
                or split_part(u.email, '@', 2) like '%.org%'
                or split_part(u.email, '@', 2) like '%cmcss%'
                or split_part(u.email, '@', 2) like '%mcusd2.com'
                or split_part(u.email, '@', 2) like '%crowleyisdtx.me%'
                or split_part(u.email, '@', 2) like '%gapps.yrdsb.ca%'
                or split_part(u.email, '@', 2) like '%crowleyisdtx.me%'
                or split_part(u.email, '@', 2) like '%skolen%'
                or split_part(u.email, '@', 2) like '%utb.kristianstad.se%'
                or split_part(u.email, '@', 2) like '%franklinps.net%'
                or split_part(u.email, '@', 2) like '%mon-avenir.ca%'
                or split_part(u.email, '@', 2) like '%utb.bollebygd.se%'
                or split_part(u.email, '@', 2) like '%vru.ac.th%'
                or split_part(u.email, '@', 2) like '%fcsdchromebooks.us'
                or split_part(u.email, '@', 2) like '%midlandisd.net'
                or split_part(u.email, '@', 2) like '%stu.sandi.net'
                or split_part(u.email, '@', 2) like '%learner.nesd.ca'
                or split_part(u.email, '@', 2) like '%misd.gs'
                or split_part(u.email, '@', 2) like '%wcdsb.ca'
                or split_part(u.email, '@', 2) like '%topamail.com'
                or split_part(u.email, '@', 2) like '%rrvsd.ca'
                or split_part(u.email, '@', 2) like '%bilanyc.net'
                or split_part(u.email, '@', 2) like '%my.htoakville.ca'
                or split_part(u.email, '@', 2) like '%skola.botkyrka.se'
                or split_part(u.email, '@', 2) like '%ocdsb.ca'
                or split_part(u.email, '@', 2) like '%egusd.net'
                or split_part(u.email, '@', 2) like '%mymail.lausd.net'
                or split_part(u.email, '@', 2) like '%mp.edzone.net'
                or split_part(u.email, '@', 2) like '%tm.ac.th'
                or split_part(u.email, '@', 2) like '%stfxavier.ca'
                or split_part(u.email, '@', 2) like '%wacohi.net'
                or split_part(u.email, '@', 2) like '%chicousd.net'
                or split_part(u.email, '@', 2) like '%alumnos.uai.cl'
                or split_part(u.email, '@', 2) like '%xtec.cat'
                or split_part(u.email, '@', 2) like '%primolevitorino.it'
                or split_part(u.email, '@', 2) like '%skola%'
                or split_part(u.email, '@', 2) like '%kerrvilleisd.net'
                or split_part(u.email, '@', 2) like '%scdsb.on.ca'
                or split_part(u.email, '@', 2) like '%ganduxer.escolateresiana.com'
                or split_part(u.email, '@', 2) like '%cvusd.co'
                or split_part(u.email, '@', 2) like '%educacion.navarra.es'
                or split_part(u.email, '@', 2) like '%uwaterloo.ca'
                or split_part(u.email, '@', 2) like '%my.ctk.ca'
                or split_part(u.email, '@', 2) like '%sausdlearns.net'
                or split_part(u.email, '@', 2) like '%txkisd.net'
                or split_part(u.email, '@', 2) like '%conestogac.on.ca'
                or split_part(u.email, '@', 2) like '%gapp.uddevalla.se'
                or split_part(u.email, '@', 2) like '%elev.solleftea.se'
                or split_part(u.email, '@', 2) like '%ryerson.ca'
                or split_part(u.email, '@', 2) like '%ualberta.ca'
                or split_part(u.email, '@', 2) like '%uottawa.ca'
                or split_part(u.email, '@', 2) like '%bdsnet.com.ar'
                or split_part(u.email, '@', 2) like '%ycjusd.us'
                or split_part(u.email, '@', 2) like '%mylaurier.ca'
                or split_part(u.email, '@', 2) like '%smcdsb.on.ca'
                or split_part(u.email, '@', 2) like '%go.ugr.es'
                or split_part(u.email, '@', 2) like '%tcdsb.ca'
                or split_part(u.email, '@', 2) like '%learn.cssd.ab.ca'
                or split_part(u.email, '@', 2) like '%ecolecatholique.ca'
                or split_part(u.email, '@', 2) like '%btgem.mx'
                or split_part(u.email, '@', 2) like '%engelska.se'
                or split_part(u.email, '@', 2) like '%aluno.humboldt.com.br'
                or split_part(u.email, '@', 2) like '%reddford.house'
                or split_part(u.email, '@', 2) like '%colegiobs.eu'
                or split_part(u.email, '@', 2) like '%gm.fsd38.ab.ca'
                or split_part(u.email, '@', 2) like '%psu.ac.th'
                or split_part(u.email, '@', 2) like '%stu.eau.ac.th'
                or split_part(u.email, '@', 2) like '%pkru.ac.th'
                or split_part(u.email, '@', 2) like '%gafe.karlskrona.se'
                or split_part(u.email, '@', 2) like '%silkeskole.dk'
                or split_part(u.email, '@', 2) like '%ecsd.me'
                or split_part(u.email, '@', 2) like '%skole%'
                or split_part(u.email, '@', 2) like '%ugcloud.ca'
                or split_part(u.email, '@', 2) like '%phasd.us%'
                or split_part(u.email, '@', 2) like '%pcti.mobi%'
                or split_part(u.email, '@', 2) like '%niagaracatholic.ca'
                or split_part(u.email, '@', 2) like '%elev.norrkoping.se'
                or split_part(u.email, '@', 2) like '%ac.th'
                or split_part(u.email, '@', 2) like '%tdsb%'
                or split_part(u.email, '@', 2) like '%psdblogs.ca'
                or split_part(u.email, '@', 2) like '%escolamontagut.cat'
                or split_part(u.email, '@', 2) like '%sd23.bc.ca'
                or split_part(u.email, '@', 2) like '%educbe.ca'
                or split_part(u.email, '@', 2) like '%ed.amdsb.ca'
                or split_part(u.email, '@', 2) like '%stu.bisd.us'
                or split_part(u.email, '@', 2) like '%nmusd.us'
                or split_part(u.email, '@', 2) like '%.net'
            then 'education'
            else 'business'
        end as business_domain
		, case 
  				when ug.num_groups > 0 then 2
  				when tm.num_teams > 0 then 2
  				when tm.num_teammemberships > 0 then 2
  				else 0
  		  end as has_group_or_team_id
  		, case
            when o.ownerid = u.userid and o.orgtype = 'self_serve' then 1
            else 0
           end as has_personal_account
        , case
            when (o.ownerid = u.userid and o.orgtype = 'self_serve' and o.organizationid != o.accountid) then 4
            else 0
           end as linked_to_parent_account
  		, case
            when (has_personal_account + linked_to_parent_account + has_group_or_team_id) = 0 then 'Orphan'
            when (has_personal_account + linked_to_parent_account + has_group_or_team_id) = 1 then 'Standalone'
            when (has_personal_account + linked_to_parent_account + has_group_or_team_id) = 2 then 'Enterprise Only'
            when (has_personal_account + linked_to_parent_account + has_group_or_team_id) = 3 then 'Hybrid'
            when (has_personal_account + linked_to_parent_account + has_group_or_team_id) = 7 then 'Enterprise Personal'
  			
  			when (has_personal_account + linked_to_parent_account + has_group_or_team_id) = 4 then 'Linked to Parent Account only'
  			when (has_personal_account + linked_to_parent_account + has_group_or_team_id) = 5 then 'Has Personal Account & Linked to Parent Account'
  			when (has_personal_account + linked_to_parent_account + has_group_or_team_id) = 6 then 'Has Group/Team membership id & Linked to Parent Account'
  
            else cast((has_personal_account + linked_to_parent_account + has_group_or_team_id) as varchar(10))
          end as user_type
        , case
            when o.createdByClientID = 'android.vidyard.com'
                then 'Android'
            when o.createdByClientID = 'govideo-mobile.vidyard.com'
                then 'iOS'
            when o.createdByClientID = 'app.slack.com'
                then 'Slack'
            when o.createdByClientID not in ('secure.vidyard.com','auth.viewedit.com','edge-extension.vidyard.com')
                then 'Partner App'
            when o.createdByClientID = 'edge-extension.vidyard.com'
                then 'Edge'
          end as signup_source
from 
dbt_vidyard_master.stg_vidyard_organizations o
join dbt_vidyard_master.stg_vidyard_users u
	on o.ownerid = u.userid and u.userid is not null and o.organizationid is not null
left join (
  		select organizationid, userid, count(distinct groupid) as num_groups
  		from dbt_vidyard_master.stg_vidyard_user_groups ug
  		where ug.inviteaccepted = 'True' and ug.groupid is not null
  		group by 1, 2
	) ug
    on ug.organizationid = o.organizationid and ug.userid = u.userid
left join 
		(
		  select 
              accountid
              , userid
              , tm.createdat as added_to_team_time
              , tm.updatedat as invitation_accepted_time
              , count(distinct t.teamid) as num_teams
              , count(distinct teammembershipid) as num_teammemberships
		  from dbt_vidyard_master.stg_vidyard_teams t
		  join dbt_vidyard_master.stg_vidyard_team_memberships tm
			on t.teamid = tm.teamid
          group by 1, 2, 3, 4
        ) tm
	on  tm.accountid = o.accountid and tm.userid = u.userid   
left join dbt_vidyard_master.stg_vidyard_org_metrics om
    on om.organizationid = o.organizationid
left join dbt_vidyard_master.stg_vidyard_videos v
    on v.videoid is not null and v.organizationid = o.organizationid and v.userid = u.userid
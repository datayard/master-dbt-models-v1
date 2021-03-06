SELECT 
		sfdc_campaignmember.id as campaignMemberId
		, sfdc_campaignmember.createddate as createdDateGMT
		, convert_timezone('EST', sfdc_campaignmember.createddate::timestamp) as createdDateEST
		, sfdc_campaignmember.isdeleted as isDeleted
		, sfdc_campaignmember.campaignid as campaignId
		, sfdc_campaignmember.type as type
		, sfdc_campaignmember.leadid as leadId
		, sfdc_campaignmember.contactid as contactId
		, sfdc_campaignmember.status as status
		, sfdc_campaignmember.name as campaignMemberName
		, sfdc_campaignmember.email as email
        , sfdc_campaignmember.account_region__c as accountregion
        , sfdc_campaignmember.account_type__c as accountType
	, case
            when split_part(sfdc_campaignmember.email, '@', 2) like '%gmail.com%'
                or split_part(sfdc_campaignmember.email, '@', 2) like '%yahoo%'
                or split_part(sfdc_campaignmember.email, '@', 2) like '%hotmail%'
                or split_part(sfdc_campaignmember.email, '@', 2) like '%icloud%'
                or split_part(sfdc_campaignmember.email, '@', 2) like '%outlook%'
                or split_part(sfdc_campaignmember.email, '@', 2) like '%googlemail.com%'
                or split_part(sfdc_campaignmember.email, '@', 2) like '%privaterelay.appleid.com%'
                or split_part(sfdc_campaignmember.email, '@', 2) like '%mail.ru%'
                or split_part(sfdc_campaignmember.email, '@', 2) like '%aol.com'
                or split_part(sfdc_campaignmember.email, '@', 2) like '%live.c%'
                or split_part(sfdc_campaignmember.email, '@', 2) like '%live.se'
                or split_part(sfdc_campaignmember.email, '@', 2) like '%synthetics.dtdg.co%'
                or split_part(sfdc_campaignmember.email, '@', 2) like '%me.com'
                or split_part(sfdc_campaignmember.email, '@', 2) like '%msn.com'
                or split_part(sfdc_campaignmember.email, '@', 2) like '%yandex.ru%'
                or split_part(sfdc_campaignmember.email, '@', 2) like '%comcast.net%'
                or split_part(sfdc_campaignmember.email, '@', 2) like '%tsu.ac.th%'
                or split_part(sfdc_campaignmember.email, '@', 2) like '%qq.com%'
                or split_part(sfdc_campaignmember.email, '@', 2) like '%protonmail.com%'
                or split_part(sfdc_campaignmember.email, '@', 2) like '%ggh.goe.go.kr%'
                or split_part(sfdc_campaignmember.email, '@', 2) like '%portoseguro.com.br%'
                or split_part(sfdc_campaignmember.email, '@', 2) like '%mail.com%'
                or split_part(sfdc_campaignmember.email, '@', 2) like '%att.net'
                or split_part(sfdc_campaignmember.email, '@', 2) like '%sbcglobal.net%'
                or split_part(sfdc_campaignmember.email, '@', 2) like '%bk.ru'
                or split_part(sfdc_campaignmember.email, '@', 2) like '%libero.it'
                or split_part(sfdc_campaignmember.email, '@', 2) like '%gmx.de'
                or split_part(sfdc_campaignmember.email, '@', 2) like '%web.de'
                or split_part(sfdc_campaignmember.email, '@', 2) like '%wp.pl'
                or split_part(sfdc_campaignmember.email, '@', 2) like '%getnada.com'
                or split_part(sfdc_campaignmember.email, '@', 2) like '%tutanota.com%'
                or split_part(sfdc_campaignmember.email, '@', 2) like '%gmx.com%'
            then 'personal'
            when split_part(sfdc_campaignmember.email, '@', 2) like '%.edu%'
                or split_part(sfdc_campaignmember.email, '@', 2) like '%edu.%'
                or split_part(sfdc_campaignmember.email, '@', 2) like '%education%'
                or split_part(sfdc_campaignmember.email, '@', 2) like '%student%'
                or split_part(sfdc_campaignmember.email, '@', 2) like '%school%'
                or split_part(sfdc_campaignmember.email, '@', 2) like '%k12%'
                or split_part(sfdc_campaignmember.email, '@', 2) like '%mcpsmd.net'
                or split_part(sfdc_campaignmember.email, '@', 2) like '%wrdsb%'
                or split_part(sfdc_campaignmember.email, '@', 2) like '%gotvdsb%'
                or split_part(sfdc_campaignmember.email, '@', 2) like '%cscprovidence.ca%'
                or split_part(sfdc_campaignmember.email, '@', 2) like '%sherrard.us%'
                or split_part(sfdc_campaignmember.email, '@', 2) like '%academy%'
                or split_part(sfdc_campaignmember.email, '@', 2) like '%daytonpublic%'
                or split_part(sfdc_campaignmember.email, '@', 2) like '%msdlt.org%'
                or split_part(sfdc_campaignmember.email, '@', 2) like '%georgiacyber.org%'
                or split_part(sfdc_campaignmember.email, '@', 2) like '%georgiacyber.org%'
                or split_part(sfdc_campaignmember.email, '@', 2) like '%spu.ac.th%'
                or split_part(sfdc_campaignmember.email, '@', 2) like '%ehancock.org%'
                or split_part(sfdc_campaignmember.email, '@', 2) like '%springbranch%'
                or split_part(sfdc_campaignmember.email, '@', 2) like '%esc18.net%'
                or split_part(sfdc_campaignmember.email, '@', 2) like '%district%'
                or split_part(sfdc_campaignmember.email, '@', 2) like '%helixcharter.net%'
                or split_part(sfdc_campaignmember.email, '@', 2) like '%dpsnc.net%'
                or split_part(sfdc_campaignmember.email, '@', 2) like '%nv.ccsd.net%'
                or split_part(sfdc_campaignmember.email, '@', 2) like '%skola.karlskrona.se%'
                or split_part(sfdc_campaignmember.email, '@', 2) like '%spumail.net%'
                or split_part(sfdc_campaignmember.email, '@', 2) like '%cspgno.ca%'
                or split_part(sfdc_campaignmember.email, '@', 2) like '%hdsb.ca%'
                or split_part(sfdc_campaignmember.email, '@', 2) like '%hillsborohawks.net%'
                or split_part(sfdc_campaignmember.email, '@', 2) like '%mesquiteisd.org%'
                or split_part(sfdc_campaignmember.email, '@', 2) like '%hillsborohawks.net%'
                or split_part(sfdc_campaignmember.email, '@', 2) like '%scholar.democracyprep%'
                or split_part(sfdc_campaignmember.email, '@', 2) like '%scsnc.org%'
                or split_part(sfdc_campaignmember.email, '@', 2) like '%epsb.ca%'
                or split_part(sfdc_campaignmember.email, '@', 2) like '%tonacsd.org%'
                or split_part(sfdc_campaignmember.email, '@', 2) like 'forneyisd.net%'
                or split_part(sfdc_campaignmember.email, '@', 2) like '%lehsd.org%'
                or split_part(sfdc_campaignmember.email, '@', 2) like '%oygardenskule.no%'
                or split_part(sfdc_campaignmember.email, '@', 2) like '%jvanier.ca%'
                or split_part(sfdc_campaignmember.email, '@', 2) like '%pdsb.net%'
                or split_part(sfdc_campaignmember.email, '@', 2) like '%lsr7.net%'
                or split_part(sfdc_campaignmember.email, '@', 2) like '%aldine-isd.org%'
                or split_part(sfdc_campaignmember.email, '@', 2) like '%pensacolachs.org%'
                or split_part(sfdc_campaignmember.email, '@', 2) like '%.org%'
                or split_part(sfdc_campaignmember.email, '@', 2) like '%cmcss%'
                or split_part(sfdc_campaignmember.email, '@', 2) like '%mcusd2.com'
                or split_part(sfdc_campaignmember.email, '@', 2) like '%crowleyisdtx.me%'
                or split_part(sfdc_campaignmember.email, '@', 2) like '%gapps.yrdsb.ca%'
                or split_part(sfdc_campaignmember.email, '@', 2) like '%crowleyisdtx.me%'
                or split_part(sfdc_campaignmember.email, '@', 2) like '%skolen%'
                or split_part(sfdc_campaignmember.email, '@', 2) like '%utb.kristianstad.se%'
                or split_part(sfdc_campaignmember.email, '@', 2) like '%franklinps.net%'
                or split_part(sfdc_campaignmember.email, '@', 2) like '%mon-avenir.ca%'
                or split_part(sfdc_campaignmember.email, '@', 2) like '%utb.bollebygd.se%'
                or split_part(sfdc_campaignmember.email, '@', 2) like '%vru.ac.th%'
                or split_part(sfdc_campaignmember.email, '@', 2) like '%fcsdchromebooks.us'
                or split_part(sfdc_campaignmember.email, '@', 2) like '%midlandisd.net'
                or split_part(sfdc_campaignmember.email, '@', 2) like '%stu.sandi.net'
                or split_part(sfdc_campaignmember.email, '@', 2) like '%learner.nesd.ca'
                or split_part(sfdc_campaignmember.email, '@', 2) like '%misd.gs'
                or split_part(sfdc_campaignmember.email, '@', 2) like '%wcdsb.ca'
                or split_part(sfdc_campaignmember.email, '@', 2) like '%topamail.com'
                or split_part(sfdc_campaignmember.email, '@', 2) like '%rrvsd.ca'
                or split_part(sfdc_campaignmember.email, '@', 2) like '%bilanyc.net'
                or split_part(sfdc_campaignmember.email, '@', 2) like '%my.htoakville.ca'
                or split_part(sfdc_campaignmember.email, '@', 2) like '%skola.botkyrka.se'
                or split_part(sfdc_campaignmember.email, '@', 2) like '%ocdsb.ca'
                or split_part(sfdc_campaignmember.email, '@', 2) like '%egusd.net'
                or split_part(sfdc_campaignmember.email, '@', 2) like '%mymail.lausd.net'
                or split_part(sfdc_campaignmember.email, '@', 2) like '%mp.edzone.net'
                or split_part(sfdc_campaignmember.email, '@', 2) like '%tm.ac.th'
                or split_part(sfdc_campaignmember.email, '@', 2) like '%stfxavier.ca'
                or split_part(sfdc_campaignmember.email, '@', 2) like '%wacohi.net'
                or split_part(sfdc_campaignmember.email, '@', 2) like '%chicousd.net'
                or split_part(sfdc_campaignmember.email, '@', 2) like '%alumnos.uai.cl'
                or split_part(sfdc_campaignmember.email, '@', 2) like '%xtec.cat'
                or split_part(sfdc_campaignmember.email, '@', 2) like '%primolevitorino.it'
                or split_part(sfdc_campaignmember.email, '@', 2) like '%skola%'
                or split_part(sfdc_campaignmember.email, '@', 2) like '%kerrvilleisd.net'
                or split_part(sfdc_campaignmember.email, '@', 2) like '%scdsb.on.ca'
                or split_part(sfdc_campaignmember.email, '@', 2) like '%ganduxer.escolateresiana.com'
                or split_part(sfdc_campaignmember.email, '@', 2) like '%cvusd.co'
                or split_part(sfdc_campaignmember.email, '@', 2) like '%educacion.navarra.es'
                or split_part(sfdc_campaignmember.email, '@', 2) like '%uwaterloo.ca'
                or split_part(sfdc_campaignmember.email, '@', 2) like '%my.ctk.ca'
                or split_part(sfdc_campaignmember.email, '@', 2) like '%sausdlearns.net'
                or split_part(sfdc_campaignmember.email, '@', 2) like '%txkisd.net'
                or split_part(sfdc_campaignmember.email, '@', 2) like '%conestogac.on.ca'
                or split_part(sfdc_campaignmember.email, '@', 2) like '%gapp.uddevalla.se'
                or split_part(sfdc_campaignmember.email, '@', 2) like '%elev.solleftea.se'
                or split_part(sfdc_campaignmember.email, '@', 2) like '%ryerson.ca'
                or split_part(sfdc_campaignmember.email, '@', 2) like '%ualberta.ca'
                or split_part(sfdc_campaignmember.email, '@', 2) like '%uottawa.ca'
                or split_part(sfdc_campaignmember.email, '@', 2) like '%bdsnet.com.ar'
                or split_part(sfdc_campaignmember.email, '@', 2) like '%ycjusd.us'
                or split_part(sfdc_campaignmember.email, '@', 2) like '%mylaurier.ca'
                or split_part(sfdc_campaignmember.email, '@', 2) like '%smcdsb.on.ca'
                or split_part(sfdc_campaignmember.email, '@', 2) like '%go.ugr.es'
                or split_part(sfdc_campaignmember.email, '@', 2) like '%tcdsb.ca'
                or split_part(sfdc_campaignmember.email, '@', 2) like '%learn.cssd.ab.ca'
                or split_part(sfdc_campaignmember.email, '@', 2) like '%ecolecatholique.ca'
                or split_part(sfdc_campaignmember.email, '@', 2) like '%btgem.mx'
                or split_part(sfdc_campaignmember.email, '@', 2) like '%engelska.se'
                or split_part(sfdc_campaignmember.email, '@', 2) like '%aluno.humboldt.com.br'
                or split_part(sfdc_campaignmember.email, '@', 2) like '%reddford.house'
                or split_part(sfdc_campaignmember.email, '@', 2) like '%colegiobs.eu'
                or split_part(sfdc_campaignmember.email, '@', 2) like '%gm.fsd38.ab.ca'
                or split_part(sfdc_campaignmember.email, '@', 2) like '%psu.ac.th'
                or split_part(sfdc_campaignmember.email, '@', 2) like '%stu.eau.ac.th'
                or split_part(sfdc_campaignmember.email, '@', 2) like '%pkru.ac.th'
                or split_part(sfdc_campaignmember.email, '@', 2) like '%gafe.karlskrona.se'
                or split_part(sfdc_campaignmember.email, '@', 2) like '%silkeskole.dk'
                or split_part(sfdc_campaignmember.email, '@', 2) like '%ecsd.me'
                or split_part(sfdc_campaignmember.email, '@', 2) like '%skole%'
                or split_part(sfdc_campaignmember.email, '@', 2) like '%ugcloud.ca'
                or split_part(sfdc_campaignmember.email, '@', 2) like '%phasd.us%'
                or split_part(sfdc_campaignmember.email, '@', 2) like '%pcti.mobi%'
                or split_part(sfdc_campaignmember.email, '@', 2) like '%niagaracatholic.ca'
                or split_part(sfdc_campaignmember.email, '@', 2) like '%elev.norrkoping.se'
                or split_part(sfdc_campaignmember.email, '@', 2) like '%ac.th'
                or split_part(sfdc_campaignmember.email, '@', 2) like '%tdsb%'
                or split_part(sfdc_campaignmember.email, '@', 2) like '%psdblogs.ca'
                or split_part(sfdc_campaignmember.email, '@', 2) like '%escolamontagut.cat'
                or split_part(sfdc_campaignmember.email, '@', 2) like '%sd23.bc.ca'
                or split_part(sfdc_campaignmember.email, '@', 2) like '%educbe.ca'
                or split_part(sfdc_campaignmember.email, '@', 2) like '%ed.amdsb.ca'
                or split_part(sfdc_campaignmember.email, '@', 2) like '%stu.bisd.us'
                or split_part(sfdc_campaignmember.email, '@', 2) like '%nmusd.us'
                or split_part(sfdc_campaignmember.email, '@', 2) like '%.net'
            then 'education'
            else 'business'
        end as domainType		
		, sfdc_campaignmember.campaign_sourced_by__c as campaignSourcedBy
        , sfdc_campaignmember.inquiry__c as inquiry
		, sfdc_campaignmember.mql__c as mql
		, sfdc_campaignmember.mql_date_and_time__c as mqlDateGMT
		, convert_timezone('EST', sfdc_campaignmember.mql_date_and_time__c::timestamp) as mqlDateEST
		, sfdc_campaignmember.sal__c as sal
		, sfdc_campaignmember.sal_date_and_time__c as salDateGMT
		, convert_timezone('EST', sfdc_campaignmember.sal_date_and_time__c::timestamp)as salDateEST
		, sfdc_campaignmember.sql__c as sql
		, sfdc_campaignmember.sql_date_and_time__c as sqlDateGMT
		, convert_timezone('EST', sfdc_campaignmember.sql_date_and_time__c::timestamp)as sqlDateEST
		, sfdc_campaignmember.sqo__c as sqo
		, sfdc_campaignmember.sqo_date__c as sqoDate
		, sfdc_campaignmember.sqo_lost__c as sqoLost
		, sfdc_campaignmember.sqo_lost_date__c as sqoLostDate
		, sfdc_campaignmember.won__c as won
		, sfdc_campaignmember.opportunity_closed_won_date__c as opportunityClosedWonDate
		, sfdc_campaignmember.opportunity__c as opportunityId
		, sfdc_campaignmember.status_reason__c as statusReason
		, sfdc_campaignmember.current_status_reason__c as currentStatusReason
        , sfdc_campaignmember.response_status__c as responseStatus
        , sfdc_campaignmember.persona_formula__c as persona

 FROM 
 		{{ source ('salesforce_production' , 'campaignmember') }} as sfdc_campaignmember
 WHERE
 		TRUE

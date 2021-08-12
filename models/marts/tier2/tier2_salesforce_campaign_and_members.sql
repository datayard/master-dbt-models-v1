SELECT
    sfcm.*
    , sfc.isdeleted AS campaign_isdeleted
    , sfc.name AS campaign_name
    , sfc.parentid AS campaign_parentid
    , sfc.type AS campaign_type
    , sfc.status AS campaign_status
    , sfc.startdate AS campaign_startdate
    , sfc.enddate AS campaign_enddate
    , sfc.expectedrevenue AS campaign_expectedrevenue
    , sfc.budgetedcost AS campaign_budgetedcost
    , sfc.actualcost AS campaign_actualcost
    , sfc.isactive AS campaign_isactive
    , sfc.description AS campaign_description
    , sfc.createddate AS campaign_createddate
    , sfc.mediatype AS campaign_mediatype
    , sfc.inboundvsoutbound AS campaign_inboundvsoutbound
    , sfc.cta AS campaign_cta
    , sfc.ctasubtype AS campaign_ctasubtype
    , sfc.channelpicklist AS campaign_channelpicklist
    , sfc.channeldetail AS campaign_channeldetail
    , sfc.channelmediatype AS campaign_channelmediatype
    , sfc.campaignsourcedby AS campaign_sourcedby
    , sfc.responsetype AS campaign_responsetype
FROM
    {{ ref('stg_salesforce_campaignmember') }} sfcm
    JOIN {{ ref('stg_salesforce_campaign') }} sfc
        ON sfc.campaignid = sfcm.campaignid
        
SELECT DISTINCT
    c.email 
    , date(ch.createdDate) as conversationDate
    , c.customProductPage
    , c.customLocation
    , c.customUpgradePathsExperience
FROM
    {{ref ('stg_intercom_contact_history')}} c
        JOIN {{ref ('stg_intercom_conversation_contact_history')}} cch 
            ON c.contactId = cch.contactid
        JOIN {{ref ('stg_intercom_conversation_history')}} ch 
            ON cch.conversationid = ch.conversationid
WHERE c.customProductPage is not null
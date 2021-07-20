SELECT entity
         , vet2.userid
         , vet2.organizationid
         , entityid
         , childentityid
         , createddate
         , updateddate
         , type
         , createdbyclientid
         , uuid
         , origin
         , derived_origin
         , status
         , issecure
         , milliseconds
         , inviteaccepted
         , userscore
         , usercomment
         , filled
         , allowcontact
         , cancelled
         , eventid
         , vid_userid
         , ht2.userid AS heapuserid
         , sessionid
         , sessiontime
         , eventtime
         , landingpage
         , domain
         , channels
         , path
         , derived_channel
         , column_for_distribution
         , tracker
    FROM {{ ref('tier2_vidyard_user_entities') }} vet2
             JOIN {{ ref('tier2_heap') }} ht2
                  ON ht2.vid_userid = vet2.userid
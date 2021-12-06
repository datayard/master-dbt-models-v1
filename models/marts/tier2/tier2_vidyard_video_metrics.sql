    SELECT 
        v.userid
         , v.organizationid
         , v.videoid
         , v.createddate
         , v.updateddate
         , v.videotype
         , v.createdbyclientid
         , v.origin
         , v.status
         , v.issecure
         , v.milliseconds
         , v.derivedOrigin
         , v.source
         , vm.viewsCount
         , vm.uniqueViewers
         , vm.identifiedViewers
         , vm.identifiedViews
    FROM 	{{ ref('stg_vidyard_videos')}} as v 
            JOIN {{ ref('stg_vidyard_video_metrics')}} as vm            
                ON v.videoid = vm.videoid
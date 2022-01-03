SELECT
    tahistory.ad_group_id as adgroupID,
    tahistory.ad_id as adID,
    tahistory.description as description1,
    tahistory.descripton_2 as description2,
    tahistory.head_line_part_1 as headline1,
    tahistory.head_line_part_2 as headline2,
    tahistory.head_line_part_3 as headline3,
    tahistory.path_1 as path1,
    tahistory.path_2 as path2
    
FROM 
	{{ source('google_adwords', 'expanded_text_ad_history') }} as tahistory
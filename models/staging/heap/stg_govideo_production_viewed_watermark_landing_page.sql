SELECT
        viewed_watermark_landing_page.event_id as eventID,
        viewed_watermark_landing_page.session_id as sessionId,
        viewed_watermark_landing_page.session_time as sessionTime,
        viewed_watermark_landing_page.user_id as userID,
        viewed_watermark_landing_page.time as eventTime,
        viewed_watermark_landing_page.country as country,
        viewed_watermark_landing_page.region as region,
        viewed_watermark_landing_page.city as city,
        viewed_watermark_landing_page.platform as platform,
        viewed_watermark_landing_page.device_type as deviceType,
        viewed_watermark_landing_page.browser as browser,
        viewed_watermark_landing_page.referrer as referrer,
        viewed_watermark_landing_page.landing_page as landingPage,
        viewed_watermark_landing_page.landing_page_query as landingPageQuery,
        viewed_watermark_landing_page.query as query,
        viewed_watermark_landing_page.domain as domain,
        viewed_watermark_landing_page.path as path,
  case 
  when country in ('Afghanistan','Åland Islands','Albania','Algeria','Andorra','Angola','Antarctica','Armenia','Austria','Azerbaijan','Bahrain','Belarus','Belgium','Benin','Bosnia and Herzegovina','Botswana','Bouvet Island','Bulgaria','Burkina Faso','Burundi','Cabo Verde','Cameroon',
                'Cape Verde','Central African Republic','Chad','Collectivity of Saint Martin','Saint Martin','Comoros','Congo','DR Congo','Republic of the Congo','Congo Republic','The Democratic Republic of Congo','Cook Islands','Côte d Ivoire','Croatia','Curaçao','Cyprus','Czechia','Czech Republic','Denmark',
                'Djibouti','Egypt','Equatorial Guinea','Eritrea','Estonia','Ethiopia','Faroe Islands','Finland','France','French Guiana','French Polynesia','French Southern Territories','Gabon','Gambia','Georgia','Germany',
                'Ghana','Gibraltar','Greece','Greenland','Guadeloupe','Guernsey','Guinea','Guinea-Bissau','Heard Island and McDonald Islands','Holy See','Hungary','Iceland','Iran','Islamic Republic of Iran','Iraq','Ireland','Isle Of Man','Israel','Italy','Ivory Coast',
                'Jersey','Jordan','Hashemite Kingdom of Jordan','Kazakhstan','Kenya','Kuwait','Kyrgyzstan','Latvia','Lebanon','Lesotho','Liberia','Libya','Liechtenstein','Lithuania','Republic of Lithuania','Luxembourg','Macedonia','North Macedonia','The former Yugoslav Republic of Macedonia','Madagascar',
                'Malawi','Mali','Malta','Martinique','Mauritania','Mauritius','Mayotte','Moldova','Republic of Moldova','Republic of Mayotte','Monaco','Montenegro','Morocco','Principality of Monaco','Mozambique','Namibia','Netherlands','New Caledonia','Niger','Nigeria',
                'Norway','Oman','Palestine','State of Palestine','Poland','Portugal','Qatar','Réunion','Romania','Russian Federation','Russia','Rwanda','São Tomé and Príncipe','Saint Barthélemy','Saint-Barthélemy','Saint Helena, Ascension and Tristan da Cunha','Saint Pierre and Miquelon','San Marino','Sao Tome & Principe','Saudi Arabia','Senegal','Serbia','Seychelles','Sierra Leone','Sint Maarten','Slovakia',
                'Slovenia','Somalia','South Africa','South Korea','South Sudan','Spain','Sudan','Swaziland','Sweden','Switzerland','Syria','Syrian Arab Republic','Tajikistan','Tanzania','United Republic of Tanzania','Togo','Tunisia',
                'Turkey','Turkmenistan','Uganda','Ukraine','United Arab Emirates','United Kingdom','United Kingdom of Great Britain and Northern Island','Uzbekistan','Vatican City','Wallis and Futuna','Western Sahara','Yemen','Zambia','Zimbabwe'
                ) then 'EMEA'
when country in (
                'American Samoa','Australia','Bangladesh','Bhutan','British Indian Ocean Territory','Brunei','Brunei Darussalam','Cambodia','China','Christmas Island','Cocos (Keeling) Islands','Cocos [Keeling] Islands','East Timor','Fiji',
                'Guam','Hong Kong','India','Indonesia','Japan','Kiribati','Republic of Korea','Democratic People\'s Republic of Korea','Korea','North Korea','People\'s Democratic Republic of Lao','Lao','Laos','Macao','Malaysia','Maldives',
                'Marshall Islands','Micronesia', 'Federated States of Micronesia','Mongolia','Myanmar','Myanmar [Burma]','Nauru','Nepal','New Zealand','Niue','Norfolk Island','Northern Mariana Islands','Pakistan','Palau','Papua New Guinea',
                'Philippines','Pitcairn','Samoa','Singapore','Solomon Islands','Sri Lanka','Taiwan','Thailand','Timor-Leste','Democratic Republic of Timor-Leste','Tokelau','Tonga','Tuvalu','United States Minor Outlying Islands','U.S. Minor Outlying Islands','Vanuatu','Vietnam') then 'APAC'

when country in ('Canada','United States of America','USA','US','United States') then 'NA'
when country in ('Anguilla','Antigua and Barbuda','Argentina','Aruba','Bahamas','Barbados','Belize','Bermuda',' Plurinational State of Bolivia','Bolivia','Brazil',
                'Cayman Islands','Chile','Colombia','Costa Rica','Cuba','Dominica','Dominican Republic','Ecuador','El Salvador','Falkland Islands (Malvinas)','Falkland Islands','Grenada',
                'Guatemala','Guyana','Haiti','Honduras','Jamaica','Mexico','Montserrat','Nicaragua','Panama','Paraguay','Peru','Puerto Rico','Saint Kitts and Nevis','St Kitts and Nevis','Saint Lucia',
                'Saint Vincent and the Grenadines','St Vincent and Grenadines','South Georgia and the South Sandwich Islands','Suriname','Trinidad and Tobago',
                'Turks and Caicos Islands','Uruguay',' Bolivarian Republic of Venezuela','Venezuela','British Virgin Islands','Virgin Islands, U.S.','U.S. Virgin Islands'
                ) then 'LATAM'

else 'other' 
end as gregion
FROM
        {{ source ( 'govideo_production' , 'acquisition_viewed_watermark_landing_page') }} as viewed_watermark_landing_page
WHERE
        viewed_watermark_landing_page.time < DATEADD(day, 1, current_date)
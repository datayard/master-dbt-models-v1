select
    id as chapterId
    , player_id as playerId
    , video_id as videoId
    , created_at as createdDate
from
    {{ source ('public','vidyard_chapters') }}
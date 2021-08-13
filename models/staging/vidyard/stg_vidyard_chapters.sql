select
    vidyard_chapters.id as chapterId
    , vidyard_chapters.player_id as playerId
    , vidyard_chapters.video_id as videoId
    , vidyard_chapters.created_at as createdDate
from
    {{ source ('public','vidyard_chapters') }} as vidyard_chapters
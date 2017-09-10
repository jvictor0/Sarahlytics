select video_id
from videos_facts
where channel_id='%(channel_id)s'
group by channel_id, video_id
order by max(view_count) desc
limit %(limit)s

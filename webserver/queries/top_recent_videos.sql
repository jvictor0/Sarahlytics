SELECT l.video_id
FROM
(
    SELECT channel_id, video_id, max(view_count) as vc, max(ts) as ts
    from videos_facts
    where channel_id = '%(channel_id)s'
    group by channel_id, video_id
) l
LEFT JOIN
(
    SELECT channel_id, video_id, max(view_count) as vc, max(ts) as ts
    FROM videos_facts
    WHERE channel_id = '%(channel_id)s' 
      AND ts < date_sub(convert_tz(now(), 'system', 'gmt'), INTERVAL %(hours)s hour)
    group by channel_id, video_id
) r
ON l.channel_id = r.channel_id AND r.video_id = l.video_id
ORDER BY if(r.vc is null, l.vc, (l.vc - r.vc) / timestampdiff(second, r.ts, l.ts)) desc
limit %(limit)s

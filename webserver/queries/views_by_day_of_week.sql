select
    hour(ts1) + 24 * dayofweek(ts1) as hour,
    hour(ts1),
    dayofweek(ts1),
    count(*) as freq,
    avg(view_count1 - view_count2) as views_avg_observed,
    sum(view_count1 - view_count2) as views_observed
from
(
    select
        convert_tz(ts, 'gmt', 'system') ts1,
        lag(convert_tz(ts, 'gmt', 'system'), 1) over (partition by channel_id, video_id order by ts) ts2,
        view_count view_count1,
        lag(view_count, 1) over (partition by channel_id, video_id order by ts) view_count2
    from videos_facts
    where channel_id="UCAbKLYEuTR1riockIgAWBiw"
) sub
where timestampdiff(minute, ts2, ts1) < 90 group by 1 order by 1;

import api
from database import db_utils

video_kill_paths = [
    ["kind"],
    ["snippet","publishedAt"],
    ["snippet","channelId"],
    ["snippet","localized"],
    ["snippet","thumbnails"],
    ["snippet","description"]]

channel_kill_paths = [
    ["kind"],
    ["snippet","thumbnails"],
    ["snippet","localized"],
    ["snippet","description"]]

def KillPath(kill_path, json):
    if kill_path[0] in json:
        if len(kill_path) == 1:
            del json[kill_path[0]]
        else:
            KillPath(kill_path[1:], json[kill_path[0]])
                
def NormalizeVideo(json, channelId, publishedAt):
    json["channelId"] = channelId
    json["publishedAt"] = publishedAt
    for kp in video_kill_paths:
        KillPath(kp, json)
    return json

def NormalizeChannel(json):
    for kp in channel_kill_paths:
        KillPath(kp, json)
    return json

def FetchVideos(previds, statistics=True, snippet=True):
    videos = api.Videos(vids=previds.keys(), statistics=statistics, snippet=snippet)
    result = []
    for v in videos:
        pv = previds[v['id']]
        result.append(NormalizeVideo(v, pv["channel_id"], pv["published_at"]))
    return result    
        
def FetchVideosForChannels(channel_ids, stop_before={}, max_quota=None, max_pages_per_channel=None, statistics=True, snippet=True):
    previds = {}
    quota = [0]
    channels = api.Channels(cids=channel_ids, quota=quota, statistics=False)
    uploads = [(pv["id"], pv["contentDetails"]["relatedPlaylists"]["uploads"]) for pv in channels]
    for cid, u in uploads:
        if max_quota is not None:
            if quota[0] + api.VideosCost(len(previds), statistics=statistics, snippet=snippet) > max_quota:
                break
        for pv in api.PlaylistItems(max_pages=max_pages_per_channel, quota=quota, playlist_id=u, stop_before=stop_before.get(cid, None)):
            if cid not in stop_before or stop_before[cid] < db_utils.DateTime(pv["snippet"]["publishedAt"]):
                previds[pv['snippet']['resourceId']['videoId']] = {"channel_id" : pv["snippet"]["channelId"],
                                                                   "published_at" : pv["snippet"]["publishedAt"]}
    return FetchVideos(previds, statistics, snippet)
    
def ObserveVideos(video_rows, statistics=True, snippet=True):
    previds = {}
    for vr in video_rows:
        previds[vr["video_id"]] = vr
    return FetchVideos(previds, statistics, snippet)
                     
def ObserveChannels(channels, statistics=True, snippet=True, content_details=True):
    result = api.Channels(cids=channels, statistics=statistics, snippet=snippet, content_details=content_details)
    return [NormalizeChannel(c) for c in result]

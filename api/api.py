import config
import apiclient.discovery
import apiclient.errors
from database import db_utils
import time
import traceback
import sys

# Set DEVELOPER_KEY to the API key value from the APIs & auth > Registered apps
# tab of
#   https://cloud.google.com/console
# Please ensure that you have enabled the YouTube Data API for your project.
YOUTUBE_API_SERVICE_NAME = "youtube"
YOUTUBE_API_VERSION = "v3"

g_youtube = None

def YouTube():
    global g_youtube
    if g_youtube is None:
        g_youtube = apiclient.discovery.build(YOUTUBE_API_SERVICE_NAME, YOUTUBE_API_VERSION, developerKey=config.DEVELOPER_KEY)
    return g_youtube

def ApiRequestRetry(fn, num_retries=10, sleep_secs=30, **kwargs):
    for i in xrange(num_retries):
        try:
            return fn(**kwargs).execute(num_retries=num_retries)
        except apiclient.errors.HttpError as e:
            traceback.print_exc(file=sys.stdout)
            if e.resp.status in [500, 503]:
                time.sleep(sleep_secs)
            else:
                raise
        except Exception as e:
            print "Exception class is", e.__class__.__name__
            raise
    raise e

def SearchInternal(quota=[0], q=None, snippet=True, max_results=50, channel_id=None, search_type="video", order="date", page_token=None, published_before=None, published_after=None):
    youtube = YouTube()

    quota[0] += 100
    
    part = ["id"]
    if snippet:
        part.append("snippet")
    part = ",".join(part)
    
    search_response = ApiRequestRetry(youtube.search().list,
                                      q=q,
                                      part=part,
                                      maxResults=max_results,
                                      type=search_type,
                                      order=order,
                                      channelId=channel_id,
                                      pageToken=page_token,
                                      publishedBefore=published_before,
                                      publishedAfter=published_after)

    return search_response

def PlaylistItemsInternal(quota=[0], snippet=True, max_results=50, playlist_id=None, page_token=None):
    youtube = YouTube()

    quota[0] += 1
    
    part = ["id"]
    if snippet:
        quota[0] += 2        
        part.append("snippet")
    part = ",".join(part)
    
    search_response = ApiRequestRetry(youtube.playlistItems().list,
                                      part=part,
                                      maxResults=max_results,
                                      playlistId=playlist_id,
                                      pageToken=page_token,
    )

    return search_response

def Search(max_pages=1, **kwargs):
    results = []
    page_token = None
    for i in xrange(max_pages):
        r = SearchInternal(page_token=page_token, **kwargs)
        results.extend(r.get("items", []))
        page_token = r.get("nextPageToken")
        if page_token is None:
            break
    return results

def PlaylistItems(max_pages=1, stop_before=None, **kwargs):
    results = []
    page_token = None
    while max_pages is None or max_pages > 0:
        if max_pages is not None:
            max_pages -= 1
        r = PlaylistItemsInternal(page_token=page_token, **kwargs)
        results.extend(r.get("items", []))
        page_token = r.get("nextPageToken")
        if page_token is None:
            break
        if stop_before is not None and db_utils.DateTime(results[-1]["snippet"]["publishedAt"]) < stop_before:
            break
    return results

def Channels(cids=[], quota=[0], statistics=True, snippet=True, content_details=True):
    if len(cids) == 0:
        return []
    
    youtube = YouTube()

    part = ["id"]
    if statistics:
        part.append("statistics")
    if snippet:
        part.append("snippet")
    if content_details:
        part.append("contentDetails")

    this_quota = 2 * len(part) - 1
    
    part = ",".join(part)
    
    result = []

    for i in xrange(0, len(cids), 50):
        quota[0] += this_quota
        the_cids = ",".join(cids[i:i+50])

        search_response = ApiRequestRetry(youtube.channels().list,
                                          part=part,
                                          id=the_cids)

        result.extend(search_response.get("items", []))

    return result

def VideosCost(len_vids, statistics=True, snippet=True):
    return (1 + 2 * sum([statistics, snippet])) * ((len_vids + 49) / 50)    

def VideosForCost(cost, statistics=True, snippet=True):
    return 50 * cost / (1 + 2 * sum([statistics, snippet]))

def Videos(vids=[], quota=[0], statistics=True, snippet=True):
    if len(vids) == 0:
        return []
    
    youtube = YouTube()
    
    part = ["id"]
    if statistics:
        part.append("statistics")
    if snippet:
        part.append("snippet")
    part = ",".join(part)

    this_quota = 2 * len(part) - 1
    
    result = []

    for i in xrange(0, len(vids), 50):
        quota[0] += this_quota        
        the_vids = ",".join(vids[i:i+50])

        search_response = ApiRequestRetry(youtube.videos().list,
                                          part=part,
                                          id=the_vids)

        result.extend(search_response.get("items", []))

    return result

import sparse_matrix
import lasso
from database import tables
from database import db_utils

def TagMatrix(tags):
    mat = sparse_matrix.SparseMatrixBuilder()
    response_dict = {}
    for t in tags:
        mat.Insert(t["video_id"], t["tag"], 1)
        if t["video_id"] not in response_dict:
            response_dict[t["video_id"]] = float(t["response"])
    return mat, mat.DenseRowVec(response_dict)

def ViewsOverSubsTags(earliest, latest, observed_at_secs=7*24*60*60, observed_at_bounds_secs=24*60*60):
    tabs = tables.Tables()
    videos = tabs.videos_facts.TemporalBand(earliest, latest, observed_at_secs, observed_at_bounds_secs)
    join = tabs.Joined(videos_facts=videos)
    query = """
    select video_id, tag, view_count / subscriber_count as response
    from 
    (%s) sub
    where view_count > 0 and subscriber_count > 0 and videos_facts_f and channels_facts_f
    """
    return db_utils.Dedent(query) % db_utils.Indent(join)
    
def ViewsOverSubsTagMatrix(con, earliest, latest, observed_at_secs=7*24*60*60, observed_at_bounds_secs=24*60*60):
    tags = con.query(ViewsOverSubsTags(earliest, latest, observed_at_secs, observed_at_bounds_secs))
    return TagMatrix(tags)

def ViewsOverSubsTagLasso(con, earliest, latest, observed_at_secs=7*24*60*60, observed_at_bounds_secs=24*60*60, alpha=1.0):
    mat_builder, vec = ViewsOverSubsTagMatrix(con, earliest, latest, observed_at_secs, observed_at_bounds_secs)
    return lasso.Lasso(mat_builder, vec, alpha=alpha)


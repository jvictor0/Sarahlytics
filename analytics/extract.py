import sparse_matrix
import lasso
from database import tables
from database import db_utils
from database import temporal_band
import argparse
import simplejson

def TagMatrix(tags):
    mat = sparse_matrix.SparseMatrixBuilder()
    response_dict = {}
    for t in tags:
        mat.Insert(t["video_id"], t["tag"], 1)
        if t["video_id"] not in response_dict:
            response_dict[t["video_id"]] = float(t["response"])
    return mat, mat.DenseRowVec(response_dict)

class ViewsOverSubsTags:
    def __init__(self, temp_band, min_subs=0):
        self.temp_band = temp_band
        self.min_subs = min_subs
        
    def Query(self):
        tabs = tables.Tables()
        join = tabs.Joined(videos_facts=self.temp_band.Query(),
                           channels_facts="select * from channels_facts where subscriber_count > %s and f" % self.min_subs)
        preds = ["view_count > 0"]
        query = """
        select video_id, tag, view_count / subscriber_count as response
        from 
        (%s) sub
        where %s
        """
        return db_utils.Dedent(query) % (db_utils.Indent(join), " and ".join(preds))
    
    def Matrix(self, con):
        print self.Query()
        print con.query("explain " + self.Query()).format_table()
        tags = con.query(self.Query())
        return TagMatrix(tags)

    def Lasso(self, con, remove_zeros=True, alpha=1.0):
        mat_builder, vec = self.Matrix(con)
        print len(mat_builder.entries)
        return lasso.Lasso(mat_builder, vec, alpha=alpha, remove_zeros=remove_zeros)


if __name__ == "__main__":
    con = db_utils.Connect()
    temp_band = temporal_band.TemporalBand(
        time_band=temporal_band.TimeBand(
            temporal_band.Time(time_ago=5*24*60*60),
            temporal_band.Time(time_ago=4*24*60*60)),
        observed_at_secs=3*24*60*60,
        predicates=["view_count > 2000", "f"])
    views_over_subs = ViewsOverSubsTags(temp_band=temp_band, min_subs=500)
    print simplejson.dumps(views_over_subs.Lasso(con), indent=4)

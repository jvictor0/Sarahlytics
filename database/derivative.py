import tables
import db_utils

class Derivative:
    def __init__(self, jsontab, inner_predicates=[], predicates=[]):
        self.jsontab = jsontab
        self.inner_predicates = inner_predicates
        self.predicates = predicates
        self.predicates.append("ts_old is not null")

    def Lag(self, name):
        assert self.jsontab.kucc[-1] == "ts"
        partition = ",".join(self.jsontab.kucc[:-1])
        return "lag(%s,1) over (partition by %s order by ts)" % (name, partition)
        
    def BaseQuery(self):
        result = "select\n    "
        cols = []

        for c in self.jsontab.kucc:
            cols.append(c)
        cols.append(self.Lag("ts") + " as ts_old")
        
        for c in self.jsontab.columns:
            if c.name not in self.jsontab.kucc:
                cols.append(c.name)
                if c.tp == "bigint":
                    cols.append(self.Lag(c.name) + " as %s_old" % c.name)

        result += ",\n    ".join(cols) + "\n"
        result += "from " + self.jsontab.name

        if len(self.inner_predicates) > 0:
            result += "\nwhere " + "\n  and ".join(self.inner_predicates)

        return result

    def Dts(self):
        return "timestampdiff(second, ts_old, ts)"
    
    def Query(self):
        result = "select\n    "
        cols = []
        for c in self.jsontab.kucc:
            cols.append(c)
        cols.append("ts_old")
        cols.append(self.Dts() + " as d_ts")
        
        for c in self.jsontab.columns:
            if c.name not in self.jsontab.kucc:
                cols.append(c.name)
                if c.tp == "bigint":
                    cols.append("%s_old" % c.name)
                    cols.append("(%s - %s_old) as d_%s" % (c.name, c.name, c.name))
                    cols.append("(%s - %s_old)/%s as d_%s_d_ts" % (c.name, c.name, self.Dts(), c.name))

        result += ",\n    ".join(cols) + "\n"

        result += "from (%s) sub" % db_utils.Indent(self.BaseQuery())
        if len(self.predicates) > 0:
            result += "\nwhere " + "\n  and ".join(self.predicates)

        return result

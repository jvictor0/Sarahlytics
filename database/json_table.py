import db_utils
import simplejson

def ExtractPath(json, path, remove=True):
    if len(path) == 0:
        return None
    while len(path) > 1:
        if path[0] in json:
            json = json[path[0]]
            path = path[1:]
        else:
            return None
    if path[0] in json:
        result  = json[path[0]]
        if remove:
            del json[path[0]]
        return result
    return None

class JSONColumn:
    def __init__(self, name, tp, nullable, path):
        self.name = name
        self.tp = tp
        self.nullable = nullable
        self.path = path

    def ToSQL(self):
        result = self.name + " " + self.tp
        if not self.nullable:
            result += " not null"
        return result

    def Extract(self, json, remove=True):
        result = ExtractPath(json, self.path, remove=remove)
        if not self.nullable:
            assert result is not None, (json, self.path)
        return result

def NormalizeJSONObject(json):
    result  = {}
    for k, v in json.iteritems():
        if isinstance(v, dict):
            v2 = NormalizeJSONObject(v)            
            if len(v2) > 0:
                result[k] = v2
        elif isinstance(v, list):
            v2 = NormalizeJSONList(v)
            if len(v2) > 0:
                result[k] = v2
        else:
            result[k] = v
    return result

def NormalizeJSONList(json):
    result = []
    for v in json:
        if isinstance(v, dict):
            v2 = NormalizeJSONObject(v)            
            if len(v2) > 0:
                result.append(v2)
        elif isinstance(v, list):
            v2 = NormalizeJSONList(v)
            if len(v2) > 0:
                result.append(v2)
        else:
            result.append(v)
    return result
    
class NormalizedArrayTable:
    def __init__(self, name, tp, nullable, parent, path, kucc):
        self.name = name
        self.tp = tp
        self.nullable = nullable
        self.parent = parent
        self.path = path
        self.parent.normalized_array_tables.append(self)
        self.kucc = kucc

    def ToSQL(self):
        result = "create table %s(\n" % (self.Name())
        for k in self.parent.kucc:
            if k == "ts":
                result += "    ts datetime not null,\n"
            else:
                result += "    " + self.parent.GetColumn(k).ToSQL() + ",\n"
        result += "    " + self.name + " " + self.tp
        if not self.nullable:
            result += " not null"
        result += ",\n"
        result += "    key(%s) using clustered columnstore," % ",".join(self.kucc) + "\n"
        result += "    shard(%s))" % ",".join(self.parent.shard)
        return result

    def Create(self, con):
        con.query(self.ToSQL())
        
    def Name(self):
        return self.parent.name + "_" + self.name + "s"
        
    def Values(self, json, now, values, remove=True):
        arr = ExtractPath(json, self.path, remove=remove)
        if arr is None:
            arr = []
        for a in arr:
            values.append([])
            for k in self.parent.kucc:
                if k == "ts":
                    values[-1].append(now)
                else:
                    values[-1].append(self.parent.GetColumn(k).Extract(json, remove=False))
            values[-1].append(a)

    def ColumnNames(self):
        return self.parent.kucc + [self.name]
        
    def Insert(self, con, jsons, now=None):
        if len(jsons) == 0:
            return
        if now is None:
            now = db_utils.Now(con)
        values = []
        for j in jsons:
            self.Values(j, now, values)
        if len(values) == 0:
            return
        q = db_utils.InsertQuery(self.Name(), self.ColumnNames(), values)
        con.query(q)

    def MostRecent(self):
        assert self.parent.kucc[-1] == "ts"
        window = "rank() over (partition by %s order by ts desc)" % ",".join(self.parent.kucc[:-1] + [self.name])
        return "select * from (select *, %s r from %s) sub where r = 1" % (window, self.Name())        

class JSONTable(object):
    def __init__(self, name, columns, kucc, shard):
        self.name = name
        self.columns = columns
        self.kucc = kucc
        self.shard = shard
        self.normalized_array_tables = []

    def GetColumn(self, name):
        for k in self.columns:
            if k.name == name:
                return k
        assert False, name
        
    def ToSQL(self):
        result = "create table %s(\n" % (self.name)
        result += "    ts datetime not null,\n"
        result += "    json json not null,\n"
        for jc in self.columns:
            result += "    " + jc.ToSQL() + ",\n"
        result += "    key(%s) using clustered columnstore," % ",".join(self.kucc) + "\n"
        result += "    shard(%s))" % ",".join(self.shard)
        return result

    def ColumnNames(self):
        return ["ts","json"] + [k.name for k in self.columns]
    
    def Values(self, json, now, values, remove=True):
        values.append([now, None])
        for k in self.columns:
            values[-1].append(k.Extract(json, remove=remove))
        values[-1][1] = simplejson.dumps(NormalizeJSONObject(json))

    def Insert(self, con, jsons, now=None):
        if len(jsons) == 0:
            return
        if now is None:
            now = db_utils.Now(con)
        for nat in self.normalized_array_tables:
            nat.Insert(con, jsons, now)
        values = []
        for j in jsons:
            self.Values(j, now, values)
        q = db_utils.InsertQuery(self.name, self.ColumnNames(), values)            
        con.query(q)

    def Create(self, con):
        con.query(self.ToSQL())
        for nat in self.normalized_array_tables:
            nat.Create(con)

    def MostRecent(self):
        assert self.kucc[-1] == "ts"
        window = "rank() over (partition by %s order by ts desc)" % ",".join(self.kucc[:-1])
        return "select * from (select *, %s r from %s) sub where r = 1" % (window, self.name)

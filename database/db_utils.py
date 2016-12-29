import database
import config
import simplejson
import datetime

def Connect(use_db=True):
    con = database.Connection(config.db_addr, "root", "")
    if use_db:
        con.query("use sarahlytics")
    return con

def Now(con):
    return con.query("select now() n")[0]["n"]

def Value(v):
    if v is None:
        return "null"
    return simplejson.dumps(v)        

def InsertQuery(table_name, columns, values):
    q = "insert into %s(%s) values " % (table_name, ",".join(columns))
    values_strings = []
    for v in values:
        values_strings.append("(%s)" % ",".join([Value(f) for f in v]))
    q += ",".join(values_strings)
    return q

def DateTime(ts):
    try:
        return datetime.datetime.strptime(ts, "%Y-%m-%d %H:%M:%S")
    except ValueError:
        return datetime.datetime.strptime(ts, "%Y-%m-%dT%H:%M:%S.000Z")

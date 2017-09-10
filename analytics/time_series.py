

class TimePoint:
    def __init__(self, time, vals=None):
        self.time = time
        if vals is None:
            vals = {}
        self.vals = vals

    def __getitem__(self, k):
        if k in self.vals:            
            return self.vals[k]
        else:
            return None

    def __setitem__(self, k, v):
        self.vals[k] = v

    def Keys(self):
        return self.vals.keys()

    def Get(self, keys):
        return (self.time, [self[k] for k in keys])
    
class Interpolator:
    def __init__(self, group_by, response, time_col='t'):
        self.group_by = group_by
        self.response = response
        self.time_col = time_col
        self.group_index = {}
        self.group_cursors = {}
        self.time_points = []
        self.rows = None

    def Groups(self):
        return self.group_index.keys()

    def GroupMatrix(self):
        groups = self.Groups()
        return groups, [tp.Get(groups) for tp in self.time_points]
         
    def RowTime(self, i):
        return int(self.rows[i][self.time_col])

    def RowGroup(self, i):
        return self.rows[i][self.group_by]

    def RowResponse(self, i):
        return float(self.rows[i][self.response])

    def CreateGroupIndex(self):
        for i in xrange(len(self.rows)):
            if self.RowGroup(i) not in self.group_index:
                self.group_index[self.RowGroup(i)] = []
                self.group_cursors[self.RowGroup(i)] = 0
            self.group_index[self.RowGroup(i)].append(i)

    def InterpolateGroup(self, i, k, t):
        if self.group_cursors[k] == len(self.group_index[k]):
            return self.RowResponse(self.group_index[k][self.group_cursors[k] - 1])
        else:
            assert self.group_cursors[k] < len(self.group_index[k]), (self.group_cursors[k], len(self.group_index[k]))
            cursor0 = self.group_index[k][self.group_cursors[k] - 1]
            cursor1 = self.group_index[k][self.group_cursors[k]]
            assert self.RowGroup(cursor0) == k
            assert self.RowGroup(cursor1) == k
            assert cursor0 < i < cursor1, (cursor0, i, cursor1)
            t0 = self.RowTime(cursor0)
            t1 = self.RowTime(cursor1)
            assert t1 > t > t0, (t1, t, t0, t1 - t0, t - t0)
            y0 = self.RowResponse(cursor0)
            y1 = self.RowResponse(cursor1)
            time_frac = float(t - t0) / float(t1 - t0)
            res = y0 + (y1 - y0) * time_frac
            assert 0 < time_frac < 1, (t0, t, t1)
            return res
        
    def Interpolate(self, rows):
        assert self.rows is None
        self.rows = rows
        self.CreateGroupIndex()
        i = 0
        while i < len(self.rows):
            t = self.RowTime(i)
            tp = TimePoint(t)
            while i < len(self.rows) and self.RowTime(i) == t:
                tp[self.RowGroup(i)] = self.RowResponse(i)
                self.group_cursors[self.RowGroup(i)] += 1
                i += 1
            for k in self.group_index.keys():
                if k not in tp.Keys() and self.group_cursors[k] > 0:
                    tp[k] = self.InterpolateGroup(i - 1, k, t)
            self.time_points.append(tp)
        return self.time_points

    def Differentiate(self, min_time=1000*1000*60*60*3, normalizer=1000*1000*60*60):
        last = {}
        result = []
        for i in xrange(len(self.rows)):
            g = self.RowGroup(i)
            if g not in last:
                last[g] = []
            last[g].append((self.RowTime(i), self.RowResponse(i)))
            dt = float(last[g][-1][0] - last[g][0][0])
            if dt > min_time:
                dr = normalizer * float(last[g][-1][1] - last[g][0][1])
                result.append({"d%s_d%s" % (self.response, self.time_col) : dr / dt,
                               self.time_col : last[g][0][0] + dt / 2,
                               self.group_by : g})
                last[g] = [last[g][-1]]
        result.sort(key=lambda r:r[self.time_col])
        return result

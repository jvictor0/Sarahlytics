from scipy import sparse
import numpy

class Enumerizer:
    def __init__(self):
        self.keys = {}
        self.entries = []

    def GetEnum(self, key, no_insert=False):
        if key not in self.keys:
            assert not no_insert, key
            self.keys[key] = len(self.keys)
            self.entries.append(key)
        return self.keys[key]

    def GetKey(self, ix):
        return self.entries[ix]

    def __len__(self):
        return len(self.keys)

    def Dense(self, dct):        
        result = numpy.array([0 for _ in xrange(len(self))])
        for k,v in dct.iteritems():
            result[self.GetEnum(k, no_insert=True)] = v
        return result
    
class SparseMatrixBuilder:
    def __init__(self):
        self.entries = []
        self.row_enum = Enumerizer()
        self.col_enum = Enumerizer()

    def Insert(self, row, col, val):
        self.entries.append((self.row_enum.GetEnum(row), self.col_enum.GetEnum(col), val))

    def LIL(self):
        result = sparse.lil_matrix((len(self.row_enum), len(self.col_enum)))
        for i,j,v in self.entries:
            result[i,j] = v
        return result

    def GetRowKey(self, ix):
        return self.row_enum.GetKey(ix)

    def GetColKey(self, ix):
        return self.col_enum.GetKey(ix)
    
    def CSR(self):
        return self.LIL().tocsr()

    def CSC(self):
        return self.LIL().tocsc()

    def DenseRowVec(self, dct):
        return self.row_enum.Dense(dct)

    def DenseColVec(self, dct):
        return self.col_enum.Dense(dct)

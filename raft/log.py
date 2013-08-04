class RaftLog(object):
    def __init__(self, log):
        self.ldict = log

    def dump(self):
        return self.ldict

    def get_max_index_term(self):
        maxindex = max(self.ldict)
        maxterm = self.ldict[maxindex][0]
        return maxindex, maxterm

    def get_commit_index(self):
        latest = 0
        for k in sorted(self.ldict):
            if self.ldict[k][1] == False:
                return latest
            latest = k

    def __le__(self, other):
        mi, mt = self.get_max_index_term()
        oi, ot = other.get_max_index_term()
        if ot > mt:
            return True
        if ot == mt and oi >= mi:
            return True
        return False

    def __gt__(self, other):
        return not self <= other

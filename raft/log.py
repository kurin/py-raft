class RaftLog(object):
    def __init__(self, log):
        if not log:
            log = {0: (0, True, '')}
        self.ldict = log

    def dump(self):
        return self.ldict

    def get_max_index_term(self):
        maxindex = self.maxindex()
        maxterm = self.ldict[maxindex][0]
        return maxindex, maxterm

    def maxindex(self):
        return max(self.ldict)

    def get(self, idx):
        return self.ldict.get(idx, None)

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

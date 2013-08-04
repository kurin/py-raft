class RaftLog(object):
    def __init__(self, log):
        if not log:
            log = {0: (0, True, {})}
        self.ldict = log
        self.idmap = {}
        for idx, val in self.ldict.iteritems():
            data = val[2]
            if 'id' in data:
                msgid = data['id']
                self.idmap[msgid] = data

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

    def add(self, term, msgid, data):
        if msgid in self.idmap:
            return
        mi = self.maxindex()
        self.ldict[mi + 1] = (term, False, data)
        self.idmap[msgid] = data

    def commit(self, index, term):
        ent = self.ldict[index]
        assert ent[0] == term
        self.ldict[index] = (term, True, ent[2])

    def logs_after_index(self, index):
        last = self.maxindex()
        logs = {}
        for x in range(index, last):
            logs[x+1] = self.ldict[x+1]
        return logs

    def get_commit_index(self):
        latest = 0
        for k in sorted(self.ldict):
            if self.ldict[k][1] == False:
                return latest
            latest = k

    def exists(self, index, term):
        if self.ldict.get(index, (None,))[0] == term:
            return True
        return False

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

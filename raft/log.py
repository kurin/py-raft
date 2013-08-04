class RaftLog(object):
    def __init__(self, log):
        if not log:
            logentry = {
                'index': 0,
                'term': 0,
                'msgid': '',
                'committed': True,  # everyone has the null message
                'msg': {}
            }
            log = {0: logentry}
        self.log_by_index = log
        self.log_by_msgid = {}
        for ent in self.log_by_index.values():
            msgid = ent['msgid']
            self.log_by_msgid[msgid] = ent

    def dump(self):
        return self.log_by_index

    def get_max_index_term(self):
        maxindex = self.maxindex()
        maxterm = self.log_by_index.get(maxindex, {}).get('term', None)
        return maxindex, maxterm

    def maxindex(self):
        return max(self.log_by_index)

    def get(self, idx):
        return self.log_by_index.get(idx, None)

    def get_term_of(self, idx):
        le = self.get(idx)
        return le['term']

    def remove(self, idx):
        ent = self.log_by_index[idx]
        del self.log_by_index[idx]
        msgid = ent['msgid']
        del self.log_by_msgid[msgid]

    def add(self, logentry):
        if not 'index' in logentry:
            # this is being appended to a leader's log; reject if msgid is known
            # and allocate a new index for it
            if logentry['msgid'] in self.log_by_msgid:
                return
            index = self.maxindex() + 1
            logentry['index'] = index
        else:
            # this is a follower being told to put logentry in a specific spot
            index = logentry['index']
            mi = self.maxindex()
            if mi + 1 != index:
                # remove everything in the log after and including the current
                # index
                remove = [x for x in self.log_by_index if x >= index]
                for rem in remove:
                    self.remove(rem)
        self.log_by_index[index] = logentry
        msgid = logentry['msgid']
        self.log_by_msgid[msgid] = logentry

    def commit(self, index, term):
        ent = self.log_by_index[index]
        assert ent['term'] == term
        ent['committed'] = True

    def logs_after_index(self, index):
        last = self.maxindex()
        logs = {}
        for x in range(index, last):
            logs[x+1] = self.log_by_index[x+1]
        return logs

    def get_commit_index(self):
        latest = 0
        for k in sorted(self.log_by_index):
            if self.log_by_index[k]['committed'] == False:
                return latest
            latest = k

    def exists(self, index, term):
        if self.log_by_index.get(index, {}).get('term', None) == term:
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

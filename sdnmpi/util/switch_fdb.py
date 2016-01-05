class SwitchFDB(object):
    def __init__(self):
        super(SwitchFDB, self).__init__()
        self._dpid_to_fdb = {}

    def update(self, dpid, src, dst, out_port):
        if dpid not in self._dpid_to_fdb:
            self._dpid_to_fdb[dpid] = {}
        self._dpid_to_fdb[dpid][(src, dst)] = out_port

    def exists(self, dpid, src, dst):
        if dpid in self._dpid_to_fdb:
            if (src, dst) in self._dpid_to_fdb[dpid]:
                return True
        return False

    def to_dict(self):
        switches = []
        for dpid, fdb in self._dpid_to_fdb.items():
            switch_fdb = []
            for (src, dst), out_port in fdb.items():
                switch_fdb.append({
                    "src": src,
                    "dst": dst,
                    "out_port": out_port,
                })
            switches.append({
                "dpid": dpid,
                "fdb": switch_fdb,
            })

        return switches

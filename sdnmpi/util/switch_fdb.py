class SwitchFDB(object):
    def __init__(self):
        super(SwitchFDB, self).__init__()
        self._mac_to_port = {}

    def update(self, dpid, mac, port):
        if dpid not in self._mac_to_port:
            self._mac_to_port[dpid] = {}
        self._mac_to_port[dpid][mac] = port

    def to_dict(self):
        return self._mac_to_port

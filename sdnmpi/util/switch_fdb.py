from .signal import Signal


class SwitchFDB(object):
    def __init__(self):
        super(SwitchFDB, self).__init__()
        self._mac_to_port = {}
        self.changed = Signal()

    def update(self, dpid, mac, port):
        if dpid not in self._mac_to_port:
            self._mac_to_port[dpid] = {}
        self._mac_to_port[dpid][mac] = port
        self.changed.fire(dpid, mac, port)

    def get_port(self, dpid, mac):
        if dpid not in self._mac_to_port:
            return None
        return self._mac_to_port[dpid].get(mac)

    def to_dict(self):
        return self._mac_to_port

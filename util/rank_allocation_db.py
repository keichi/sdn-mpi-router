from .signal import Signal


class RankAllocationDB(object):
    def __init__(self):
        super(RankAllocationDB, self).__init__()
        self._rank_to_mac = {}
        self.changed = Signal()

    def update(self, rank, mac):
        self._rank_to_mac[rank] = mac
        self.changed.fire(rank, mac)

    def get_mac(self, rank):
        return self._rank_to_mac.get(rank)

    def to_dict(self):
        return self._rank_to_mac

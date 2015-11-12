from .signal import Signal


class RankAllocationDB(object):
    def __init__(self):
        super(RankAllocationDB, self).__init__()
        self._rank_to_mac = {}
        self.process_added = Signal()
        self.process_deleted = Signal()

    def add_process(self, rank, mac):
        self._rank_to_mac[rank] = mac
        self.process_added.fire(rank, mac)

    def delete_prcess(self, rank):
        if rank in self._rank_to_mac:
            del self._rank_to_mac[rank]
        self.process_deleted.fire(rank)

    def get_mac(self, rank):
        return self._rank_to_mac.get(rank)

    def to_dict(self):
        return self._rank_to_mac

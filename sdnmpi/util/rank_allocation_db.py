class RankAllocationDB(object):
    def __init__(self):
        super(RankAllocationDB, self).__init__()
        self._rank_to_mac = {}

    def add_process(self, rank, mac):
        self._rank_to_mac[rank] = mac

    def delete_prcess(self, rank):
        if rank in self._rank_to_mac:
            del self._rank_to_mac[rank]

    def get_mac(self, rank):
        return self._rank_to_mac.get(rank)

    def to_dict(self):
        return self._rank_to_mac

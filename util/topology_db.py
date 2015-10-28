from .signal import Signal


class TopologyDB(object):
    def __init__(self):
        super(TopologyDB, self).__init__()
        self.switches = {}
        self.links = {}
        self.hosts = {}
        self.switch_added = Signal()
        self.switch_deleted = Signal()
        self.link_added = Signal()
        self.link_deleted = Signal()
        self.host_added = Signal()

    def add_host(self, host):
        self.hosts[host.mac] = host
        self.host_added.fire(host)

    def add_switch(self, switch):
        self.switches[switch.dp.id] = switch
        self.switch_added.fire(switch)

    def delete_switch(self, switch):
        del self.switches[switch.dp.id]
        self.switch_deleted(switch)

    def add_link(self, link):
        src_dpid = link.src.dpid
        dst_dpid = link.dst.dpid
        if src_dpid not in self.links:
            self.links[src_dpid] = {}
        self.links[src_dpid][dst_dpid] = link
        self.link_added(link)

    def delete_link(self, link):
        src_dpid = link.src.dpid
        dst_dpid = link.dst.dpid
        del self.links[src_dpid][dst_dpid]
        self.link_deleted(link)

    def to_dict(self):
        switches = [switch.to_dict() for switch in self.switches]
        links = []
        for dst_to_link in self.links.values():
            links.extend(dst_to_link.values())

        return {
            "switches": switches,
            "links": links,
        }

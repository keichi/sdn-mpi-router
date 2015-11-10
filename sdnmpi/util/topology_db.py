from .signal import Signal


class TopologyDB(object):
    def __init__(self):
        super(TopologyDB, self).__init__()
        # Switch DPID -> ryu.topology.switches.Switch
        # switches[dpid].dp is a Datapath
        # switches[dpid].ports[idx].port_no is a Port num
        self.switches = {}
        # Src switch DPID -> dst switch DPID -> ryu.topology.switches.Link
        self.links = {}
        # MAC address -> ryu.topology.switches.Host
        self.hosts = {}
        # Switch DPID -> port number
        self.broadcast_ports = {}

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
        self.update_spanning_tree()
        self.switch_added.fire(switch)

    def delete_switch(self, switch):
        del self.switches[switch.dp.id]
        self.update_spanning_tree()
        self.switch_deleted.fire(switch)

    def add_link(self, link):
        src_dpid = link.src.dpid
        dst_dpid = link.dst.dpid
        if src_dpid not in self.links:
            self.links[src_dpid] = {}
        self.links[src_dpid][dst_dpid] = link
        self.update_spanning_tree()
        self.link_added.fire(link)

    def delete_link(self, link):
        src_dpid = link.src.dpid
        dst_dpid = link.dst.dpid
        del self.links[src_dpid][dst_dpid]
        self.update_spanning_tree()
        self.link_deleted.fire(link)

    def to_dict(self):
        switches = [switch.to_dict() for switch in self.switches.values()]
        hosts = [host.to_dict() for host in self.hosts.values()]
        links = []
        for dst_to_link in self.links.values():
            for link in dst_to_link.values():
                links.append(link.to_dict())

        return {
            "switches": switches,
            "links": links,
            "hosts": hosts,
        }

    def _calculate_spanning_tree(self, src, broadcast_ports, visited):
        # if switch has no outgoing links
        if src not in self.links:
            return
        # looop through outgoing links
        for dst, link in self.links[src].items():
            if dst not in visited:
                visited.add(dst)
                broadcast_ports[src] = link.src.port_no
                self._calculate_spanning_tree(dst, broadcast_ports, visited)

    def update_spanning_tree(self):
        """Update spanning tree of topology using depth-first search"""
        broadcast_ports = {}
        visited_dpids = set()

        if self.switches:
            root = self.switches.keys()[0]
            self._calculate_spanning_tree(root, broadcast_ports, visited_dpids)

        self.broadcast_ports = broadcast_ports

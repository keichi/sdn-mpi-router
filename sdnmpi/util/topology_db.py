# TODO Should not depend on a specific version of ofproto
import ryu.ofproto.ofproto_v1_0 as ofproto


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

    def add_host(self, host):
        self.hosts[host.mac] = host

    def add_switch(self, switch):
        self.switches[switch.dp.id] = switch

    def delete_switch(self, switch):
        if switch.dp.id in self.switches:
            del self.switches[switch.dp.id]

    def add_link(self, link):
        src_dpid = link.src.dpid
        dst_dpid = link.dst.dpid
        if src_dpid not in self.links:
            self.links[src_dpid] = {}
        self.links[src_dpid][dst_dpid] = link

    def delete_link(self, link):
        src_dpid = link.src.dpid
        dst_dpid = link.dst.dpid
        if src_dpid in self.links:
            if dst_dpid in self.links[src_dpid]:
                del self.links[src_dpid][dst_dpid]

    def to_dict(self):
        """Convert this object to a JSON-serializable object"""
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

    def _find_route(self, src_dpid, dst_dpid):
        """Find a route between two switches using DFS
        Returns a list of switches included in the route"""
        # visited switches
        visited = set([src_dpid])
        # intermediate paths
        paths = [[src_dpid]]
        while paths:
            current_path = paths.pop()
            dpid = current_path[-1]
            # we have reached the goal
            if dpid == dst_dpid:
                return current_path
            # check if switch has outgoing links
            if dpid not in self.links:
                continue
            # loop through outgoing links
            for next_dpid, link in self.links[dpid].items():
                # if the link is connected to an unvisited switch
                if next_dpid not in visited:
                    next_path = list(current_path)
                    next_path.append(next_dpid)
                    visited.add(next_dpid)
                    paths.append(next_path)
        # destination is unreachable
        return []

    def _mac_to_int(self, mac):
        return int(mac.replace(":", ""), 16)

    def find_route(self, src_mac, dst_mac):
        """Find a route between two hosts using depth-first search
        Returns a list of tuples (datapath id, output port)"""
        # Check if src/dst is a switch local port
        is_local_src = False
        is_local_dst = False
        if self._mac_to_int(src_mac) in self.switches:
            is_local_src = True
        if self._mac_to_int(dst_mac) in self.switches:
            is_local_dst = True

        # Check if src/dst host exist
        if not is_local_src and src_mac not in self.hosts:
            return []
        elif not is_local_dst and dst_mac not in self.hosts:
            return []

        # Get src/dst edge switches
        if is_local_src:
            src_dpid = self._mac_to_int(src_mac)
        else:
            src_dpid = self.hosts[src_mac].port.dpid

        if is_local_dst:
            dst_dpid = self._mac_to_int(dst_mac)
        else:
            dst_dpid = self.hosts[dst_mac].port.dpid

        # Perform depth-first search to find a route from src to dst
        route = self._find_route(src_dpid, dst_dpid)
        if not route:
            return []

        fdb = []
        for idx, dpid in enumerate(route[:-1]):
            fdb.append((dpid, self.links[dpid][route[idx+1]].src.port_no))

        # Dst switch to dst host
        if is_local_dst:
            fdb.append((dst_dpid, ofproto.OFPP_LOCAL))
        else:
            fdb.append((dst_dpid, self.hosts[dst_mac].port.port_no))

        return fdb

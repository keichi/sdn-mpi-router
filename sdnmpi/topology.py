from ryu.base import app_manager
from ryu.controller.handler import MAIN_DISPATCHER, set_ev_cls
from ryu.controller.event import EventRequestBase, EventReplyBase
from ryu.topology import event, switches
from ryu.controller import ofp_event
from ryu.lib.mac import haddr_to_bin, BROADCAST_STR, BROADCAST
from ryu.lib.packet import packet, ethernet, udp

from util.topology_db import TopologyDB


class CurrentTopologyRequest(EventRequestBase):
    def __init__(self):
        super(CurrentTopologyRequest, self).__init__()
        self.dst = "TopologyManager"


class CurrentTopologyReply(EventReplyBase):
    def __init__(self, dst, topology):
        super(CurrentTopologyReply, self).__init__(dst)
        self.topology = topology


class FindRouteRequest(EventRequestBase):
    def __init__(self, src_mac, dst_mac):
        super(FindRouteRequest, self).__init__()
        self.dst = "TopologyManager"
        self.src_mac = src_mac
        self.dst_mac = dst_mac


class FindRouteReply(EventReplyBase):
    def __init__(self, dst, fdb):
        super(FindRouteReply, self).__init__(dst)
        self.fdb = fdb


class BroadcastRequest(EventRequestBase):
    def __init__(self, data, src_dpid, src_in_port):
        super(BroadcastRequest, self).__init__()
        self.dst = "TopologyManager"
        self.data = data
        self.src_dpid = src_dpid
        self.src_in_port = src_in_port


class TopologyManager(app_manager.RyuApp):
    _CONTEXTS = {
        "switches": switches.Switches,
    }
    _EVENTS = [CurrentTopologyRequest, BroadcastRequest]

    def __init__(self, *args, **kwargs):
        super(TopologyManager, self).__init__(*args, **kwargs)
        self.topologydb = TopologyDB()

    def _add_flow(self, datapath, in_port, dst, actions):
        ofproto = datapath.ofproto

        match = datapath.ofproto_parser.OFPMatch(
            in_port=in_port, dl_dst=haddr_to_bin(dst))

        mod = datapath.ofproto_parser.OFPFlowMod(
            datapath=datapath, match=match, cookie=0,
            command=ofproto.OFPFC_ADD, idle_timeout=0, hard_timeout=0,
            priority=ofproto.OFP_DEFAULT_PRIORITY,
            flags=ofproto.OFPFF_SEND_FLOW_REM, actions=actions)
        datapath.send_msg(mod)

    def _install_multicast_drop(self, datapath, dst):
        ofproto = datapath.ofproto

        match = datapath.ofproto_parser.OFPMatch(dl_dst=haddr_to_bin(dst))

        # Install a flow to drop all packets sent to dst
        mod = datapath.ofproto_parser.OFPFlowMod(
            datapath=datapath, match=match, cookie=0,
            command=ofproto.OFPFC_ADD, idle_timeout=0, hard_timeout=0,
            priority=0xffff, actions=[])
        datapath.send_msg(mod)

    @set_ev_cls(ofp_event.EventOFPStateChange, MAIN_DISPATCHER)
    def _state_change_handler(self, ev):
        datapath = ev.datapath
        ofproto = datapath.ofproto
        ofproto_parser = datapath.ofproto_parser

        match = ofproto_parser.OFPMatch(dl_dst=BROADCAST)
        actions = [ofproto_parser.OFPActionOutput(ofproto.OFPP_CONTROLLER)]

        # Install a flow to send all broadcast packets to the controller
        mod = datapath.ofproto_parser.OFPFlowMod(
            datapath=datapath, match=match, cookie=0,
            command=ofproto.OFPFC_ADD, idle_timeout=0, hard_timeout=0,
            priority=0xfffe, actions=actions)
        datapath.send_msg(mod)

    @set_ev_cls(ofp_event.EventOFPPacketIn, MAIN_DISPATCHER)
    def _packet_in_handler(self, ev):
        msg = ev.msg
        datapath = msg.datapath
        pkt = packet.Packet(msg.data)
        eth = pkt.get_protocol(ethernet.ethernet)
        dst = eth.dst

        # Do not handle IPv6 multicast packets
        if dst.startswith("33:33"):
            self._install_multicast_drop(datapath, dst)
            return
        # Do not handler unicast packets
        elif dst != BROADCAST_STR:
            return

        # Do not handle announcement packets
        udph = pkt.get_protocol(udp.udp)
        if udph and udph.dst_port == 61000:
            return

        self._do_broadcast(msg.data)

    @set_ev_cls(CurrentTopologyRequest)
    def _current_topology_request_handler(self, req):
        reply = CurrentTopologyReply(req.src, self.topologydb)
        self.reply_to_request(req, reply)

    @set_ev_cls(FindRouteRequest)
    def _find_route_request_handler(self, req):
        fdb = self.topologydb.find_route(req.src_mac, req.dst_mac)
        reply = FindRouteReply(req.src, fdb)
        self.reply_to_request(req, reply)

    def _is_edge_port(self, port):
        for dpid_to_link in self.topologydb.links.values():
            for link in dpid_to_link.values():
                if port == link.src or port == link.dst:
                    return False
        return True

    def _do_broadcast(self, data):
        for switch in self.topologydb.switches.values():
            datapath = switch.dp
            ofproto = datapath.ofproto
            ofproto_parser = datapath.ofproto_parser

            actions = [ofproto_parser.OFPActionOutput(port.port_no)
                       for port in switch.ports if self._is_edge_port(port)]
            actions.append(ofproto_parser.OFPActionOutput(ofproto.OFPP_LOCAL))

            out = ofproto_parser.OFPPacketOut(
                datapath=datapath, in_port=ofproto.OFPP_NONE,
                buffer_id=ofproto.OFP_NO_BUFFER, actions=actions,
                data=data)
            datapath.send_msg(out)

    @set_ev_cls(BroadcastRequest)
    def _broadcast_request_handler(self, req):
        self._do_broadcast(req.data)
        self.reply_to_request(req, EventReplyBase(req.src))

    @set_ev_cls(event.EventSwitchEnter)
    def _event_switch_enter_handler(self, ev):
        self.topologydb.add_switch(ev.switch)

    @set_ev_cls(event.EventSwitchLeave)
    def _event_switch_leave_handler(self, ev):
        self.topologydb.delete_switch(ev.switch)

    @set_ev_cls(event.EventLinkAdd)
    def _event_link_add_handler(self, ev):
        self.topologydb.add_link(ev.link)

    @set_ev_cls(event.EventLinkDelete)
    def _event_link_delete_handler(self, ev):
        self.topologydb.delete_link(ev.link)

    @set_ev_cls(event.EventHostAdd)
    def _event_host_add_handler(self, ev):
        self.topologydb.add_host(ev.host)

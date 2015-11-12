from ryu.base import app_manager
from ryu.controller.handler import MAIN_DISPATCHER, set_ev_cls
from ryu.controller.event import EventRequestBase, EventReplyBase
from ryu.topology import event, switches
from ryu.controller import ofp_event
from ryu.lib.mac import haddr_to_bin, BROADCAST_STR
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


class TopologyManager(app_manager.RyuApp):
    _CONTEXTS = {
        "switches": switches.Switches,
    }
    _EVENTS = [CurrentTopologyRequest]

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

        match = datapath.ofproto_parser.OFPMatch(
            in_port=in_port, dl_dst=haddr_to_bin(dst))

        mod = datapath.ofproto_parser.OFPFlowMod(
            datapath=datapath, match=match, cookie=0,
            command=ofproto.OFPFC_ADD, idle_timeout=0, hard_timeout=0,
            priority=ofproto.OFP_DEFAULT_PRIORITY, actions=[])
        datapath.send_msg(mod)

    @set_ev_cls(ofp_event.EventOFPPacketIn, MAIN_DISPATCHER)
    def _packet_in_handler(self, ev):
        msg = ev.msg
        datapath = msg.datapath
        dpid = datapath.id
        pkt = packet.Packet(msg.data)
        eth = pkt.get_protocol(ethernet.ethernet)
        dst = eth.dst
        ofproto = datapath.ofproto

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

        if dpid in self.topologydb.switches:
            switch = self.topologydb.switches[dpid]
            out_ports = {port.port_no for port in switch.ports}
            out_ports -= self.topologydb.disabled_ports[dpid]
            out_ports -= set([msg.in_port])
        else:
            out_ports = set([ofproto.OFPP_FLOOD])

        actions = [datapath.ofproto_parser.OFPActionOutput(out_port)
                   for out_port in out_ports]

        # install a flow to avoid packet_in next time
        if ofproto.OFPP_FLOOD not in out_ports:
            self._add_flow(datapath, msg.in_port, dst, actions)

        # send packet out message for this packet
        data = None
        if msg.buffer_id == ofproto.OFP_NO_BUFFER:
            data = msg.data
        out = datapath.ofproto_parser.OFPPacketOut(
            datapath=datapath, buffer_id=msg.buffer_id, in_port=msg.in_port,
            actions=actions, data=data)
        datapath.send_msg(out)

    @set_ev_cls(CurrentTopologyRequest)
    def _current_topology_request_handler(self, req):
        reply = CurrentTopologyReply(req.src, self.topologydb)
        self.reply_to_request(req, reply)

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

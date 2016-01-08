import struct

from ryu.base import app_manager
from ryu.controller import ofp_event
from ryu.controller.handler import MAIN_DISPATCHER, DEAD_DISPATCHER, set_ev_cls
from ryu.controller.event import EventBase, EventRequestBase, EventReplyBase
from ryu.ofproto import ofproto_v1_0
from ryu.lib.mac import haddr_to_bin, BROADCAST_STR
from ryu.lib.packet import packet, ethernet, ether_types

from util.switch_fdb import SwitchFDB
from topology import FindRouteRequest, BroadcastRequest
from process import RankResolutionRequest, ProcessManager


class EventFDBUpdate(EventBase):
    def __init__(self, dpid, src, dst, port):
        super(EventFDBUpdate, self).__init__()
        self.dpid = dpid
        self.src = src
        self.dst = dst
        self.port = port


class CurrentFDBRequest(EventRequestBase):
    def __init__(self):
        super(CurrentFDBRequest, self).__init__()
        self.dst = "Router"


class CurrentFDBReply(EventReplyBase):
    def __init__(self, dst, fdb):
        super(CurrentFDBReply, self).__init__(dst)
        self.fdb = fdb


class Router(app_manager.RyuApp):
    _EVENTS = [EventFDBUpdate, CurrentFDBRequest]
    _CONTEXTS = {
        "process_manager": ProcessManager,
    }
    OFP_VERSIONS = [ofproto_v1_0.OFP_VERSION]

    def __init__(self, *args, **kwargs):
        super(Router, self).__init__(*args, **kwargs)
        self.fdb = SwitchFDB()
        self.dps = {}

    def _add_flow(self, datapath, src, dst, out_port, actions=[]):
        ofproto = datapath.ofproto

        match = datapath.ofproto_parser.OFPMatch(
            dl_src=haddr_to_bin(src), dl_dst=haddr_to_bin(dst))

        actions = actions + [datapath.ofproto_parser.OFPActionOutput(out_port)]

        mod = datapath.ofproto_parser.OFPFlowMod(
            datapath=datapath, match=match, cookie=0,
            command=ofproto.OFPFC_ADD, idle_timeout=0, hard_timeout=0,
            priority=ofproto.OFP_DEFAULT_PRIORITY,
            flags=ofproto.OFPFF_SEND_FLOW_REM, actions=actions)
        datapath.send_msg(mod)

    @set_ev_cls(CurrentFDBRequest)
    def _current_fdb_request_handler(self, req):
        reply = CurrentFDBReply(req.src, self.fdb)
        self.reply_to_request(req, reply)

    @set_ev_cls(ofp_event.EventOFPStateChange,
                [MAIN_DISPATCHER, DEAD_DISPATCHER])
    def state_change_handler(self, ev):
        dp = ev.datapath
        if ev.state == MAIN_DISPATCHER:
            if dp.id is None:
                return
            self.dps[dp.id] = dp
        elif ev.state == DEAD_DISPATCHER:
            if dp.id is None:
                return
            if dp.id in self.dps:
                del self.dps[dp.id]

    def _add_flows_for_path(self, fdb, src, dst, true_dst=None):
        for (idx, (dpid, out_port)) in enumerate(fdb):
            # Check if a flow for this packet has already been installed
            if self.fdb.exists(dpid, src, dst):
                continue

            # Update FDB and notify to observers
            self.fdb.update(dpid, src, dst, out_port)
            self.send_event_to_observers(
                EventFDBUpdate(dpid, src, dst, out_port)
            )

            # If a datapath having dpid is connected to controller
            if dpid in self.dps:
                datapath = self.dps[dpid]
                if true_dst and idx == len(fdb) - 1:
                    actions = [datapath.ofproto_parser.OFPActionSetDlDst(
                        haddr_to_bin(true_dst)
                    )]
                    self._add_flow(datapath, src, dst, out_port, actions)
                else:
                    self._add_flow(datapath, src, dst, out_port)

    def _send_packet_out(self, fdb, datapath, data, buffer_id):
        ofproto = datapath.ofproto
        ofproto_parser = datapath.ofproto_parser

        # If the packet is buffered, do not re-send packet with packet-out
        if buffer_id != ofproto.OFP_NO_BUFFER:
            data = None

        for (dpid, out_port) in fdb:
            # If dpid is the datapath that caused packet-in
            if datapath.id == dpid:
                actions = [ofproto_parser.OFPActionOutput(out_port)]
                out = ofproto_parser.OFPPacketOut(
                    datapath=datapath, in_port=ofproto.OFPP_NONE,
                    actions=actions, buffer_id=buffer_id,
                    data=data)
                datapath.send_msg(out)
                break

    @set_ev_cls(ofp_event.EventOFPPacketIn, MAIN_DISPATCHER)
    def _packet_in_handler(self, ev):
        msg = ev.msg
        datapath = msg.datapath

        pkt = packet.Packet(msg.data)
        eth = pkt.get_protocol(ethernet.ethernet)
        src = eth.src
        dst = eth.dst

        # ignore LLDP packet
        if eth.ethertype == ether_types.ETH_TYPE_LLDP:
            return
        # ignore broadcast packet (broadcasts are handled by TopologyManager)
        if dst == BROADCAST_STR:
            return
        # ignore IPv6 multicast packets
        if dst.startswith("33:33"):
            return
        # Handle MPI packets
        if dst.startswith("02:00"):
            return self._mpi_packet_in_handler(ev)

        self.logger.info("Packet in at %s (%s) %s -> %s", datapath.id,
                         msg.in_port, src, dst)

        fdb = self.send_request(FindRouteRequest(src, dst)).fdb

        if fdb:
            # Install rules to all datapaths in path
            self._add_flows_for_path(fdb, src, dst)
            # Output packet from current switch
            self._send_packet_out(fdb, datapath, msg.data, msg.buffer_id)
        else:
            req = BroadcastRequest(msg.data, datapath.id, msg.in_port)
            self.send_request(req)

    def _mpi_packet_in_handler(self, ev):
        msg = ev.msg
        datapath = msg.datapath

        pkt = packet.Packet(msg.data)
        eth = pkt.get_protocol(ethernet.ethernet)
        dst = eth.dst
        src = eth.src

        bin_dst = haddr_to_bin(dst)
        src_rank = struct.unpack("<h", bin_dst[2:4])[0]
        dst_rank = struct.unpack("<h", bin_dst[4:6])[0]

        self.logger.info("SDNMPI communication from rank %s to rank %s",
                         src_rank, dst_rank)

        # True MAC address of dst, resolved using ProcessManager
        true_dst = self.send_request(RankResolutionRequest(dst_rank)).mac
        if not true_dst:
            return

        fdb = self.send_request(FindRouteRequest(src, true_dst)).fdb

        if fdb:
            # Install rules to all datapaths in path
            self._add_flows_for_path(fdb, src, dst, true_dst)
            # Output packet from current switch
            self._send_packet_out(fdb, datapath, msg.data, msg.buffer_id)

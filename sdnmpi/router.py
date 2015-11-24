#  import struct

from ryu.base import app_manager
from ryu.controller import ofp_event
from ryu.controller.handler import MAIN_DISPATCHER, DEAD_DISPATCHER, set_ev_cls
from ryu.controller.event import EventBase, EventRequestBase, EventReplyBase
from ryu.ofproto import ofproto_v1_0
from ryu.lib.mac import haddr_to_bin, BROADCAST_STR
from ryu.lib.packet import packet, ethernet, ether_types

from util.switch_fdb import SwitchFDB
from topology import FindRouteRequest
#  from process import RankResolutionRequest, ProcessManager
from process import ProcessManager


class EventFDBUpdate(EventBase):
    def __init__(self, dpid, mac, port):
        super(EventFDBUpdate, self).__init__()
        self.dpid = dpid
        self.mac = mac
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

    def _add_flow(self, datapath, src, dst, out_port):
        ofproto = datapath.ofproto

        match = datapath.ofproto_parser.OFPMatch(
            dl_src=haddr_to_bin(src), dl_dst=haddr_to_bin(dst))

        actions = [
            datapath.ofproto_parser.OFPActionOutput(out_port),
        ]

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

    @set_ev_cls(ofp_event.EventOFPPacketIn, MAIN_DISPATCHER)
    def _packet_in_handler(self, ev):
        msg = ev.msg
        datapath = msg.datapath
        ofproto = datapath.ofproto

        pkt = packet.Packet(msg.data)
        eth = pkt.get_protocol(ethernet.ethernet)
        dst = eth.dst
        src = eth.src

        # ignore LLDP packet
        if eth.ethertype == ether_types.ETH_TYPE_LLDP:
            return
        # ignore broadcast packet (broadcasts are handled by TopologyManager)
        if dst == BROADCAST_STR:
            return
        # ignore IPv6 multicast packets
        if dst.startswith("33:33"):
            return

        dpid = datapath.id

        self.logger.info("Packet in %s %s %s %s", dpid, src, dst, msg.in_port)

        print "Finding route from %s to %s" % (src, dst)
        fdb = self.send_request(FindRouteRequest(src, dst)).fdb
        print "Found route is: %s" % fdb
        # Install rules to all datapaths in path
        if fdb:
            for (dpid, out_port) in fdb:
                if dpid in self.dps:
                    datapath = self.dps[dpid]
                    self._add_flow(datapath, src, dst, out_port)

        # if dst.startswith("02:00"):
        #     bin_dst = haddr_to_bin(dst)
        #     src_rank = struct.unpack("<h", bin_dst[2:4])[0]
        #     dst_rank = struct.unpack("<h", bin_dst[4:6])[0]

        #     self.logger.info("SDNMPI communication from rank %s to rank %s",
        #                      src_rank, dst_rank)

        #     reply = self.send_request(RankResolutionRequest(dst_rank))
        #     dst_mac = reply.mac
        #     out_port = self.fdb.get_port(dpid, dst_mac)
        #     if out_port is not None:
        #         actions = [
        #             datapath.ofproto_parser.OFPActionSetDlDst(
        #                 haddr_to_bin(dst_mac)
        #             ),
        #             datapath.ofproto_parser.OFPActionOutput(out_port),
        #         ]
        #     else:
        #         out_port = ofproto.OFPP_FLOOD
        #         actions = [
        #             datapath.ofproto_parser.OFPActionOutput(out_port),
        #         ]

        # send packet out message for this packet

        data = None
        if msg.buffer_id == ofproto.OFP_NO_BUFFER:
            data = msg.data

        if fdb:
            actions = [
                datapath.ofproto_parser.OFPActionOutput(fdb[0][1]),
            ]
        else:
            actions = [
                datapath.ofproto_parser.OFPActionOutput(ofproto.OFPP_FLOOD),
            ]

        out = datapath.ofproto_parser.OFPPacketOut(
            datapath=datapath, buffer_id=msg.buffer_id, in_port=msg.in_port,
            actions=actions, data=data)
        datapath.send_msg(out)

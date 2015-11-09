import struct

from ryu.base import app_manager
from ryu.controller import ofp_event
from ryu.controller.handler import MAIN_DISPATCHER, set_ev_cls
from ryu.controller.event import EventBase, EventRequestBase, EventReplyBase
from ryu.ofproto import ofproto_v1_0
from ryu.lib.mac import haddr_to_bin, BROADCAST_STR
from ryu.lib.packet import packet, ethernet, ether_types, udp

from util.switch_fdb import SwitchFDB
from process import RankResolutionRequest, ProcessManager


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

    @set_ev_cls(CurrentFDBRequest)
    def _current_fdb_request_handler(self, req):
        reply = CurrentFDBReply(req.src, self.fdb)
        self.reply_to_request(req, reply)

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
        if eth.dst == BROADCAST_STR:
            udp_pkt = pkt.get_protocol(udp.udp)
            if udp_pkt and udp_pkt.dst_port == 61000:
                return

        dpid = datapath.id

        self.logger.info("Packet in %s %s %s %s", dpid, src, dst, msg.in_port)

        # learn a mac address to avoid FLOOD next time.
        self.fdb.update(dpid, src, msg.in_port)
        self.send_event_to_observers(EventFDBUpdate(dpid, src, msg.in_port))

        # check if we know the destionation port; if not we flood
        out_port = self.fdb.get_port(dpid, dst)
        if out_port is None:
            out_port = ofproto.OFPP_FLOOD

        actions = [
            datapath.ofproto_parser.OFPActionOutput(out_port),
        ]
        if dst.startswith("02:00"):
            bin_dst = haddr_to_bin(dst)
            src_rank = struct.unpack("<h", bin_dst[2:4])[0]
            dst_rank = struct.unpack("<h", bin_dst[4:6])[0]

            self.logger.info("SDNMPI communication from rank %s to rank %s",
                             src_rank, dst_rank)

            reply = self.send_request(RankResolutionRequest(dst_rank))
            dst_mac = reply.mac
            out_port = self.fdb.get_port(dpid, dst_mac)
            if out_port is not None:
                actions = [
                    datapath.ofproto_parser.OFPActionSetDlDst(
                        haddr_to_bin(dst_mac)
                    ),
                    datapath.ofproto_parser.OFPActionOutput(out_port),
                ]
            else:
                out_port = ofproto.OFPP_FLOOD
                actions = [
                    datapath.ofproto_parser.OFPActionOutput(out_port),
                ]

        # install a flow to avoid packet_in next time
        if out_port != ofproto.OFPP_FLOOD:
            self._add_flow(datapath, msg.in_port, dst, actions)

        # send packet out message for this packet
        data = None
        if msg.buffer_id == ofproto.OFP_NO_BUFFER:
            data = msg.data
        out = datapath.ofproto_parser.OFPPacketOut(
            datapath=datapath, buffer_id=msg.buffer_id, in_port=msg.in_port,
            actions=actions, data=data)
        datapath.send_msg(out)

from ryu.base import app_manager
from ryu.controller import ofp_event
from ryu.controller.handler import MAIN_DISPATCHER, set_ev_cls
from ryu.controller.event import EventBase, EventRequestBase, EventReplyBase
from ryu.ofproto import ofproto_v1_0
from ryu.lib.mac import BROADCAST_STR
from ryu.lib.packet import packet, ethernet, ether_types, udp
from ryu.lib.packet.ether_types import ETH_TYPE_IP
from ryu.lib.packet.in_proto import IPPROTO_UDP

from util.rank_allocation_db import RankAllocationDB
from protocol.announcement import announcement


class EventProcessAdd(EventBase):
    def __init__(self, rank, mac):
        super(EventProcessAdd, self).__init__()
        self.rank = rank
        self.mac = mac


class EventProcessDelete(EventBase):
    def __init__(self, rank):
        super(EventProcessDelete, self).__init__()
        self.rank = rank


class RankResolutionRequest(EventRequestBase):
    def __init__(self, rank):
        super(RankResolutionRequest, self).__init__()
        self.dst = "ProcessManager"
        self.rank = rank


class RankResolutionReply(EventReplyBase):
    def __init__(self, dst, mac):
        super(RankResolutionReply, self).__init__(dst)
        self.mac = mac


class CurrentProcessAllocationRequest(EventRequestBase):
    def __init__(self):
        super(CurrentProcessAllocationRequest, self).__init__()
        self.dst = "ProcessManager"


class CurrentProcessAllocationReply(EventReplyBase):
    def __init__(self, dst, processes):
        super(CurrentProcessAllocationReply, self).__init__(dst)
        self.processes = processes


class ProcessManager(app_manager.RyuApp):
    _EVENTS = [EventProcessAdd, EventProcessDelete]
    OFP_VERSIONS = [ofproto_v1_0.OFP_VERSION]

    def __init__(self, *args, **kwargs):
        super(ProcessManager, self).__init__(*args, **kwargs)
        self._rankdb = RankAllocationDB()

    @set_ev_cls(ofp_event.EventOFPStateChange, MAIN_DISPATCHER)
    def _state_change_handler(self, ev):
        datapath = ev.datapath
        ofproto = datapath.ofproto
        ofproto_parser = datapath.ofproto_parser

        match = ofproto_parser.OFPMatch(
            dl_type=ETH_TYPE_IP,
            nw_proto=IPPROTO_UDP,
            tp_dst=61000)

        actions = [ofproto_parser.OFPActionOutput(ofproto.OFPP_CONTROLLER)]

        # Install a flow to send all announcement packets to the controller
        mod = datapath.ofproto_parser.OFPFlowMod(
            datapath=datapath, match=match, cookie=0,
            command=ofproto.OFPFC_ADD, idle_timeout=0, hard_timeout=0,
            priority=0xffff, actions=actions)
        datapath.send_msg(mod)

    @set_ev_cls(ofp_event.EventOFPPacketIn, MAIN_DISPATCHER)
    def _packet_in_handler(self, ev):
        msg = ev.msg
        pkt = packet.Packet(msg.data)
        eth = pkt.get_protocol(ethernet.ethernet)

        if eth.dst == BROADCAST_STR:
            if eth.ethertype == ether_types.ETH_TYPE_IP:
                self._broadcast_handler(eth, pkt)

    @set_ev_cls(RankResolutionRequest)
    def _rank_resolution_handler(self, req):
        reply = RankResolutionReply(req.src, self._rankdb.get_mac(req.rank))
        self.reply_to_request(req, reply)

    @set_ev_cls(CurrentProcessAllocationRequest)
    def _current_process_allocation_request(self, req):
        reply = CurrentProcessAllocationReply(req.src, self._rankdb)
        self.reply_to_request(req, reply)

    def _broadcast_handler(self, eth, pkt):
        udp_pkt = pkt.get_protocol(udp.udp)
        if udp_pkt and udp_pkt.dst_port == 61000:
            payload = pkt.protocols[-1]
            ann = announcement.parse(payload)

            if ann.type == "LAUNCH":
                rank = ann.args.rank
                self._rankdb.add_process(rank, eth.src)
                self.send_event_to_observers(EventProcessAdd(rank, eth.src))
                self.logger.info("MPI process %s started at %s", rank, eth.src)
            elif ann.type == "EXIT":
                rank = ann.args.rank
                self._rankdb.delete_prcess(rank)
                self.send_event_to_observers(EventProcessDelete(rank))
                self.logger.info("MPI process %s exited at %s", rank, eth.src)
            return True

        return False

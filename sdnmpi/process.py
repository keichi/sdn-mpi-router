from ryu.base import app_manager
from ryu.controller import ofp_event
from ryu.controller.handler import MAIN_DISPATCHER, set_ev_cls
from ryu.controller.event import EventBase, EventRequestBase, EventReplyBase
from ryu.ofproto import ofproto_v1_0
from ryu.lib.mac import BROADCAST_STR
from ryu.lib.packet import packet, ethernet, ether_types, udp

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
        self.dst = "ProcessManager"
        self.rank = rank


class RankResolutionReply(EventReplyBase):
    def __init__(self, dst, mac):
        self.mac = mac


class ProcessManager(app_manager.RyuApp):
    _EVENTS = [EventProcessAdd, EventProcessDelete]
    OFP_VERSIONS = [ofproto_v1_0.OFP_VERSION]

    def __init__(self, *args, **kwargs):
        super(ProcessManager, self).__init__(*args, **kwargs)
        self._rankdb = RankAllocationDB()

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

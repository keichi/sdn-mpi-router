from operator import attrgetter
import time

from ryu.base import app_manager
from ryu.controller import ofp_event
from ryu.controller.handler import MAIN_DISPATCHER, DEAD_DISPATCHER, set_ev_cls
from ryu.ofproto import ofproto_v1_0
from ryu.lib import hub


class PortStats(object):
    def __init__(self, timestamp):
        super(PortStats, self).__init__()
        self.timestamp = timestamp
        self.rx_packets = 0
        self.rx_bytes = 0
        self.tx_packets = 0
        self.tx_bytes = 0


class Monitor(app_manager.RyuApp):
    OFP_VERSIONS = [ofproto_v1_0.OFP_VERSION]

    MONITOR_INTERVAL = 1

    def __init__(self, *args, **kwargs):
        super(Monitor, self).__init__(*args, **kwargs)
        # DPID -> Datapath
        self.datapaths = {}
        # DPID -> Port Number -> PortStats
        self.datapath_stats = {}
        self.monitor_thread = hub.spawn(self._monitor)

    @set_ev_cls(ofp_event.EventOFPStateChange,
                [MAIN_DISPATCHER, DEAD_DISPATCHER])
    def _state_change_handler(self, ev):
        datapath = ev.datapath
        if ev.state == MAIN_DISPATCHER:
            if datapath.id not in self.datapaths:
                self.datapaths[datapath.id] = datapath
                self.datapath_stats[datapath.id] = {}
        elif ev.state == DEAD_DISPATCHER:
            if datapath.id in self.datapaths:
                del self.datapaths[datapath.id]
                del self.datapath_stats[datapath.id]

    def _monitor(self):
        self.logger.debug("Starting monitor thread")
        while True:
            for datapath in self.datapaths.values():
                self._request_stats(datapath)
            hub.sleep(self.MONITOR_INTERVAL)

    def _request_stats(self, datapath):
        self.logger.debug("Sending port stats request to: %016x", datapath.id)
        ofproto = datapath.ofproto
        parser = datapath.ofproto_parser

        req = parser.OFPPortStatsRequest(datapath, 0, ofproto.OFPP_NONE)
        datapath.send_msg(req)

    @set_ev_cls(ofp_event.EventOFPPortStatsReply, MAIN_DISPATCHER)
    def _port_stats_reply_handler(self, ev):
        dpid = ev.msg.datapath.id
        body = ev.msg.body

        for stat in sorted(body, key=attrgetter("port_no")):
            current_timestamp = time.time()

            if stat.port_no not in self.datapath_stats[dpid]:
                last_stat = PortStats(current_timestamp)
                last_stat.rx_packets = stat.rx_packets
                last_stat.rx_bytes = stat.rx_bytes
                last_stat.tx_packets = stat.tx_packets
                last_stat.tx_bytes = stat.tx_bytes
                self.datapath_stats[dpid][stat.port_no] = last_stat
                continue

            last_stat = self.datapath_stats[dpid][stat.port_no]
            time_delta = current_timestamp - last_stat.timestamp

            rx_pps = (stat.rx_packets - last_stat.rx_packets) / time_delta
            rx_bps = (stat.rx_bytes - last_stat.rx_bytes) / time_delta
            tx_pps = (stat.tx_packets - last_stat.tx_packets) / time_delta
            tx_bps = (stat.tx_bytes - last_stat.tx_bytes) / time_delta

            self.logger.info('%016x\t%d\t%d\t%d\t%d\t%d', dpid, stat.port_no,
                             rx_pps, rx_bps, tx_pps, tx_bps)

            last_stat.timestamp = current_timestamp
            last_stat.rx_packets = stat.rx_packets
            last_stat.rx_bytes = stat.rx_bytes
            last_stat.tx_packets = stat.tx_packets
            last_stat.tx_bytes = stat.tx_bytes

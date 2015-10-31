import struct

from socket import error as SocketError

from ryu.base import app_manager
from ryu.lib.hub import spawn
from ryu.controller import ofp_event
from ryu.controller.handler import MAIN_DISPATCHER, set_ev_cls
from ryu.ofproto import ofproto_v1_0
from ryu.lib.mac import haddr_to_bin, BROADCAST_STR
from ryu.lib.packet import packet, ethernet, ether_types, udp
from ryu.app.wsgi import (ControllerBase, WSGIApplication, websocket,
                          WebSocketRPCClient)
from ryu.contrib.tinyrpc.exc import InvalidReplyError
from ryu.topology import event, switches

from util.rank_allocation_db import RankAllocationDB
from util.switch_fdb import SwitchFDB
from util.topology_db import TopologyDB
from protocol.announcement import announcement


class SDNMPIRouter(app_manager.RyuApp):
    _CONTEXTS = {
        "wsgi": WSGIApplication,
        "switches": switches.Switches,
    }
    OFP_VERSIONS = [ofproto_v1_0.OFP_VERSION]

    def __init__(self, *args, **kwargs):
        super(SDNMPIRouter, self).__init__(*args, **kwargs)
        self.fdb = SwitchFDB()
        self.fdb.changed.connect(self.fdb_update_handler)

        self.rankdb = RankAllocationDB()
        self.rankdb.process_added.connect(
            self.rankdb_process_added_handler
        )
        self.rankdb.process_deleted.connect(
            self.rankdb_process_deleted_handler
        )

        self.topologydb = TopologyDB()
        self.topologydb.switch_added.connect(
            self.topologydb_switch_added_handler
        )
        self.topologydb.switch_deleted.connect(
            self.topologydb_switch_deleted_handler
        )
        self.topologydb.link_added.connect(
            self.topologydb_link_added_handler
        )
        self.topologydb.link_deleted.connect(
            self.topologydb_link_deleted_handler
        )
        self.topologydb.host_added.connect(
            self.topologydb_host_added_handler
        )

        self.rpc_clients = []
        wsgi = kwargs["wsgi"]
        wsgi.register(WebSocketSDNMPIController, {"app": self})

    def rankdb_process_added_handler(self, rank, mac):
        self._rpc_broadcall("add_process", rank, mac)

    def rankdb_process_deleted_handler(self, rank):
        self._rpc_broadcall("delete_process", rank)

    def topologydb_switch_added_handler(self, switch):
        self._rpc_broadcall("add_switch", switch.to_dict())

    def topologydb_switch_deleted_handler(self, switch):
        self._rpc_broadcall("delete_switch", switch.to_dict())

    def topologydb_link_added_handler(self, link):
        self._rpc_broadcall("add_link", link.to_dict())

    def topologydb_link_deleted_handler(self, link):
        self._rpc_broadcall("delete_link", link.to_dict())

    def topologydb_host_added_handler(self, host):
        self._rpc_broadcall("add_host", host.to_dict())

    def fdb_update_handler(self, dpid, mac, port):
        self._rpc_broadcall("update_fdb", dpid, mac, port)

    def init_client(self, rpc_client):
        self._rpc_call(rpc_client, "init_fdb", self.fdb.to_dict())
        self._rpc_call(rpc_client, "init_rankdb", self.rankdb.to_dict())
        self._rpc_call(rpc_client, "init_topologydb",
                       self.topologydb.to_dict())

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

    def add_flow(self, datapath, in_port, dst, actions):
        ofproto = datapath.ofproto

        match = datapath.ofproto_parser.OFPMatch(
            in_port=in_port, dl_dst=haddr_to_bin(dst))

        mod = datapath.ofproto_parser.OFPFlowMod(
            datapath=datapath, match=match, cookie=0,
            command=ofproto.OFPFC_ADD, idle_timeout=0, hard_timeout=0,
            priority=ofproto.OFP_DEFAULT_PRIORITY,
            flags=ofproto.OFPFF_SEND_FLOW_REM, actions=actions)
        datapath.send_msg(mod)

    def broadcast_handler(self, eth, pkt):
        udp_pkt = pkt.get_protocol(udp.udp)
        if udp_pkt and udp_pkt.dst_port == 61000:
            payload = pkt.protocols[-1]
            ann = announcement.parse(payload)

            if ann.type == "LAUNCH":
                rank = ann.args.rank
                self.rankdb.add_process(rank, eth.src)
                self.logger.info("MPI process %s started at %s", rank, eth.src)
            elif ann.type == "EXIT":
                rank = ann.args.rank
                self.rankdb.delete_prcess(rank)
                self.logger.info("MPI process %s exited at %s", rank, eth.src)
            return True

        return False

    @set_ev_cls(ofp_event.EventOFPPacketIn, MAIN_DISPATCHER)
    def packet_in_handler(self, ev):
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
            handled = self.broadcast_handler(eth, pkt)
            if handled:
                return

        dpid = datapath.id

        self.logger.info("Packet in %s %s %s %s", dpid, src, dst, msg.in_port)

        # learn a mac address to avoid FLOOD next time.
        self.fdb.update(dpid, src, msg.in_port)

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

            dst_mac = self.rankdb.get_mac(dst_rank)
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
            self.add_flow(datapath, msg.in_port, dst, actions)

        # send packet out message for this packet
        data = None
        if msg.buffer_id == ofproto.OFP_NO_BUFFER:
            data = msg.data
        out = datapath.ofproto_parser.OFPPacketOut(
            datapath=datapath, buffer_id=msg.buffer_id, in_port=msg.in_port,
            actions=actions, data=data)
        datapath.send_msg(out)

    def _rpc_call(self, rpc_client, func_name, *args):
        try:
            # one_way option is ignored in Ryu 3.26 :-(
            rpc_server = rpc_client.get_proxy()
            getattr(rpc_server, func_name)(*args)
        except SocketError:
            self.logger.debug("WebSocket disconnected: ", rpc_server.ws)
            return False
        except InvalidReplyError as e:
            self.logger.error(e)
        return True

    def _rpc_broadcall(self, func_name, *args):
        disconnected_clients = []
        for rpc_client in self.rpc_clients:
            success = self._rpc_call(rpc_client, func_name, *args)
            if not success:
                disconnected_clients.append(rpc_client)
        for client in disconnected_clients:
            self.rpc_clients.remove(client)


class WebSocketSDNMPIController(ControllerBase):
    def __init__(self, req, link, data, **config):
        super(WebSocketSDNMPIController, self).__init__(req, link, data,
                                                        **config)
        self.app = data["app"]

    @websocket("sdnmpi", "/v1.0/sdnmpi/ws")
    def _websocket_handler(self, ws):
        rpc_client = WebSocketRPCClient(ws)
        self.app.rpc_clients.append(rpc_client)
        # init_client requires a running event loop
        spawn(self.app.init_client, rpc_client)
        rpc_client.serve_forever()

from socket import error as SocketError
from ryu.topology.switches import Switches
from ryu.topology.event import (EventSwitchEnter, EventSwitchLeave,
                                EventHostAdd, EventLinkAdd, EventLinkDelete)
from ryu.base.app_manager import RyuApp
from ryu.lib.hub import spawn
from ryu.app.wsgi import (ControllerBase, WSGIApplication, websocket,
                          WebSocketRPCClient)
from ryu.contrib.tinyrpc.exc import InvalidReplyError
from ryu.controller.handler import set_ev_cls

from process import (CurrentProcessAllocationRequest, EventProcessAdd,
                     EventProcessDelete)
from topology import CurrentTopologyRequest
from router import CurrentFDBRequest, EventFDBUpdate


class RPCInterface(RyuApp):
    _CONTEXTS = {
        "wsgi": WSGIApplication,
        "switches": Switches,
    }

    def __init__(self, *args, **kwargs):
        super(RPCInterface, self).__init__(*args, **kwargs)
        self.rpc_clients = []

        wsgi = kwargs["wsgi"]
        wsgi.register(WebSocketSDNMPIController, {"app": self})

    def init_client(self, rpc_client):
        fdb = self.send_request(CurrentFDBRequest()).fdb
        self._rpc_call(rpc_client, "init_fdb", fdb.to_dict())
        rankdb = self.send_request(CurrentProcessAllocationRequest()).processes
        self._rpc_call(rpc_client, "init_rankdb", rankdb.to_dict())
        topologydb = self.send_request(CurrentTopologyRequest()).topology
        self._rpc_call(rpc_client, "init_topologydb", topologydb.to_dict())

    @set_ev_cls(EventProcessAdd)
    def _event_process_add_handler(self, ev):
        self._rpc_broadcall("add_process", ev.rank, ev.mac)

    @set_ev_cls(EventProcessDelete)
    def _event_process_delete_handler(self, ev):
        self._rpc_broadcall("delete_process", ev.rank)

    @set_ev_cls(EventFDBUpdate)
    def _event_fdb_update_handler(self, ev):
        self._rpc_broadcall("update_fdb", ev.dpid, ev.mac, ev.port)

    @set_ev_cls(EventSwitchEnter)
    def _event_switch_enter_handler(self, ev):
        self._rpc_broadcall("add_switch", ev.switch.to_dict())

    @set_ev_cls(EventSwitchLeave)
    def _event_switch_leave_handler(self, ev):
        self._rpc_broadcall("delete_switch", ev.switch.to_dict())

    @set_ev_cls(EventLinkAdd)
    def _event_link_add_handler(self, ev):
        self._rpc_broadcall("add_link", ev.link.to_dict())

    @set_ev_cls(EventLinkDelete)
    def _event_link_delete_handler(self, ev):
        self._rpc_broadcall("delete_link", ev.link.to_dict())

    @set_ev_cls(EventHostAdd)
    def _event_host_add_handler(self, ev):
        self._rpc_broadcall("add_host", ev.host.to_dict())

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

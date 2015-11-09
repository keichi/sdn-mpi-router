from ryu.base import app_manager
from ryu.controller.handler import set_ev_cls
from ryu.controller.event import EventRequestBase, EventReplyBase
from ryu.topology import event, switches

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

from ryu.base import app_manager
from ryu.controller.handler import set_ev_cls
from ryu.topology import event, switches

from util.topology_db import TopologyDB


class TopologyManager(app_manager.RyuApp):
    _CONTEXTS = {
        "switches": switches.Switches,
    }

    def __init__(self, *args, **kwargs):
        super(TopologyManager, self).__init__(*args, **kwargs)
        self.topologydb = TopologyDB()

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

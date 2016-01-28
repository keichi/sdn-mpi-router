from unittest import TestCase
from nose.tools import eq_

from tests.mock import MockPort, MockLink, MockHost, MockSwitch
from sdnmpi.util.topology_db import TopologyDB

MAC1 = "02:00:00:00:00:01"
MAC2 = "02:00:00:00:00:02"
MAC3 = "02:00:00:00:00:03"
MAC4 = "02:00:00:00:00:04"


class TopologyDBTestCase(TestCase):
    def setUp(self):
        self.topology = TopologyDB()

        port11 = MockPort(1, 1)
        port12 = MockPort(1, 2)
        port13 = MockPort(1, 3)
        port21 = MockPort(2, 1)
        port22 = MockPort(2, 2)
        port23 = MockPort(2, 3)
        port31 = MockPort(3, 1)
        port32 = MockPort(3, 2)
        port33 = MockPort(3, 3)
        port41 = MockPort(4, 1)
        port42 = MockPort(4, 2)
        port43 = MockPort(4, 3)

        self.topology.links = {
            1: {
                2: MockLink(port12, port22),
                3: MockLink(port13, port33),
            },
            2: {
                1: MockLink(port22, port12),
                4: MockLink(port23, port42),
            },
            3: {
                1: MockLink(port33, port13),
                4: MockLink(port32, port43),
            },
            4: {
                2: MockLink(port42, port23),
                3: MockLink(port43, port32),
            },
        }

        self.topology.hosts = {
            MAC1: MockHost(MAC1, port11),
            MAC2: MockHost(MAC2, port21),
            MAC3: MockHost(MAC3, port31),
            MAC4: MockHost(MAC4, port41),
        }

        self.topology.switches = {
            1: MockSwitch(1),
            2: MockSwitch(2),
            3: MockSwitch(3),
            4: MockSwitch(4),
        }

    def test_find_route_inter_switch(self):
        route = self.topology.find_route(MAC1, MAC1)
        eq_(route, [(1, 1)])
        route = self.topology.find_route(MAC2, MAC2)
        eq_(route, [(2, 1)])
        route = self.topology.find_route(MAC3, MAC3)
        eq_(route, [(3, 1)])
        route = self.topology.find_route(MAC4, MAC4)
        eq_(route, [(4, 1)])

    def test_find_route_unreachable(self):
        del self.topology.links[1]
        route = self.topology.find_route(MAC1, MAC2)
        eq_(route, [])
        route = self.topology.find_route(MAC1, MAC3)
        eq_(route, [])
        route = self.topology.find_route(MAC1, MAC4)
        eq_(route, [])

    def test_find_route_intra_switch(self):
        route = self.topology.find_route(MAC1, MAC2)
        eq_(route, [(1, 2), (2, 1)])
        route = self.topology.find_route(MAC1, MAC3)
        eq_(route, [(1, 3), (3, 1)])
        route = self.topology.find_route(MAC2, MAC4)
        eq_(route, [(2, 3), (4, 1)])
        route = self.topology.find_route(MAC3, MAC4)
        eq_(route, [(3, 2), (4, 1)])

    def test_find_multiple_routes(self):
        routes = self.topology.find_route(MAC1, MAC4, True)
        route1 = [(1, 2), (2, 3), (4, 1)]
        route2 = [(1, 3), (3, 2), (4, 1)]
        eq_(sorted(routes), sorted([route1, route2]))

        routes = self.topology.find_route(MAC3, MAC4, True)
        route1 = [(3, 2), (4, 1)]
        eq_(sorted(routes), [route1])

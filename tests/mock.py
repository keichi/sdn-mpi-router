class MockDatapath(object):
    def __init__(self, id):
        super(MockDatapath, self).__init__()
        self.id = id


class MockSwitch(object):
    def __init__(self, id):
        super(MockSwitch, self).__init__()
        self.dp = MockDatapath(id)


class MockPort(object):
    def __init__(self, dpid, port_no):
        super(MockPort, self).__init__()
        self.dpid = dpid
        self.port_no = port_no


class MockHost(object):
    def __init__(self, mac, port):
        super(MockHost, self).__init__()
        self.mac = mac
        self.port = port


class MockLink(object):
    def __init__(self, src, dst):
        super(MockLink, self).__init__()
        self.src = src
        self.dst = dst

#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
#

import unittest2 as unittest
from proton import Message, Timeout
from system_test import TestCase, Qdrouterd, main_module, TIMEOUT
from proton.handlers import MessagingHandler
from proton.reactor import Container, DynamicNodeProperties

# PROTON-828:
try:
    from proton import MODIFIED
except ImportError:
    from proton import PN_STATUS_MODIFIED as MODIFIED


class RouterTest(TestCase):

    inter_router_port = None

    @classmethod
    def setUpClass(cls):
        """Start a router"""
        super(RouterTest, cls).setUpClass()

        def router(name, mode, connection, extra=None):
            config = [
                ('router', {'mode': mode, 'id': name}),
                ('listener', {'port': cls.tester.get_port(), 'stripAnnotations': 'no'}),
                ('listener', {'port': cls.tester.get_port(), 'stripAnnotations': 'no', 'multiTenant': 'yes'}),
                ('listener', {'port': cls.tester.get_port(), 'stripAnnotations': 'no', 'role': 'route-container'}),
                ('linkRoute', {'prefix': '0.0.0.0/link', 'dir': 'in', 'containerId': 'LRC'}),
                ('linkRoute', {'prefix': '0.0.0.0/link', 'dir': 'out', 'containerId': 'LRC'}),
                ('autoLink', {'addr': '0.0.0.0/queue.waypoint', 'containerId': 'ALC', 'dir': 'in'}),
                ('autoLink', {'addr': '0.0.0.0/queue.waypoint', 'containerId': 'ALC', 'dir': 'out'}),
                ('address', {'prefix': 'closest', 'distribution': 'closest'}),
                ('address', {'prefix': 'spread', 'distribution': 'balanced'}),
                ('address', {'prefix': 'multicast', 'distribution': 'multicast'}),
                ('address', {'prefix': '0.0.0.0/queue', 'waypoint': 'yes'}),
                connection
            ]

            if extra:
                config.append(extra)
            config = Qdrouterd.Config(config)
            cls.routers.append(cls.tester.qdrouterd(name, config, wait=True))

        cls.routers = []

        inter_router_port = cls.tester.get_port()
        edge_port_A       = cls.tester.get_port()
        edge_port_B       = cls.tester.get_port()

        router('INT.A', 'interior', ('listener', {'role': 'inter-router', 'port': inter_router_port}),
               ('listener', {'role': 'edge-uplink', 'port': edge_port_A}))
        router('INT.B', 'interior', ('connector', {'name': 'connectorToA', 'role': 'inter-router', 'port': inter_router_port}),
               ('listener', {'role': 'edge-uplink', 'port': edge_port_B}))
        router('EA1',   'edge',     ('connector', {'name': 'uplink', 'role': 'edge-uplink', 'port': edge_port_A}))
        router('EA2',   'edge',     ('connector', {'name': 'uplink', 'role': 'edge-uplink', 'port': edge_port_A}))
        router('EB1',   'edge',     ('connector', {'name': 'uplink', 'role': 'edge-uplink', 'port': edge_port_B}))
        router('EB2',   'edge',     ('connector', {'name': 'uplink', 'role': 'edge-uplink', 'port': edge_port_B}))

        cls.routers[0].wait_router_connected('INT.B')
        cls.routers[1].wait_router_connected('INT.A')


    def test_01_connectivity_INTA_EA1(self):
        test = ConnectivityTest(self.routers[0].addresses[0],
                                self.routers[2].addresses[0],
                                'EA1')
        test.run()
        self.assertEqual(None, test.error)

    def test_02_connectivity_INTA_EA2(self):
        test = ConnectivityTest(self.routers[0].addresses[0],
                                self.routers[3].addresses[0],
                                'EA2')
        test.run()
        self.assertEqual(None, test.error)

    def test_03_connectivity_INTB_EB1(self):
        test = ConnectivityTest(self.routers[1].addresses[0],
                                self.routers[4].addresses[0],
                                'EB1')
        test.run()
        self.assertEqual(None, test.error)

    def test_04_connectivity_INTB_EB2(self):
        test = ConnectivityTest(self.routers[1].addresses[0],
                                self.routers[5].addresses[0],
                                'EB2')
        test.run()
        self.assertEqual(None, test.error)

    def test_05_dynamic_address_same_edge(self):
        test = DynamicAddressTest(self.routers[2].addresses[0],
                                  self.routers[2].addresses[0])
        test.run()
        self.assertEqual(None, test.error)

    def test_06_dynamic_address_int_edge(self):
        test = DynamicAddressTest(self.routers[2].addresses[0],
                                  self.routers[0].addresses[0])
        test.run()
        self.assertEqual(None, test.error)

    def test_07_dynamic_address_int_int_edge(self):
        test = DynamicAddressTest(self.routers[2].addresses[0],
                                  self.routers[1].addresses[0])
        test.run()
        self.assertEqual(None, test.error)

    def test_08_dynamic_address_edge_int(self):
        test = DynamicAddressTest(self.routers[0].addresses[0],
                                  self.routers[2].addresses[0])
        test.run()
        self.assertEqual(None, test.error)

    def test_09_dynamic_address_edge_int_int(self):
        test = DynamicAddressTest(self.routers[1].addresses[0],
                                  self.routers[2].addresses[0])
        test.run()
        self.assertEqual(None, test.error)

    def test_10_dynamic_address_edge_int_edge(self):
        test = DynamicAddressTest(self.routers[2].addresses[0],
                                  self.routers[3].addresses[0])
        test.run()
        self.assertEqual(None, test.error)

    def test_11_dynamic_address_edge_int_int_edge(self):
        test = DynamicAddressTest(self.routers[2].addresses[0],
                                  self.routers[4].addresses[0])
        test.run()
        self.assertEqual(None, test.error)

    def test_12_one_mobile_receiver_same_edge(self):
        test = MobileUnicastTest(self.routers[2].addresses[0],
                                 self.routers[2].addresses[0],
                                 "test_12")
        test.run()
        self.assertEqual(None, test.error)

    def test_13_one_mobile_receiver_int_edge(self):
        test = MobileUnicastTest(self.routers[2].addresses[0],
                                 self.routers[0].addresses[0],
                                 "test_13")
        test.run()
        self.assertEqual(None, test.error)

    def test_14_one_mobile_receiver_int_int_edge(self):
        test = MobileUnicastTest(self.routers[2].addresses[0],
                                 self.routers[1].addresses[0],
                                 "test_14")
        test.run()
        self.assertEqual(None, test.error)

    def test_15_one_mobile_receiver_edge_int(self):
        test = MobileUnicastTest(self.routers[0].addresses[0],
                                 self.routers[2].addresses[0],
                                 "test_15")
        test.run()
        self.assertEqual(None, test.error)

    def test_16_one_mobile_receiver_edge_int_int(self):
        test = MobileUnicastTest(self.routers[1].addresses[0],
                                 self.routers[2].addresses[0],
                                 "test_16")
        test.run()
        self.assertEqual(None, test.error)

    def test_17_one_mobile_receiver_edge_int_edge(self):
        test = MobileUnicastTest(self.routers[2].addresses[0],
                                 self.routers[3].addresses[0],
                                 "test_17")
        test.run()
        self.assertEqual(None, test.error)

    def test_18_one_mobile_receiver_edge_int_int_edge(self):
        test = MobileUnicastTest(self.routers[2].addresses[0],
                                 self.routers[4].addresses[0],
                                 "test_18")
        test.run()
        self.assertEqual(None, test.error)


class Entity(object):
    def __init__(self, status_code, status_description, body):
        self.status_code        = status_code
        self.status_description = status_description
        if body.__class__ == dict and len(body.keys()) == 2 and 'attributeNames' in body.keys() and 'results' in body.keys():
            results = []
            names   = body['attributeNames']
            for result in body['results']:
                result_map = {}
                for i in range(len(names)):
                    result_map[names[i]] = result[i]
                results.append(Entity(status_code, status_description, result_map))
            self.attrs = {'results': results}
        else:
            self.attrs = body

    def __getattr__(self, key):
        return self.attrs[key]


class RouterProxy(object):
    def __init__(self, reply_addr):
        self.reply_addr = reply_addr

    def response(self, msg):
        ap = msg.properties
        return Entity(ap['statusCode'], ap['statusDescription'], msg.body)

    def query_connections(self):
        ap = {'operation': 'QUERY', 'type': 'org.apache.qpid.dispatch.connection'}
        return Message(properties=ap, reply_to=self.reply_addr)

    def query_links(self):
        ap = {'operation': 'QUERY', 'type': 'org.apache.qpid.dispatch.router.link'}
        return Message(properties=ap, reply_to=self.reply_addr)


class Timeout(object):
    def __init__(self, parent):
        self.parent = parent

    def on_timer_task(self, event):
        self.parent.timeout()


class PollTimeout(object):
    def __init__(self, parent):
        self.parent = parent

    def on_timer_task(self, event):
        self.parent.poll_timeout()


class ConnectivityTest(MessagingHandler):
    def __init__(self, interior_host, edge_host, edge_id):
        super(ConnectivityTest, self).__init__()
        self.interior_host = interior_host
        self.edge_host     = edge_host
        self.edge_id       = edge_id

        self.interior_conn = None
        self.edge_conn     = None
        self.error         = None
        self.proxy         = None
        self.query_sent    = False

    def timeout(self):
        self.error = "Timeout Expired"
        self.interior_conn.close()
        self.edge_conn.close()

    def on_start(self, event):
        self.timer          = event.reactor.schedule(10.0, Timeout(self))
        self.interior_conn  = event.container.connect(self.interior_host)
        self.edge_conn      = event.container.connect(self.edge_host)
        self.reply_receiver = event.container.create_receiver(self.interior_conn, dynamic=True)

    def on_link_opened(self, event):
        if event.receiver == self.reply_receiver:
            self.proxy        = RouterProxy(self.reply_receiver.remote_source.address)
            self.agent_sender = event.container.create_sender(self.interior_conn, "$management")

    def on_sendable(self, event):
        if not self.query_sent:
            self.query_sent = True
            self.agent_sender.send(self.proxy.query_connections())

    def on_message(self, event):
        if event.receiver == self.reply_receiver:
            response = self.proxy.response(event.message)
            if response.status_code != 200:
                self.error = "Unexpected error code from agent: %d - %s" % (response.status_code, response.status_description)
            connections = response.results
            count = 0
            for conn in connections:
                if conn.role == 'edge-uplink' and conn.container == self.edge_id:
                    count += 1
            if count != 1:
                self.error = "Incorrect uplink count for container-id.  Expected 1, got %d" % count
            self.interior_conn.close()
            self.edge_conn.close()
            self.timer.cancel()

    def run(self):
        Container(self).run()


class DynamicAddressTest(MessagingHandler):
    """
    Create a receiver on the receiver-host with a dynamic source.
    Create a sender on the sender-host with the address of the dynamic source.
    Deliver a batch of messages from sender and verify they are all received on the receiver and accepted.
    """
    def __init__(self, receiver_host, sender_host):
        super(DynamicAddressTest, self).__init__()
        self.receiver_host = receiver_host
        self.sender_host   = sender_host

        self.receiver_conn = None
        self.sender_conn   = None
        self.receiver      = None
        self.address       = None
        self.count         = 10
        self.n_rcvd        = 0
        self.n_sent        = 0
        self.n_accepted    = 0
        self.error         = None

    def timeout(self):
        self.error = "Timeout Expired - n_sent=%d n_rcvd=%d n_accepted=%daddr=%s" % \
                     (self.n_sent, self.n_rcvd, self.n_accepted, self.address)
        self.receiver_conn.close()
        self.sender_conn.close()

    def on_start(self, event):
        self.timer         = event.reactor.schedule(5.0, Timeout(self))
        self.receiver_conn = event.container.connect(self.receiver_host)
        self.sender_conn   = event.container.connect(self.sender_host)
        self.receiver      = event.container.create_receiver(self.receiver_conn, dynamic=True)

    def on_link_opened(self, event):
        if event.receiver == self.receiver:
            self.address = self.receiver.remote_source.address
            self.sender  = event.container.create_sender(self.sender_conn, self.address)

    def on_sendable(self, event):
        while self.n_sent < self.count:
            self.sender.send(Message(body="Message %d" % self.n_sent))
            self.n_sent += 1

    def on_message(self, event):
        self.n_rcvd += 1

    def on_accepted(self, event):
        self.n_accepted += 1
        if self.n_accepted == self.count:
            self.receiver_conn.close()
            self.sender_conn.close()
            self.timer.cancel()

    def run(self):
        Container(self).run()


class MobileUnicastTest(MessagingHandler):
    """
    Create a receiver on the receiver-host with the supplied address.
    Create a sender on the sender-host with the supplied address.
    Deliver a batch of messages from sender and verify they are all received on the receiver and accepted.
    """
    def __init__(self, receiver_host, sender_host, address):
        super(MobileUnicastTest, self).__init__()
        self.receiver_host = receiver_host
        self.sender_host   = sender_host
        self.address       = address

        self.receiver_conn = None
        self.sender_conn   = None
        self.receiver      = None
        self.count         = 10
        self.n_rcvd        = 0
        self.n_sent        = 0
        self.n_accepted    = 0
        self.error         = None

    def timeout(self):
        self.error = "Timeout Expired - n_sent=%d n_rcvd=%d n_accepted=%d addr=%s" % \
                     (self.n_sent, self.n_rcvd, self.n_accepted, self.address)
        self.receiver_conn.close()
        self.sender_conn.close()

    def on_start(self, event):
        self.timer         = event.reactor.schedule(5.0, Timeout(self))
        self.receiver_conn = event.container.connect(self.receiver_host)
        self.sender_conn   = event.container.connect(self.sender_host)
        self.receiver      = event.container.create_receiver(self.receiver_conn, self.address)
        self.sender        = event.container.create_sender(self.sender_conn, self.address)

    def on_sendable(self, event):
        while self.n_sent < self.count:
            self.sender.send(Message(body="Message %d" % self.n_sent))
            self.n_sent += 1

    def on_message(self, event):
        self.n_rcvd += 1

    def on_accepted(self, event):
        self.n_accepted += 1
        if self.n_accepted == self.count:
            self.receiver_conn.close()
            self.sender_conn.close()
            self.timer.cancel()

    def run(self):
        Container(self).run()


if __name__ == '__main__':
    unittest.main(main_module())

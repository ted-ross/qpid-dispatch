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

from __future__ import unicode_literals
from __future__ import division
from __future__ import absolute_import
from __future__ import print_function

from time import sleep
from threading import Event
from threading import Timer

import unittest2 as unittest
from proton import Message, Timeout, symbol
from system_test import TestCase, Qdrouterd, main_module, TIMEOUT, MgmtMsgProxy
from system_test import AsyncTestReceiver
from system_test import AsyncTestSender
from system_test import QdManager
from system_tests_link_routes import ConnLinkRouteService
from proton.handlers import MessagingHandler
from proton.reactor import Container, DynamicNodeProperties
from proton.utils import BlockingConnection
from qpid_dispatch.management.client import Node
from subprocess import PIPE, STDOUT
import re


class AddrTimer(object):
    def __init__(self, parent):
        self.parent = parent

    def on_timer_task(self, event):
            self.parent.check_address()


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
                ('address', {'prefix': 'dest', 'undeliverableSuffix': '.alt'}),
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
               ('listener', {'role': 'edge', 'port': edge_port_A}))
        router('INT.B', 'interior', ('connector', {'name': 'connectorToA', 'role': 'inter-router', 'port': inter_router_port}),
               ('listener', {'role': 'edge', 'port': edge_port_B}))
        router('EA1',   'edge',     ('connector', {'name': 'edge', 'role': 'edge', 'port': edge_port_A}))
        router('EA2',   'edge',     ('connector', {'name': 'edge', 'role': 'edge', 'port': edge_port_A}))
        router('EB1',   'edge',     ('connector', {'name': 'edge', 'role': 'edge', 'port': edge_port_B}))
        router('EB2',   'edge',     ('connector', {'name': 'edge', 'role': 'edge', 'port': edge_port_B}))

        cls.routers[0].wait_router_connected('INT.B')
        cls.routers[1].wait_router_connected('INT.A')


    def test_01_sender_first_primary_same_interior(self):
        test = SenderFirstTest(self.routers[0].addresses[0],
                               self.routers[0].addresses[0],
                               'dest.01',
                               'dest.01')
        test.run()
        self.assertEqual(None, test.error)

    def test_02_sender_first_alternate_same_interior(self):
        test = SenderFirstTest(self.routers[0].addresses[0],
                               self.routers[0].addresses[0],
                               'dest.02',
                               'dest.02.alt')
        test.run()
        self.assertEqual(None, test.error)

    def test_03_sender_first_primary_same_edge(self):
        test = SenderFirstTest(self.routers[2].addresses[0],
                               self.routers[2].addresses[0],
                               'dest.03',
                               'dest.03')
        test.run()
        self.assertEqual(None, test.error)

    def test_04_sender_first_alternate_same_edge(self):
        test = SenderFirstTest(self.routers[2].addresses[0],
                               self.routers[2].addresses[0],
                               'dest.04',
                               'dest.04.alt')
        test.run()
        self.assertEqual(None, test.error)

    def test_05_sender_first_primary_interior_interior(self):
        test = SenderFirstTest(self.routers[0].addresses[0],
                               self.routers[1].addresses[0],
                               'dest.05',
                               'dest.05')
        test.run()
        self.assertEqual(None, test.error)

    def test_06_sender_first_alternate_interior_interior(self):
        test = SenderFirstTest(self.routers[0].addresses[0],
                               self.routers[1].addresses[0],
                               'dest.06',
                               'dest.06.alt')
        test.run()
        self.assertEqual(None, test.error)

    def test_07_sender_first_primary_edge_interior(self):
        test = SenderFirstTest(self.routers[2].addresses[0],
                               self.routers[1].addresses[0],
                               'dest.07',
                               'dest.07')
        test.run()
        self.assertEqual(None, test.error)

    def test_08_sender_first_alternate_edge_interior(self):
        test = SenderFirstTest(self.routers[2].addresses[0],
                               self.routers[1].addresses[0],
                               'dest.08',
                               'dest.08.alt')
        test.run()
        self.assertEqual(None, test.error)

    def test_09_sender_first_primary_interior_edge(self):
        test = SenderFirstTest(self.routers[1].addresses[0],
                               self.routers[2].addresses[0],
                               'dest.09',
                               'dest.09')
        test.run()
        self.assertEqual(None, test.error)

    def test_10_sender_first_alternate_interior_edge(self):
        test = SenderFirstTest(self.routers[1].addresses[0],
                               self.routers[2].addresses[0],
                               'dest.10',
                               'dest.10.alt')
        test.run()
        self.assertEqual(None, test.error)

    def test_11_sender_first_primary_edge_edge(self):
        test = SenderFirstTest(self.routers[2].addresses[0],
                               self.routers[4].addresses[0],
                               'dest.11',
                               'dest.11')
        test.run()
        self.assertEqual(None, test.error)

    def test_12_sender_first_alternate_edge_edge(self):
        test = SenderFirstTest(self.routers[2].addresses[0],
                               self.routers[4].addresses[0],
                               'dest.12',
                               'dest.12.alt')
        test.run()
        self.assertEqual(None, test.error)


class Timeout(object):
    def __init__(self, parent):
        self.parent = parent

    def on_timer_task(self, event):
        self.parent.timeout()


class SenderFirstTest(MessagingHandler):
    def __init__(self, sender_host, receiver_host, send_addr, recv_addr):
        super(SenderFirstTest, self).__init__()
        self.sender_host   = sender_host
        self.receiver_host = receiver_host
        self.send_addr     = send_addr
        self.recv_addr     = recv_addr
        self.count         = 300

        self.sender_conn   = None
        self.receiver_conn = None
        self.error         = None
        self.n_tx          = 0
        self.n_rx          = 0
        self.n_rel         = 0

    def timeout(self):
        self.error = "Timeout Expired - n_tx=%d, n_rx=%d, n_rel=%d" % (self.n_tx, self.n_rx, self.n_rel)
        self.sender_conn.close()
        self.receiver_conn.close()

    def fail(self, error):
        self.error = error
        self.sender_conn.close()
        self.receiver_conn.close()
        self.timer.cancel()

    def on_start(self, event):
        self.timer         = event.reactor.schedule(10.0, Timeout(self))
        self.sender_conn   = event.container.connect(self.sender_host)
        self.receiver_conn = event.container.connect(self.receiver_host)
        self.sender        = event.container.create_sender(self.sender_conn, self.send_addr)

    def on_link_opened(self, event):
        if event.sender == self.sender:
            self.receiver = event.container.create_receiver(self.receiver_conn, self.recv_addr)

    def on_sendable(self, event):
        if event.sender == self.sender:
            while self.sender.credit > 0 and self.n_tx < self.count:
                self.sender.send(Message("Message %d" % self.n_tx))
                self.n_tx += 1

    def on_message(self, event):
        if event.receiver == self.receiver:
            self.n_rx += 1
            if self.n_rx == self.count:
                self.fail(None)

    def on_released(self, event):
        self.n_rel += 1

    def run(self):
        Container(self).run()


if __name__== '__main__':
    unittest.main(main_module())

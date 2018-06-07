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

from data import MessageHELLO
from qpid_dispatch_internal.dispatch import LOG_INFO, LOG_TRACE, LOG_CRITICAL

class HelloProtocol(object):
    """
    This module is responsible for running the HELLO protocol.
    """
    def __init__(self, container, node_tracker):
        self.container         = container
        self.node_tracker      = node_tracker
        self.id                = self.container.id
        self.ticks             = 0.0
        self.last_hello_ticks  = 0.0
        self.hello_interval    = container.config.helloIntervalSeconds
        self.hello_max_age     = container.config.helloMaxAgeSeconds
        self.mode              = container.mode
        self.mode_is_edge      = self.mode == 'edge'
        self.hellos            = {}
        self.chosen_uplink     = []     # list that will have zero or one items.
        self.dup_reported      = False
        self.mismatch_reported = False


    def tick(self, now):
        self._expire_hellos(now)
        self.ticks += 1.0
        if self.ticks - self.last_hello_ticks >= self.hello_interval:
            self.last_hello_ticks = self.ticks
            seen = self.hellos.keys() if not self.mode_is_edge else self.chosen_uplink
            msg = MessageHELLO(None, self.id, seen, self.container.instance, self.mode)
            self.container.send('amqp:/_local/qdhello', msg)
            self.container.log_hello(LOG_TRACE, "SENT: %r" % msg)


    def handle_hello(self, msg, now, link_id, link_maskbit, cost):
        if msg.id == self.id:
            if not self.dup_reported and (msg.instance != self.container.instance):
                self.dup_reported = True
                self.container.log_hello(LOG_CRITICAL, "Detected Neighbor Router with a Duplicate ID - %s" % msg.id)
            return
        peer_is_edge = msg.mode == 'edge'
        if self.mode_is_edge and peer_is_edge and not self.mismatch_reported:
            self.mismatch_reported = True
            self.container.log_hello(LOG_CRITICAL, "Illegal Edge-to-Edge Connection with ID - %s" % msg.id)
            return
        self.hellos[msg.id] = now
        if msg.is_seen(self.id):
            self.node_tracker.neighbor_refresh(msg.id, msg.version, msg.instance, peer_is_edge, link_id, link_maskbit, cost, now)


    def set_chosen_uplink(self, uplink_id):
        self.chosen_uplink = [uplink_id] if uplink_id else []


    def _expire_hellos(self, now):
        """
        Expire local records of received hellos.  This is not involved in the
        expiration of neighbor status for routers.
        """
        for key, last_seen in self.hellos.items():
            if now - last_seen > self.hello_max_age:
                self.hellos.pop(key)
                if key in self.chosen_uplink:
                    self.chosen_uplink = []
                self.container.log_hello(LOG_TRACE, "HELLO peer expired: %s" % key)




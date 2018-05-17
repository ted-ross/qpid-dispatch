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

from ..dispatch import LOG_INFO, LOG_TRACE, LOG_DEBUG
from data import ProtocolVersion
from .address import Address

class NodeTrackerEdge(object):
    """
    This module is the node tracker for an edge router.  It's responsibilities are very different
    from those of the interior router node tracker.

    This node tracker drives the edge-specific LS and MA protocols with the uplinked interior router
    and tracks the connectivity to the uplink.
    """
    def __init__(self, container, max_routers):
        self.container        = container
        self.my_id            = container.id
        self.max_routers      = max_routers
        self.neighbor_max_age = self.container.config.helloMaxAgeSeconds
        self.ls_max_age       = self.container.config.remoteLsMaxAgeSeconds
        self.uplink           = None
        self.container.router_adapter.get_agent().add_implementation(self, "router.node")


    def refresh_entity(self, attributes):
        """Refresh management attributes"""
        attributes.update({
            "id": self.my_id,
            "protocolVersion": ProtocolVersion,
            "instance": self.container.instance, # Boot number, integer
            "linkState": None,
            "nextHop":  self.uplink.id if self.uplink else None,
            "validOrigins": None,
            "address": Address.topological(self.my_id, area=self.container.area),
            "lastTopoChange" : None
        })


    def _do_expirations(self, now):
        """
        Run through the list of routers and check for expired conditions
        """
        ##
        ## Check the freshness of the uplink.  
        ##
        if now - self.uplink.neighbor_refresh_time > self.neighbor_max_age:
            pass

    def tick(self, now):
        send_ra = False
        if not self.uplink:
            return

        ##
        ## Expire neighbors and link state
        ##
        self._do_expirations(now)

        ##
        ## Send link-state requests and mobile-address requests to the nodes
        ## that have pending requests and are reachable
        ##
        node_id = self.uplink.id
        node    = self.uplink
        if node.mobile_address_requested():
            self.container.mobile_address_engine.send_mar(node_id, node.mobile_address_sequence)

        ##
        ## If local changes have been made to the list of mobile addresses, send
        ## an unsolicited mobile-address-update to all routers.
        ##
        mobile_seq = self.container.mobile_address_engine.tick(now)
        self.container.link_state_engine.set_mobile_seq(mobile_seq)


    def neighbor_refresh(self, node_id, version, instance, link_id, cost, now):
        """
        Invoked when the hello protocol has received positive confirmation
        of continued bi-directional connectivity with a neighbor router.
        """

        ##
        ## If the node id is not known, create a new RouterNode to track it.
        ##
        if node_id not in self.nodes:
            self.nodes[node_id] = RouterNode(self, node_id, version, instance)
        node = self.nodes[node_id]

        ##
        ## Add the version if we haven't already done so.
        ##
        if node.version == None:
            node.version = version

        ##
        ## Update the refresh time for later expiration checks
        ##
        node.neighbor_refresh_time = now

        ##
        ## If the instance was updated (i.e. the neighbor restarted suddenly),
        ## handle the mobile-address handling for a restarted neighbor.
        ##
        node.update_instance(instance, version)


    def link_lost(self, link_id):
        """
        Invoked when an inter-router link is dropped.
        """
        self.container.log_ls(LOG_INFO, "Link to Neighbor Router Lost - link_tag=%d" % link_id)
        pass


    def in_flux_mode(self, now):
        return False


    def ra_received(self, node_id, version, ls_seq, mobile_seq, instance, now):
        """
        Invoked when a router advertisement is received from another router.
        """
        ##
        ## If the node id is not known, create a new RouterNode to track it.
        ##
        if node_id not in self.nodes:
            self.nodes[node_id] = RouterNode(self, node_id, version, instance)
        node = self.nodes[node_id]

        ##
        ## Add the version if we haven't already done so.
        ##
        if node.version == None:
            node.version = version

        ##
        ## If the instance was updated (i.e. the router restarted suddenly),
        ## drop the mobile addresses and request new data.
        ##
        node.update_instance(instance, version)

        ##
        ## Update the last seen time to now to control expiration of the link state.
        ##
        node.link_state.last_seen = now

        ##
        ## Check the mobile sequence.  Send a mobile-address-request if we are
        ## behind the advertized sequence.
        ##
        if node.mobile_address_sequence < mobile_seq:
            node.mobile_address_request()


    def router_learned(self, node_id, version):
        """
        Should not be invoked on an edge router.
        """
        pass


    def link_state_received(self, node_id, version, link_state, instance, now):
        """
        Should not be invoked on an edge router.
        """
        pass


    def router_node(self, node_id):
        return self.nodes[node_id]


    def link_id_to_node_id(self, link_id):
        if link_id in self.nodes_by_link_id:
            return self.nodes_by_link_id[link_id].id
        return None


class RouterNode(object):
    """
    RouterNode is used to track remote routers in the router network.
    For a router in edge mode (this module), there is only one RouterNode
    and it represents the uplink interior router.
    """

    def __init__(self, parent, node_id, version, instance):
        self.parent                  = parent
        self.adapter                 = parent.container.router_adapter
        self.log                     = parent.container.log
        self.id                      = node_id
        self.version                 = version
        self.instance                = instance
        self.neighbor_refresh_time   = 0.0
        self.mobile_addresses        = []
        self.mobile_address_sequence = 0
        self.need_mobile_request     = False
        self.adapter.get_agent().add_implementation(self, "router.node")

    def refresh_entity(self, attributes):
        """Refresh management attributes"""
        attributes.update({
            "id": self.id,
            "protocolVersion": self.version,
            "instance": self.instance, # Boot number, integer
            "linkState": None,
            "nextHop":  None,
            "validOrigins": None,
            "address": Address.topological(self.id, area=self.parent.container.area),
            "routerLink": None,
            "cost": None
        })

    def _logify(self, addr):
        cls   = addr[0]
        phase = None
        if cls == 'M':
            phase = addr[1]
            return "%s;class=%c;phase=%c" % (addr[2:], cls, phase)
        return "%s;class=%c" % (addr[1:], cls)


    def delete(self):
        self.adapter.get_agent().remove_implementation(self)
        self.unmap_all_addresses()
        self.adapter.del_router(self.maskbit)
        self.parent._free_maskbit(self.maskbit)
        self.log(LOG_TRACE, "Node %s deleted" % self.id)


    def set_next_hop(self, next_hop):
        pass


    def set_valid_origins(self, valid_origins):
        pass


    def set_cost(self, cost):
        pass


    def is_neighbor(self):
        return self.peer_link_id != None


    def mobile_address_request(self):
        self.need_mobile_request = True


    def mobile_address_requested(self):
        if self.need_mobile_request and (self.peer_link_id != None or self.next_hop_router != None):
            self.need_mobile_request = False
            return True
        return False


    def map_address(self, addr):
        self.mobile_addresses.append(addr)
        self.adapter.map_destination(addr, self.maskbit)
        self.log(LOG_DEBUG, "Remote destination %s mapped to router %s" % (self._logify(addr), self.id))


    def unmap_address(self, addr):
        self.mobile_addresses.remove(addr)
        self.adapter.unmap_destination(addr, self.maskbit)
        self.log(LOG_DEBUG, "Remote destination %s unmapped from router %s" % (self._logify(addr), self.id))


    def unmap_all_addresses(self):
        self.mobile_address_sequence = 0
        while self.mobile_addresses:
            self.unmap_address(self.mobile_addresses[0])


    def overwrite_addresses(self, addrs):
        added   = []
        deleted = []
        for a in addrs:
            if a not in self.mobile_addresses:
                added.append(a)
        for a in self.mobile_addresses:
            if a not in addrs:
                deleted.append(a)
        for a in added:
            self.map_address(a)
        for a in deleted:
            self.unmap_address(a)


    def update_instance(self, instance, version):
        if instance == None:
            return False
        if self.instance == None:
            self.instance = instance
            return False
        if self.instance == instance:
            return False

        self.instance = instance
        self.version  = version
        self.link_state.del_all_peers()
        self.unmap_all_addresses()
        self.log(LOG_INFO, "Detected Restart of Router Node %s" % self.id)
        if self.peer_link_id == -1:
            return False
        return True


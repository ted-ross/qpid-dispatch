/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

#include "core_link_endpoint.h"
#include "qpid/dispatch/alloc.h"
#include <stdio.h>

struct qdrc_endpoint_t {
    qdrc_endpoint_desc_t *desc;
    void                 *link_context;
    qdr_link_t           *link;
};

ALLOC_DECLARE(qdrc_endpoint_t);
ALLOC_DEFINE(qdrc_endpoint_t);

void qdrc_endpoint_bind_mobile_address_CT(qdr_core_t           *core,
                                          const char           *address,
                                          char                  phase,
                                          qdrc_endpoint_desc_t *desc,
                                          void                 *bind_context)
{
    qdr_address_t *addr = 0;
    qd_iterator_t *iter = qd_iterator_string(address, ITER_VIEW_ADDRESS_HASH);
    qd_iterator_annotate_phase(iter, phase);

    qd_hash_retrieve(core->addr_hash, iter, (void*) &addr);
    if (!addr) {
        qd_address_treatment_t treatment = qdr_treatment_for_address_CT(core, 0, iter, 0, 0);
        if (treatment == QD_TREATMENT_UNAVAILABLE)
            treatment = QD_TREATMENT_ANYCAST_BALANCED;
        addr = qdr_address_CT(core, treatment);
        DEQ_INSERT_TAIL(core->addrs, addr);
        qd_hash_insert(core->addr_hash, iter, addr, &addr->hash_handle);
    }

    assert(addr->core_endpoint == 0);
    addr->core_endpoint         = desc;
    addr->core_endpoint_context = bind_context;

    qd_iterator_free(iter);
}


qdrc_endpoint_t *qdrc_endpoint_create_link_CT(qdr_core_t           *core,
                                              qdr_connection_t     *conn,
                                              qd_direction_t        dir,
                                              qdr_terminus_t       *source,
                                              qdr_terminus_t       *target,
                                              qdrc_endpoint_desc_t *desc,
                                              void                 *link_context)
{
    qdrc_endpoint_t *ep = new_qdrc_endpoint_t();

    ep->desc         = desc;
    ep->link_context = link_context;
    ep->link         = qdr_create_link_CT(core, conn, QD_LINK_ENDPOINT, dir, source, target);

    ep->link->core_endpoint = ep;
    return ep;
}


qd_direction_t qdrc_endpoint_get_direction_CT(const qdrc_endpoint_t *ep)
{
    return ep->link->link_direction;
}


qdr_connection_t *qdrc_endpoint_get_connection_CT(qdrc_endpoint_t *ep)
{
    return !!ep ? (!!ep->link ? ep->link->conn : 0) : 0;
}


void qdrc_endpoint_flow_CT(qdr_core_t *core, qdrc_endpoint_t *ep, int credit, bool drain)
{
    qdr_link_issue_credit_CT(core, ep->link, credit, drain);
}


void qdrc_endpoint_send_CT(qdr_core_t *core, qdrc_endpoint_t *ep, qdr_delivery_t *dlv, bool presettled)
{
    uint64_t *tag = (uint64_t*) dlv->tag;

    dlv->link          = ep->link;
    dlv->settled       = presettled;
    dlv->presettled    = presettled;
    *tag               = core->next_tag++;
    dlv->tag_length    = 8;
    dlv->error         = 0;
    dlv->ingress_index = -1;
    
    qdr_forward_deliver_CT(core, ep->link, dlv);
}


qdr_delivery_t *qdrc_endpoint_delivery_CT(qdr_core_t *core, qdrc_endpoint_t *endpoint, qd_message_t *message)
{
    qdr_delivery_t *dlv = new_qdr_delivery_t();
    uint64_t       *tag = (uint64_t*) dlv->tag;

    ZERO(dlv);
    dlv->link           = endpoint->link;
    dlv->msg            = message;
    *tag                = core->next_tag++;
    dlv->tag_length = 8;
    dlv->ingress_index = -1;
    return dlv;
}


void qdrc_endpoint_settle_CT(qdr_core_t *core, qdr_delivery_t *dlv, uint64_t disposition)
{
    //
    // Set the new delivery state
    //
    dlv->disposition = disposition;
    dlv->settled     = true;

    //
    // Activate the connection to push this update out
    //
    qdr_delivery_push_CT(core, dlv);

    //
    // Remove the endpoint's reference
    //
    qdr_delivery_decref_CT(core, dlv, "qdrc_endpoint_settle_CT - no longer held by endpoint");
}


void qdrc_endpoint_detach_CT(qdr_core_t *core, qdrc_endpoint_t *ep, qdr_error_t *error)
{
    qdr_link_outbound_detach_CT(core, ep->link, error, QDR_CONDITION_NONE, true);
    if (ep->link->detach_count == 2) {
        ep->link->core_endpoint = 0;
        free_qdrc_endpoint_t(ep);
    }
}


bool qdrc_endpoint_do_bound_attach_CT(qdr_core_t *core, qdr_address_t *addr, qdr_link_t *link, qdr_error_t **error)
{
    qdrc_endpoint_t *ep = new_qdrc_endpoint_t();
    ZERO(ep);
    ep->desc = addr->core_endpoint;
    ep->link = link;

    link->core_endpoint = ep;

    *error = 0;
    bool accept = !!ep->desc->on_first_attach ?
        ep->desc->on_first_attach(addr->core_endpoint_context, ep, &ep->link_context, error) : false;

    if (!accept) {
        link->core_endpoint = 0;
        free_qdrc_endpoint_t(ep);
    }

    return accept;
}



void qdrc_endpoint_do_deliver_CT(qdr_core_t *core, qdrc_endpoint_t *ep, qdr_delivery_t *dlv)
{
    ep->desc->on_transfer(ep->link_context, dlv, dlv->msg);
}


void qdrc_endpoint_do_flow_CT(qdr_core_t *core, qdrc_endpoint_t *ep, int credit, bool drain)
{
    ep->desc->on_flow(ep->link_context, credit, drain);
}


void qdrc_endpoint_do_detach_CT(qdr_core_t *core, qdrc_endpoint_t *ep, qdr_error_t *error)
{
    ep->desc->on_detach(ep->link_context, error);
    if (ep->link->detach_count == 2) {
        ep->link->core_endpoint = 0;
        free_qdrc_endpoint_t(ep);
    }
}


void qdrc_endpoint_do_cleanup_CT(qdr_core_t *core, qdrc_endpoint_t *ep)
{
    ep->desc->on_cleanup(ep->link_context);
    free_qdrc_endpoint_t(ep);
}



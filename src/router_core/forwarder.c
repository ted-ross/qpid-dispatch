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

#include "router_core_private.h"
#include <qpid/dispatch/amqp.h>
#include <stdio.h>
#include <strings.h>

//
// NOTE: If the in_delivery argument is NULL, the resulting out deliveries
//       shall be pre-settled.
//
typedef int (*qdr_forward_message_t) (qdr_core_t      *core,
                                      qdr_address_t   *addr,
                                      qd_message_t    *msg,
                                      qdr_delivery_t  *in_delivery,
                                      bool             exclude_inprocess,
                                      bool             control);

typedef bool (*qdr_forward_attach_t) (qdr_core_t     *core,
                                      qdr_address_t  *addr,
                                      qdr_link_t     *link,
                                      qdr_terminus_t *source,
                                      qdr_terminus_t *target);

struct qdr_forwarder_t {
    qdr_forward_message_t forward_message;
    qdr_forward_attach_t  forward_attach;
    bool                  bypass_valid_origins;
};

//==================================================================================
// Built-in Forwarders
//==================================================================================

static int qdr_forward_message_null_CT(qdr_core_t      *core,
                                       qdr_address_t   *addr,
                                       qd_message_t    *msg,
                                       qdr_delivery_t  *in_delivery,
                                       bool             exclude_inprocess,
                                       bool             control)
{
    qd_log(core->log, QD_LOG_CRITICAL, "NULL Message Forwarder Invoked");
    return 0;
}


static bool qdr_forward_attach_null_CT(qdr_core_t     *core,
                                       qdr_address_t  *addr,
                                       qdr_link_t     *link,
                                       qdr_terminus_t *source,
                                       qdr_terminus_t *target)
{
    qd_log(core->log, QD_LOG_CRITICAL, "NULL Attach Forwarder Invoked");
    return false;
}


static void qdr_forward_find_closest_remotes_CT(qdr_core_t *core, qdr_address_t *addr)
{
    qdr_node_t *rnode       = DEQ_HEAD(core->routers);
    int         lowest_cost = 0;

    if (!addr->closest_remotes)
        addr->closest_remotes = qd_bitmask(0);
    addr->cost_epoch  = core->cost_epoch;
    addr->next_remote = -1;

    qd_bitmask_clear_all(addr->closest_remotes);
    while (rnode) {
        if (qd_bitmask_value(addr->rnodes, rnode->mask_bit)) {
            if (lowest_cost == 0) {
                lowest_cost = rnode->cost;
                addr->next_remote = rnode->mask_bit;
            }
            if (lowest_cost == rnode->cost)
                qd_bitmask_set_bit(addr->closest_remotes, rnode->mask_bit);
            else
                break;
        }
        rnode = DEQ_NEXT(rnode);
    }
}


qdr_delivery_t *qdr_forward_new_delivery_CT(qdr_core_t *core, qdr_delivery_t *in_dlv, qdr_link_t *link, qd_message_t *msg)
{
    qdr_delivery_t *out_dlv = new_qdr_delivery_t();
    uint64_t       *tag = (uint64_t*) out_dlv->tag;

    ZERO(out_dlv);
    out_dlv->link       = link;
    out_dlv->msg        = qd_message_copy(msg);
    out_dlv->settled    = !in_dlv || in_dlv->settled;
    out_dlv->presettled = out_dlv->settled;
    *tag                = core->next_tag++;
    out_dlv->tag_length = 8;
    out_dlv->error      = 0;

    //
    // Add one to the message fanout. This will later be used in the qd_message_send function that sends out messages.
    //
    qd_message_add_fanout(msg);

    //
    // Create peer linkage if the outgoing delivery is unsettled. This peer linkage is necessary to deal with dispositions that show up in the future.
    // Also create peer linkage if the message is not yet been completely received. This linkage will help us stream large pre-settled multicast messages.
    //
    if (!out_dlv->settled || !qd_message_receive_complete(msg))
        qdr_delivery_link_peers_CT(in_dlv, out_dlv);

    return out_dlv;
}


//
// Drop all pre-settled deliveries pending on the link's
// undelivered list.
//
static void qdr_forward_drop_presettled_CT_LH(qdr_core_t *core, qdr_link_t *link)
{
    qdr_delivery_t *dlv = DEQ_HEAD(link->undelivered);
    qdr_delivery_t *next;

    while (dlv) {
        next = DEQ_NEXT(dlv);
        //
        // Remove pre-settled deliveries unless they are in a link_work
        // record that is being processed.  If it's being processed, it is
        // too late to drop the delivery.
        //
        if (dlv->settled && dlv->link_work && !dlv->link_work->processing) {
            DEQ_REMOVE(link->undelivered, dlv);
            dlv->where = QDR_DELIVERY_NOWHERE;

            //
            // The link-work item representing this pending delivery must be
            // updated to reflect the removal of the delivery.  If the item
            // has no other deliveries associated with it, it can be removed
            // from the work list.
            //
            assert(dlv->link_work);
            if (dlv->link_work && (--dlv->link_work->value == 0)) {
                DEQ_REMOVE(link->work_list, dlv->link_work);
                free_qdr_link_work_t(dlv->link_work);
                dlv->link_work = 0;
            }
            qdr_delivery_decref_CT(core, dlv, "qdr_forward_drop_presettled_CT_LH - remove from link-work list");
        }
        dlv = next;
    }
}


void qdr_forward_deliver_CT(qdr_core_t *core, qdr_link_t *out_link, qdr_delivery_t *out_dlv)
{
    sys_mutex_lock(out_link->conn->work_lock);

    //
    // If the delivery is pre-settled and the outbound link is at or above capacity,
    // discard all pre-settled deliveries on the undelivered list prior to enqueuing
    // the new delivery.
    //
    if (out_dlv->settled && out_link->capacity > 0 && DEQ_SIZE(out_link->undelivered) >= out_link->capacity)
        qdr_forward_drop_presettled_CT_LH(core, out_link);

    DEQ_INSERT_TAIL(out_link->undelivered, out_dlv);
    out_dlv->where = QDR_DELIVERY_IN_UNDELIVERED;

    // This incref is for putting the delivery in the undelivered list
    qdr_delivery_incref(out_dlv, "qdr_forward_deliver_CT - add to undelivered list");

    //
    // We must put a work item on the link's work list to represent this pending delivery.
    // If there's already a delivery item on the tail of the work list, simply join that item
    // by incrementing the value.
    //
    qdr_link_work_t *work = DEQ_TAIL(out_link->work_list);
    if (work && work->work_type == QDR_LINK_WORK_DELIVERY) {
        work->value++;
    } else {
        work = new_qdr_link_work_t();
        ZERO(work);
        work->work_type = QDR_LINK_WORK_DELIVERY;
        work->value     = 1;
        DEQ_INSERT_TAIL(out_link->work_list, work);
    }
    qdr_add_link_ref(&out_link->conn->links_with_work, out_link, QDR_LINK_LIST_CLASS_WORK);
    out_dlv->link_work = work;
    sys_mutex_unlock(out_link->conn->work_lock);

    //
    // Activate the outgoing connection for later processing.
    //
    qdr_connection_activate_CT(core, out_link->conn);
}


void qdr_forward_on_message(qdr_core_t *core, qdr_general_work_t *work)
{
    work->on_message(work->on_message_context, work->msg, work->maskbit, work->inter_router_cost);
    qd_message_free(work->msg);
}


void qdr_forward_on_message_CT(qdr_core_t *core, qdr_subscription_t *sub, qdr_link_t *link, qd_message_t *msg)
{
    qdr_general_work_t *work = qdr_general_work(qdr_forward_on_message);
    work->on_message         = sub->on_message;
    work->on_message_context = sub->on_message_context;
    work->msg                = qd_message_copy(msg);
    work->maskbit            = link ? link->conn->mask_bit : 0;
    work->inter_router_cost  = link ? link->conn->inter_router_cost : 1;
    qdr_post_general_work_CT(core, work);
}


int qdr_forward_multicast_CT(qdr_core_t      *core,
                             qdr_address_t   *addr,
                             qd_message_t    *msg,
                             qdr_delivery_t  *in_delivery,
                             bool             exclude_inprocess,
                             bool             control)
{
    bool          bypass_valid_origins = addr->forwarder->bypass_valid_origins;
    int           fanout               = 0;
    qd_bitmask_t *link_exclusion       = !!in_delivery ? in_delivery->link_exclusion : 0;
    bool          presettled           = !!in_delivery ? in_delivery->settled : true;
    bool          receive_complete     = qd_message_receive_complete(qdr_delivery_message(in_delivery));

    //
    // If the delivery is not presettled, set the settled flag for forwarding so all
    // outgoing deliveries will be presettled.
    //
    // NOTE:  This is the only multicast mode currently supported.  Others will likely be
    //        implemented in the future.
    //
    if (!presettled) {
        in_delivery->settled = true;
        //
        // If the router is configured to reject unsettled multicasts, settle and reject this delivery.
        //
        if (!core->qd->allow_unsettled_multicast) {
            in_delivery->disposition = PN_REJECTED;
            in_delivery->error = qdr_error("qd:forbidden", "Deliveries to a multicast address must be pre-settled");
            qdr_delivery_push_CT(core, in_delivery);
            return 0;
        }
    }

    //
    // Forward to local subscribers
    //
    if (!addr->local || exclude_inprocess) {
        qdr_link_ref_t *link_ref = DEQ_HEAD(addr->rlinks);
        while (link_ref) {
            qdr_link_t     *out_link     = link_ref->link;
            qdr_delivery_t *out_delivery = qdr_forward_new_delivery_CT(core, in_delivery, out_link, msg);
            qdr_forward_deliver_CT(core, out_link, out_delivery);
            fanout++;
            if (out_link->link_type != QD_LINK_CONTROL && out_link->link_type != QD_LINK_ROUTER)
                addr->deliveries_egress++;
            link_ref = DEQ_NEXT(link_ref);
        }
    }

    //
    // Forward to remote routers with subscribers using the appropriate
    // link for the traffic class: control or data
    //
    //
    // Get the mask bit associated with the ingress router for the message.
    // This will be compared against the "valid_origin" masks for each
    // candidate destination router.
    //
    int origin = -1;
    qd_iterator_t *ingress_iter = in_delivery ? in_delivery->origin : 0;

    if (ingress_iter && !bypass_valid_origins) {
        qd_iterator_reset_view(ingress_iter, ITER_VIEW_NODE_HASH);
        qdr_address_t *origin_addr;
        qd_hash_retrieve(core->addr_hash, ingress_iter, (void*) &origin_addr);
        if (origin_addr && qd_bitmask_cardinality(origin_addr->rnodes) == 1)
            qd_bitmask_first_set(origin_addr->rnodes, &origin);
    } else
        origin = 0;

    //
    // Forward to the next-hops for remote destinations.
    //
    if (origin >= 0) {
        int           dest_bit;
        qdr_link_t   *dest_link;
        qdr_node_t   *next_node;
        qd_bitmask_t *link_set = qd_bitmask(0);

        //
        // Loop over the target nodes for this address.  Build a set of outgoing links
        // for which there are valid targets.  We do this to avoid sending more than one
        // message down a given link.  It's possible that there are multiple destinations
        // for this address that are all reachable over the same link.  In this case, we
        // will send only one copy of the message over the link and allow a downstream
        // router to fan the message out.
        //
        int c;
        for (QD_BITMASK_EACH(addr->rnodes, dest_bit, c)) {
            qdr_node_t *rnode = core->routers_by_mask_bit[dest_bit];
            if (!rnode)
                continue;

            if (rnode->next_hop)
                next_node = rnode->next_hop;
            else
                next_node = rnode;

            dest_link = control ? PEER_CONTROL_LINK(core, next_node) : PEER_DATA_LINK(core, next_node);
            if (dest_link && qd_bitmask_value(rnode->valid_origins, origin))
                qd_bitmask_set_bit(link_set, dest_link->conn->mask_bit);
        }

        //
        // Send a copy of the message outbound on each identified link.
        //
        int link_bit;
        while (qd_bitmask_first_set(link_set, &link_bit)) {
            qd_bitmask_clear_bit(link_set, link_bit);
            dest_link = control ?
                core->control_links_by_mask_bit[link_bit] :
                core->data_links_by_mask_bit[link_bit];
            if (dest_link && (!link_exclusion || qd_bitmask_value(link_exclusion, link_bit) == 0)) {
                qdr_delivery_t *out_delivery = qdr_forward_new_delivery_CT(core, in_delivery, dest_link, msg);
                qdr_forward_deliver_CT(core, dest_link, out_delivery);
                fanout++;
                addr->deliveries_transit++;
            }
        }

        qd_bitmask_free(link_set);
    }

    if (!exclude_inprocess) {
        //
        // Forward to in-process subscribers
        //

        qdr_subscription_t *sub = DEQ_HEAD(addr->subscriptions);
        while (sub) {
            //
            // Only if the message has been completely received, forward it to the subscription
            // Subscriptions, at the moment, dont have the ability to deal with partial messages
            //
            if (receive_complete)
                qdr_forward_on_message_CT(core, sub, in_delivery ? in_delivery->link : 0, msg);
            else
                //
                // Receive is not complete, we will store the sub in in_delivery->subscriptions so we can send the message to the subscription
                // after the message fully arrives
                //
                DEQ_INSERT_TAIL(in_delivery->subscriptions, sub);


            fanout++;
            addr->deliveries_to_container++;
            sub = DEQ_NEXT(sub);
        }
    }

    if (in_delivery && !presettled) {
        if (fanout == 0)
            //
            // The delivery was not presettled and it was not forwarded to any
            // destinations, return it to its original unsettled state.
            //
            in_delivery->settled = false;
        else {
            //
            // The delivery was not presettled and it was forwarded to at least
            // one destination.  Accept and settle the delivery only if the entire delivery
            // has been received.
            //
            if (receive_complete) {
                in_delivery->disposition = PN_ACCEPTED;
                qdr_delivery_push_CT(core, in_delivery);
            }
        }
    }

    return fanout;
}


int qdr_forward_closest_CT(qdr_core_t      *core,
                           qdr_address_t   *addr,
                           qd_message_t    *msg,
                           qdr_delivery_t  *in_delivery,
                           bool             exclude_inprocess,
                           bool             control)
{
    qdr_link_t     *out_link;
    qdr_delivery_t *out_delivery;

    //
    // Forward to an in-process subscriber if there is one.
    //
    if (!exclude_inprocess) {
        bool receive_complete = qd_message_receive_complete(msg);
        qdr_subscription_t *sub = DEQ_HEAD(addr->subscriptions);
        if (sub) {

            //
            // Only if the message has been completely received, forward it.
            // Subscriptions, at the moment, dont have the ability to deal with partial messages
            //
            if (receive_complete)
                qdr_forward_on_message_CT(core, sub, in_delivery ? in_delivery->link : 0, msg);
            else
                //
                // Receive is not complete, we will store the sub in in_delivery->subscriptions so we can send the message to the subscription
                // after the message fully arrives
                //
                DEQ_INSERT_TAIL(in_delivery->subscriptions, sub);

            //
            // If the incoming delivery is not settled, it should be accepted and settled here.
            //
            if (in_delivery && !in_delivery->settled) {
                in_delivery->disposition = PN_ACCEPTED;
                in_delivery->settled     = true;
                qdr_delivery_push_CT(core, in_delivery);
            }

            //
            // Rotate this subscription to the end of the list to get round-robin distribution.
            //
            if (DEQ_SIZE(addr->subscriptions) > 1) {
                DEQ_REMOVE_HEAD(addr->subscriptions);
                DEQ_INSERT_TAIL(addr->subscriptions, sub);
            }

            addr->deliveries_to_container++;
            return 1;
        }
    }

    //
    // Forward to a local subscriber.
    //
    qdr_link_ref_t *link_ref = DEQ_HEAD(addr->rlinks);
    if (link_ref) {
        out_link     = link_ref->link;
        out_delivery = qdr_forward_new_delivery_CT(core, in_delivery, out_link, msg);
        qdr_forward_deliver_CT(core, out_link, out_delivery);

        //
        // If there are multiple local subscribers, rotate the list of link references
        // so deliveries will be distributed among the subscribers in a round-robin pattern.
        //
        if (DEQ_SIZE(addr->rlinks) > 1) {
            DEQ_REMOVE_HEAD(addr->rlinks);
            DEQ_INSERT_TAIL(addr->rlinks, link_ref);
        }

        addr->deliveries_egress++;
        return 1;
    }

    //
    // If the cached list of closest remotes is stale (i.e. cost data has changed),
    // recompute the closest remote routers.
    //
    if (addr->cost_epoch != core->cost_epoch)
        qdr_forward_find_closest_remotes_CT(core, addr);

    //
    // Forward to remote routers with subscribers using the appropriate
    // link for the traffic class: control or data
    //
    qdr_node_t *next_node;

    if (addr->next_remote >= 0) {
        qdr_node_t *rnode = core->routers_by_mask_bit[addr->next_remote];
        if (rnode) {
            _qdbm_next(addr->closest_remotes, &addr->next_remote);
            if (addr->next_remote == -1)
                qd_bitmask_first_set(addr->closest_remotes, &addr->next_remote);

            if (rnode->next_hop)
                next_node = rnode->next_hop;
            else
                next_node = rnode;

            out_link = control ? PEER_CONTROL_LINK(core, next_node) : PEER_DATA_LINK(core, next_node);
            if (out_link) {
                out_delivery = qdr_forward_new_delivery_CT(core, in_delivery, out_link, msg);
                qdr_forward_deliver_CT(core, out_link, out_delivery);
                addr->deliveries_transit++;
                return 1;
            }
        }
    }

    return 0;
}


int qdr_forward_balanced_CT(qdr_core_t      *core,
                            qdr_address_t   *addr,
                            qd_message_t    *msg,
                            qdr_delivery_t  *in_delivery,
                            bool             exclude_inprocess,
                            bool             control)
{
    //
    // Control messages should never use balanced treatment.
    //
    assert(!control);

    //
    // If this is the first time through here, allocate the array for outstanding delivery counts.
    //
    if (addr->outstanding_deliveries == 0) {
        addr->outstanding_deliveries = NEW_ARRAY(int, qd_bitmask_width());
        for (int i = 0; i < qd_bitmask_width(); i++)
            addr->outstanding_deliveries[i] = 0;
    }

    qdr_link_t *best_eligible_link       = 0;
    int         best_eligible_link_bit   = -1;
    uint32_t    eligible_link_value      = UINT32_MAX;
    qdr_link_t *best_ineligible_link     = 0;
    int         best_ineligible_link_bit = -1;
    uint32_t    ineligible_link_value    = UINT32_MAX;

    //
    // Find all the possible outbound links for this delivery, searching for the one with the
    // smallest eligible value.  Value = outstanding_deliveries + minimum_downrange_cost.
    // A link is ineligible if the outstanding_deliveries is equal to or greater than the
    // link's capacity.
    //
    // If there are no eligible links, use the best ineligible link.  Zero fanout should be returned
    // only if there are no available destinations.
    //

    //
    // Start with the local links
    //
    qdr_link_ref_t *link_ref = DEQ_HEAD(addr->rlinks);
    while (link_ref) {
        qdr_link_t *link     = link_ref->link;
        uint32_t    value    = DEQ_SIZE(link->undelivered) + DEQ_SIZE(link->unsettled);
        bool        eligible = link->capacity > value;

        //
        // If this is the best eligible link so far, record the fact.
        // Otherwise, if this is the best ineligible link, make note of that.
        //
        if (eligible && eligible_link_value > value) {
            best_eligible_link  = link;
            eligible_link_value = value;
        } else if (!eligible && ineligible_link_value > value) {
            best_ineligible_link  = link;
            ineligible_link_value = value;
        }

        link_ref = DEQ_NEXT(link_ref);
    }

    //
    // If we haven't already found a link with zero (best possible) value, check the
    // inter-router links as well.
    //
    if (!best_eligible_link || eligible_link_value > 0) {
        //
        // Get the mask bit associated with the ingress router for the message.
        // This will be compared against the "valid_origin" masks for each
        // candidate destination router.
        //
        int origin = 0;
        qd_iterator_t *ingress_iter = in_delivery ? in_delivery->origin : 0;

        if (ingress_iter) {
            qd_iterator_reset_view(ingress_iter, ITER_VIEW_NODE_HASH);
            qdr_address_t *origin_addr;
            qd_hash_retrieve(core->addr_hash, ingress_iter, (void*) &origin_addr);
            if (origin_addr && qd_bitmask_cardinality(origin_addr->rnodes) == 1)
                qd_bitmask_first_set(origin_addr->rnodes, &origin);
        }

        int c;
        int node_bit;
        for (QD_BITMASK_EACH(addr->rnodes, node_bit, c)) {
            qdr_node_t *rnode     = core->routers_by_mask_bit[node_bit];
            qdr_node_t *next_node = rnode->next_hop ? rnode->next_hop : rnode;
            qdr_link_t *link      = PEER_DATA_LINK(core, next_node);
            if (!link) continue;
            int         link_bit  = link->conn->mask_bit;
            int         value     = addr->outstanding_deliveries[link_bit];
            bool        eligible  = link->capacity > value;

            if (qd_bitmask_value(rnode->valid_origins, origin)) {
                //
                // Link is a candidate, adjust the value by the bias (node cost).
                //
                value += rnode->cost;
                if (eligible && eligible_link_value > value) {
                    best_eligible_link     = link;
                    best_eligible_link_bit = link_bit;
                    eligible_link_value    = value;
                } else if (!eligible && ineligible_link_value > value) {
                    best_ineligible_link     = link;
                    best_ineligible_link_bit = link_bit;
                    ineligible_link_value    = value;
                }
            }
        }
    } else if (best_eligible_link) {
        //
        // Rotate the rlinks list to enhance the appearance of balance when there is
        // little load (see DISPATCH-367)
        //
        if (DEQ_SIZE(addr->rlinks) > 1) {
            link_ref = DEQ_HEAD(addr->rlinks);
            DEQ_REMOVE_HEAD(addr->rlinks);
            DEQ_INSERT_TAIL(addr->rlinks, link_ref);
        }
    }

    qdr_link_t *chosen_link     = 0;
    int         chosen_link_bit = -1;

    if (best_eligible_link) {
        chosen_link     = best_eligible_link;
        chosen_link_bit = best_eligible_link_bit;
    } else if (best_ineligible_link) {
        chosen_link     = best_ineligible_link;
        chosen_link_bit = best_ineligible_link_bit;
    }

    if (chosen_link) {
        qdr_delivery_t *out_delivery = qdr_forward_new_delivery_CT(core, in_delivery, chosen_link, msg);
        qdr_forward_deliver_CT(core, chosen_link, out_delivery);

        //
        // If the delivery is unsettled and the link is inter-router, account for the outstanding delivery.
        //
        if (in_delivery && !in_delivery->settled && chosen_link_bit >= 0) {
            addr->outstanding_deliveries[chosen_link_bit]++;
            out_delivery->tracking_addr     = addr;
            out_delivery->tracking_addr_bit = chosen_link_bit;
            addr->tracked_deliveries++;
        }

        //
        // Bump the appropriate counter based on where we sent the delivery.
        //
        if (chosen_link_bit >= 0)
            addr->deliveries_transit++;
        else
            addr->deliveries_egress++;
        return 1;
    }

    return 0;
}


bool qdr_forward_link_balanced_CT(qdr_core_t     *core,
                                  qdr_address_t  *addr,
                                  qdr_link_t     *in_link,
                                  qdr_terminus_t *source,
                                  qdr_terminus_t *target)
{
    qdr_connection_ref_t *conn_ref = DEQ_HEAD(addr->conns);
    qdr_connection_t     *conn     = 0;

    //
    // Check for locally connected containers that can handle this link attach.
    //
    if (conn_ref) {
        conn = conn_ref->conn;

        //
        // If there are more than one local connections available for handling this link,
        // rotate the list so the attaches are balanced across the containers.
        //
        if (DEQ_SIZE(addr->conns) > 1) {
            DEQ_REMOVE_HEAD(addr->conns);
            DEQ_INSERT_TAIL(addr->conns, conn_ref);
        }
    } else {
        //
        // Look for a next-hop we can use to forward the link-attach.
        //
        qdr_node_t *next_node;

        if (addr->cost_epoch != core->cost_epoch) {
            addr->next_remote = -1;
            addr->cost_epoch  = core->cost_epoch;
        }

        if (addr->next_remote < 0) {
            qd_bitmask_first_set(addr->rnodes, &addr->next_remote);
        }

        if (addr->next_remote >= 0) {

            qdr_node_t *rnode = core->routers_by_mask_bit[addr->next_remote];

            if (rnode) {
                //
                // Advance the addr->next_remote so there will be link balance across containers
                //
                _qdbm_next(addr->rnodes, &addr->next_remote);
                if (addr->next_remote == -1)
                    qd_bitmask_first_set(addr->rnodes, &addr->next_remote);

                if (rnode->next_hop)
                    next_node = rnode->next_hop;
                else
                    next_node = rnode;

                if (next_node && PEER_DATA_LINK(core, next_node))
                    conn = PEER_DATA_LINK(core, next_node)->conn;
            }
        }
    }

    if (conn) {
        qdr_link_t *out_link = new_qdr_link_t();
        ZERO(out_link);
        out_link->core           = core;
        out_link->identity       = qdr_identifier(core);
        out_link->conn           = conn;
        out_link->link_type      = QD_LINK_ENDPOINT;
        out_link->link_direction = qdr_link_direction(in_link) == QD_OUTGOING ? QD_INCOMING : QD_OUTGOING;
        out_link->admin_enabled  = true;
        out_link->terminus_addr  = 0;

        out_link->oper_status    = QDR_LINK_OPER_DOWN;

        out_link->name = (char*) malloc(strlen(in_link->name) + 1);
        strcpy(out_link->name, in_link->name);

        out_link->connected_link = in_link;
        in_link->connected_link  = out_link;

        DEQ_INSERT_TAIL(core->open_links, out_link);
        qdr_add_link_ref(&conn->links, out_link, QDR_LINK_LIST_CLASS_CONNECTION);

        qdr_connection_work_t *work = new_qdr_connection_work_t();
        ZERO(work);
        work->work_type = QDR_CONNECTION_WORK_FIRST_ATTACH;
        work->link      = out_link;
        work->source    = source;
        work->target    = target;

        qdr_connection_enqueue_work_CT(core, conn, work);

        return true;
    }

    return false;
}


//==================================================================================
// In-Thread API Functions
//==================================================================================

qdr_forwarder_t *qdr_new_forwarder(qdr_forward_message_t fm, qdr_forward_attach_t fa, bool bypass_valid_origins)
{
    qdr_forwarder_t *forw = NEW(qdr_forwarder_t);

    forw->forward_message      = fm ? fm : qdr_forward_message_null_CT;
    forw->forward_attach       = fa ? fa : qdr_forward_attach_null_CT;
    forw->bypass_valid_origins = bypass_valid_origins;

    return forw;
}


void qdr_forwarder_setup_CT(qdr_core_t *core)
{
    //
    // Create message forwarders
    //
    core->forwarders[QD_TREATMENT_MULTICAST_FLOOD]  = qdr_new_forwarder(qdr_forward_multicast_CT, 0, true);
    core->forwarders[QD_TREATMENT_MULTICAST_ONCE]   = qdr_new_forwarder(qdr_forward_multicast_CT, 0, false);
    core->forwarders[QD_TREATMENT_ANYCAST_CLOSEST]  = qdr_new_forwarder(qdr_forward_closest_CT,   0, false);
    core->forwarders[QD_TREATMENT_ANYCAST_BALANCED] = qdr_new_forwarder(qdr_forward_balanced_CT,  0, false);

    //
    // Create link forwarders
    //
    core->forwarders[QD_TREATMENT_LINK_BALANCED] = qdr_new_forwarder(0, qdr_forward_link_balanced_CT, false);
}


qdr_forwarder_t *qdr_forwarder_CT(qdr_core_t *core, qd_address_treatment_t treatment)
{
    if (treatment <= QD_TREATMENT_LINK_BALANCED)
        return core->forwarders[treatment];
    return 0;
}


int qdr_forward_message_CT(qdr_core_t *core, qdr_address_t *addr, qd_message_t *msg, qdr_delivery_t *in_delivery,
                           bool exclude_inprocess, bool control)
{
    if (addr->forwarder)
        return addr->forwarder->forward_message(core, addr, msg, in_delivery, exclude_inprocess, control);

    // TODO - Deal with this delivery's disposition
    return 0;
}


bool qdr_forward_attach_CT(qdr_core_t *core, qdr_address_t *addr, qdr_link_t *in_link,
                           qdr_terminus_t *source, qdr_terminus_t *target)
{
    if (addr->forwarder)
        return addr->forwarder->forward_attach(core, addr, in_link, source, target);
    return false;
}


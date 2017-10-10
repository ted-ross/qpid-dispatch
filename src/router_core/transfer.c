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

static void qdr_link_deliver_CT(qdr_core_t *core, qdr_action_t *action, bool discard);
static void qdr_link_flow_CT(qdr_core_t *core, qdr_action_t *action, bool discard);
static void qdr_send_to_CT(qdr_core_t *core, qdr_action_t *action, bool discard);
static void qdr_update_delivery_CT(qdr_core_t *core, qdr_action_t *action, bool discard);
static void qdr_delete_delivery_CT(qdr_core_t *core, qdr_action_t *action, bool discard);
static void qdr_deliver_continue_CT(qdr_core_t *core, qdr_action_t *action, bool discard);

//==================================================================================
// Internal Functions
//==================================================================================

void qdr_delivery_read_extension_state(qdr_delivery_t *dlv, uint64_t disposition, pn_data_t* disposition_date, bool update_disposition);
void qdr_delivery_copy_extension_state(qdr_delivery_t *src, qdr_delivery_t *dest, bool update_disposition);


//==================================================================================
// Interface Functions
//==================================================================================

qdr_delivery_t *qdr_link_deliver(qdr_link_t *link, qd_message_t *msg, qd_iterator_t *ingress,
                                 bool settled, qd_bitmask_t *link_exclusion)
{
    qdr_action_t   *action = qdr_action(qdr_link_deliver_CT, "link_deliver");
    qdr_delivery_t *dlv    = new_qdr_delivery_t();

    ZERO(dlv);
    sys_atomic_init(&dlv->ref_count, 1); // referenced by the action
    dlv->link           = link;
    dlv->msg            = msg;
    dlv->to_addr        = 0;
    dlv->origin         = ingress;
    dlv->settled        = settled;
    dlv->presettled     = settled;
    dlv->link_exclusion = link_exclusion;
    dlv->error          = 0;

    action->args.connection.delivery = dlv;
    qdr_action_enqueue(link->core, action);
    return dlv;
}


qdr_delivery_t *qdr_link_deliver_to(qdr_link_t *link, qd_message_t *msg,
                                    qd_iterator_t *ingress, qd_iterator_t *addr,
                                    bool settled, qd_bitmask_t *link_exclusion)
{
    qdr_action_t   *action = qdr_action(qdr_link_deliver_CT, "link_deliver");
    qdr_delivery_t *dlv    = new_qdr_delivery_t();

    ZERO(dlv);
    sys_atomic_init(&dlv->ref_count, 1); // referenced by the action
    dlv->link           = link;
    dlv->msg            = msg;
    dlv->to_addr        = addr;
    dlv->origin         = ingress;
    dlv->settled        = settled;
    dlv->presettled     = settled;
    dlv->link_exclusion = link_exclusion;
    dlv->error          = 0;

    action->args.connection.delivery = dlv;
    qdr_action_enqueue(link->core, action);
    return dlv;
}


qdr_delivery_t *qdr_link_deliver_to_routed_link(qdr_link_t *link, qd_message_t *msg, bool settled,
                                                const uint8_t *tag, int tag_length,
                                                uint64_t disposition, pn_data_t* disposition_data)
{
    if (tag_length > 32)
        return 0;
    
    qdr_action_t   *action = qdr_action(qdr_link_deliver_CT, "link_deliver");
    qdr_delivery_t *dlv    = new_qdr_delivery_t();

    ZERO(dlv);
    sys_atomic_init(&dlv->ref_count, 1); // referenced by the action
    dlv->link       = link;
    dlv->msg        = msg;
    dlv->settled    = settled;
    dlv->presettled = settled;
    dlv->error      = 0;

    qdr_delivery_read_extension_state(dlv, disposition, disposition_data, true);

    action->args.connection.delivery = dlv;
    action->args.connection.tag_length = tag_length;
    memcpy(action->args.connection.tag, tag, tag_length);
    qdr_action_enqueue(link->core, action);
    return dlv;
}


qdr_delivery_t *qdr_deliver_continue(qdr_delivery_t *in_dlv)
{
    qdr_action_t   *action = qdr_action(qdr_deliver_continue_CT, "deliver_continue");
    action->args.connection.delivery = in_dlv;

    // This incref is for the action reference
    qdr_delivery_incref(in_dlv);
    qdr_action_enqueue(in_dlv->link->core, action);
    return in_dlv;
}


int qdr_link_process_deliveries(qdr_core_t *core, qdr_link_t *link, int credit)
{
    qdr_connection_t *conn = link->conn;
    qdr_delivery_t   *dlv;
    int               offer   = -1;
    bool              settled = false;
    bool              send_complete = false;
    int               num_deliveries_completed = 0;

    if (link->link_direction == QD_OUTGOING) {
        while (credit > 0) {
            sys_mutex_lock(conn->work_lock);
            dlv = DEQ_HEAD(link->undelivered);
            sys_mutex_unlock(conn->work_lock);
            if (dlv) {
                settled = dlv->settled;
                core->deliver_handler(core->user_context, link, dlv, settled);
                sys_mutex_lock(conn->work_lock);
                send_complete = qdr_delivery_send_complete(dlv);
                if (send_complete) {
                    //
                    // The entire message has been sent. It is now the appropriate time to have the delivery removed
                    // from the head of the undelivered list and move it to the unsettled list if it is not settled.
                    //
                    DEQ_REMOVE_HEAD(link->undelivered);
                    num_deliveries_completed ++;
                    dlv->link_work = 0;

                    if (settled) {
                        dlv->where = QDR_DELIVERY_NOWHERE;

                        // This decref is for removing this settled delivery from the undelivered list
                        qdr_delivery_decref(core, dlv);

                    } else {
                        DEQ_INSERT_TAIL(link->unsettled, dlv);
                        dlv->where = QDR_DELIVERY_IN_UNSETTLED;
                    }

                    credit--;
                    link->credit_to_core--;
                    link->total_deliveries++;
                    offer = DEQ_SIZE(link->undelivered);
                }
                else {
                    //
                    // The message is still being received/sent.
                    // 1. We cannot remove the delivery from the undelivered list.
                    //    This delivery needs to stay at the head of the undelivered list until the entire message
                    //    has been sent out i.e other deliveries in the undelivered list have to wait before this
                    //    entire large delivery is sent out
                    // 2. We need to call deliver_handler so any newly arrived bytes can be pushed out
                    // 3. We need to break out of this loop otherwise a thread will keep spinning in here until
                    //    the entire message has been sent out.
                    //
                    sys_mutex_unlock(conn->work_lock);

                    //
                    // Note here that we are not incrementing num_deliveries_processed. Since this delivery is
                    // still coming in or still being sent out, we cannot consider this delivery as fully processed.
                    //
                    return num_deliveries_completed;
                }
                sys_mutex_unlock(conn->work_lock);
            } else
                break;
        }

        if (offer != -1)
            core->offer_handler(core->user_context, link, offer);
    }

    return num_deliveries_completed;
}


void qdr_link_flow(qdr_core_t *core, qdr_link_t *link, int credit, bool drain_mode)
{
    qdr_action_t *action = qdr_action(qdr_link_flow_CT, "link_flow");

    //
    // Compute the number of credits now available that we haven't yet given
    // incrementally to the router core.  i.e. convert absolute credit to
    // incremental credit.
    //
    credit -= link->credit_to_core;
    if (credit < 0)
        credit = 0;
    link->credit_to_core += credit;

    action->args.connection.link   = link;
    action->args.connection.credit = credit;
    action->args.connection.drain  = drain_mode;

    qdr_action_enqueue(core, action);
}


void qdr_send_to1(qdr_core_t *core, qd_message_t *msg, qd_iterator_t *addr, bool exclude_inprocess, bool control)
{
    qdr_action_t *action = qdr_action(qdr_send_to_CT, "send_to");
    action->args.io.address           = qdr_field_from_iter(addr);
    action->args.io.message           = qd_message_copy(msg);
    action->args.io.exclude_inprocess = exclude_inprocess;
    action->args.io.control           = control;

    qdr_action_enqueue(core, action);
}


void qdr_send_to2(qdr_core_t *core, qd_message_t *msg, const char *addr, bool exclude_inprocess, bool control)
{
    qdr_action_t *action = qdr_action(qdr_send_to_CT, "send_to");
    action->args.io.address           = qdr_field(addr);
    action->args.io.message           = qd_message_copy(msg);
    action->args.io.exclude_inprocess = exclude_inprocess;
    action->args.io.control           = control;

    qdr_action_enqueue(core, action);
}


void qdr_delivery_update_disposition(qdr_core_t *core, qdr_delivery_t *delivery, uint64_t disposition,
                                     bool settled, qdr_error_t *error, pn_data_t *ext_state, bool ref_given)
{
    qdr_action_t *action = qdr_action(qdr_update_delivery_CT, "update_delivery");
    action->args.delivery.delivery    = delivery;
    action->args.delivery.disposition = disposition;
    action->args.delivery.settled     = settled;
    action->args.delivery.error       = error;

    // handle delivery-state extensions e.g. declared, transactional-state
    qdr_delivery_read_extension_state(delivery, disposition, ext_state, false);

    //
    // The delivery's ref_count must be incremented to protect its travels into the
    // core thread.  If the caller has given its reference to us, we can simply use
    // the given ref rather than increment a new one.
    //
    if (!ref_given)
        qdr_delivery_incref(delivery);

    qdr_action_enqueue(core, action);
}


void qdr_delivery_set_context(qdr_delivery_t *delivery, void *context)
{
    delivery->context = context;
}

void qdr_delivery_set_cleared_proton_ref(qdr_delivery_t *dlv, bool cleared_proton_ref)
{
    dlv->cleared_proton_ref = cleared_proton_ref;
}

void qdr_delivery_set_set_proton_ref(qdr_delivery_t *dlv, bool set_proton_ref)
{
    dlv->set_proton_ref = set_proton_ref;
}

bool qdr_delivery_get_set_proton_ref(qdr_delivery_t *dlv)
{
    return dlv->set_proton_ref;
}


void *qdr_delivery_get_context(qdr_delivery_t *delivery)
{
    return delivery->context;
}


bool qdr_delivery_send_complete(const qdr_delivery_t *delivery)
{
    if (!delivery)
        return false;
    return qd_message_send_complete(delivery->msg);
}

bool qdr_delivery_tag_sent(const qdr_delivery_t *delivery)
{
    if (!delivery)
        return false;
    return qd_message_tag_sent(delivery->msg);
}

void qdr_delivery_set_tag_sent(const qdr_delivery_t *delivery, bool tag_sent)
{
    if (!delivery)
        return;

    qd_message_set_tag_sent(delivery->msg, tag_sent);
}


bool qdr_delivery_receive_complete(const qdr_delivery_t *delivery)
{
    if (!delivery)
        return false;
    return qd_message_receive_complete(delivery->msg);
}


void qdr_delivery_incref(qdr_delivery_t *delivery)
{
    sys_atomic_inc(&delivery->ref_count);
}


void qdr_delivery_decref(qdr_core_t *core, qdr_delivery_t *delivery)
{
    uint32_t ref_count = sys_atomic_dec(&delivery->ref_count);
    assert(ref_count > 0);

    if (ref_count == 1) {
        //
        // The delivery deletion must occur inside the core thread.
        // Queue up an action to do the work.
        //
        qdr_action_t *action = qdr_action(qdr_delete_delivery_CT, "delete_delivery");
        action->args.delivery.delivery = delivery;
        qdr_action_enqueue(core, action);
    }
}


void qdr_delivery_tag(const qdr_delivery_t *delivery, const char **tag, int *length)
{
    *tag    = (const char*) delivery->tag;
    *length = delivery->tag_length;
}


qd_message_t *qdr_delivery_message(const qdr_delivery_t *delivery)
{
    if (!delivery)
        return 0;
    return delivery->msg;
}

qdr_error_t *qdr_delivery_error(const qdr_delivery_t *delivery)
{
    return delivery->error;
}


//==================================================================================
// In-Thread Functions
//==================================================================================

void qdr_delivery_release_CT(qdr_core_t *core, qdr_delivery_t *dlv)
{
    bool push = dlv->disposition != PN_RELEASED;

    dlv->disposition = PN_RELEASED;
    dlv->settled = true;
    bool moved = qdr_delivery_settled_CT(core, dlv);

    if (push || moved)
        qdr_delivery_push_CT(core, dlv);

    //
    // Remove the unsettled reference
    //
    if (moved)
        qdr_delivery_decref_CT(core, dlv);
}


void qdr_delivery_failed_CT(qdr_core_t *core, qdr_delivery_t *dlv)
{
    bool push = dlv->disposition != PN_MODIFIED;

    dlv->disposition = PN_MODIFIED;
    dlv->settled = true;
    bool moved = qdr_delivery_settled_CT(core, dlv);

    if (push || moved)
        qdr_delivery_push_CT(core, dlv);

    //
    // Remove the unsettled reference
    //
    if (moved)
        qdr_delivery_decref_CT(core, dlv);
}


bool qdr_delivery_settled_CT(qdr_core_t *core, qdr_delivery_t *dlv)
{
    //
    // Remove a delivery from its unsettled list.  Side effects include issuing
    // replacement credit and visiting the link-quiescence algorithm
    //
    qdr_link_t       *link  = dlv->link;
    qdr_connection_t *conn  = link ? link->conn : 0;
    bool              moved = false;

    if (!link || !conn)
        return false;

    //
    // The lock needs to be acquired only for outgoing links
    //
    if (link->link_direction == QD_OUTGOING)
        sys_mutex_lock(conn->work_lock);

    if (dlv->where == QDR_DELIVERY_IN_UNSETTLED) {
        DEQ_REMOVE(link->unsettled, dlv);
        dlv->where = QDR_DELIVERY_NOWHERE;
        moved = true;
    }

    if (link->link_direction == QD_OUTGOING)
        sys_mutex_unlock(conn->work_lock);

    if (dlv->tracking_addr) {
        dlv->tracking_addr->outstanding_deliveries[dlv->tracking_addr_bit]--;
        dlv->tracking_addr->tracked_deliveries--;

        if (dlv->tracking_addr->tracked_deliveries == 0)
            qdr_check_addr_CT(core, dlv->tracking_addr, false);

        dlv->tracking_addr = 0;
    }

    //
    // If this is an incoming link and it is not link-routed or inter-router, issue
    // one replacement credit on the link.  Note that credit on inter-router links is
    // issued immediately even for unsettled deliveries.
    //
    if (moved && link->link_direction == QD_INCOMING &&
        link->link_type != QD_LINK_ROUTER && !link->connected_link)
        qdr_link_issue_credit_CT(core, link, 1, false);

    return moved;
}


static void qdr_do_message_to_addr_free(qdr_core_t *core, qdr_general_work_t *work)
{
    if (work->msg)
        qd_message_free(work->msg);
    if (work->on_message_context)
        qd_iterator_free((qd_iterator_t *)work->on_message_context);
}


static void qdr_delete_delivery_internal_CT(qdr_core_t *core, qdr_delivery_t *delivery)
{
    qdr_link_t *link = delivery->link;

    if (delivery->msg || delivery->to_addr) {
        qdr_general_work_t *work = qdr_general_work(qdr_do_message_to_addr_free);
        work->msg                = delivery->msg;
        work->on_message_context = delivery->to_addr;
        qdr_post_general_work_CT(core, work);
    }

    if (delivery->tracking_addr) {
        delivery->tracking_addr->outstanding_deliveries[delivery->tracking_addr_bit]--;
        delivery->tracking_addr->tracked_deliveries--;

        if (delivery->tracking_addr->tracked_deliveries == 0)
            qdr_check_addr_CT(core, delivery->tracking_addr, false);

        delivery->tracking_addr = 0;
    }

    if (link) {
        if (delivery->presettled)
            link->presettled_deliveries++;
        else if (delivery->disposition == PN_ACCEPTED)
            link->accepted_deliveries++;
        else if (delivery->disposition == PN_REJECTED)
            link->rejected_deliveries++;
        else if (delivery->disposition == PN_RELEASED)
            link->released_deliveries++;
        else if (delivery->disposition == PN_MODIFIED)
            link->modified_deliveries++;
    }

    //
    // Free all the peer qdr_delivery_ref_t references
    //
    qdr_delivery_ref_t *ref = DEQ_HEAD(delivery->peers);
    while (ref) {
        qdr_del_delivery_ref(&delivery->peers, ref);
        ref = DEQ_HEAD(delivery->peers);
    }

    qd_bitmask_free(delivery->link_exclusion);
    qdr_error_free(delivery->error);
    free_qdr_delivery_t(delivery);

}

static bool qdr_delivery_has_peer_CT(qdr_delivery_t *dlv)
{
    return dlv->peer || DEQ_SIZE(dlv->peers) > 0;
}

void qdr_delivery_link_peers_CT(qdr_delivery_t *in_dlv, qdr_delivery_t *out_dlv)
{
    // If there is no delivery or a peer, we cannot link each other.
    if (!in_dlv || !out_dlv)
        return;

    if (!qdr_delivery_has_peer_CT(in_dlv)) {
        // This is the very first peer. Link them up.
        assert(!out_dlv->peer);
        in_dlv->peer = out_dlv;
    }
    else {
        if (in_dlv->peer) {
            // This is the first time we know that in_dlv is going to have more than one peer.
            // There is already a peer in the in_dlv->peer pointer, move it into a list and zero it out.
            qdr_add_delivery_ref_CT(&in_dlv->peers, in_dlv->peer);

            // Zero out the peer pointer. Since there is more than one peer, this peer has been moved to the "peers" linked list.
            // All peers will now reside in the peers linked list. No need to decref/incref here because you are transferring ownership.
            in_dlv->peer = 0;
        }

        qdr_add_delivery_ref_CT(&in_dlv->peers, out_dlv);
    }

    out_dlv->peer = in_dlv;

    qdr_delivery_incref(out_dlv);
    qdr_delivery_incref(in_dlv);
}


void qdr_delivery_unlink_peers_CT(qdr_core_t *core, qdr_delivery_t *dlv, qdr_delivery_t *peer)
{

    // If there is no delivery or a peer, we cannot proceed.
    if (!dlv || !peer)
        return;

    if (dlv->peer) {
        //
        // This is the easy case. One delivery has only one peer. we can simply
        // zero them out and directly decref.
        //
        assert(dlv->peer == peer);
        dlv->peer  = 0;
        peer->peer = 0;
        qdr_delivery_decref_CT(core, dlv);
        qdr_delivery_decref_CT(core, peer);
    }
    else {
        //
        // The dlv has more than one peer. We are going to find the peer of dlv that match with the passed in peer
        // and delete that peer.
        //
        qdr_delivery_ref_t *peer_ref = DEQ_HEAD(dlv->peers);
        while (peer_ref) {
            qdr_delivery_t *peer_dlv = peer_ref->dlv;
            if (peer_dlv == peer) {
                qdr_del_delivery_ref(&dlv->peers, peer_ref);
                if (peer->peer == dlv)  {
                    peer->peer = 0;
                    qdr_delivery_decref_CT(core, dlv);
                }
                qdr_delivery_decref_CT(core, peer);
                break;
            }
            peer_ref = DEQ_NEXT(peer_ref);
        }
    }
}


qdr_delivery_t *qdr_delivery_first_peer_CT(qdr_delivery_t *dlv)
{
    // What if there are no peers for this delivery?
    if (!qdr_delivery_has_peer_CT(dlv))
        return 0;

    if (dlv->peer) {
        // If there is a dlv->peer, it is the one and only peer.
        return dlv->peer;
    }
    else {
        // The delivery has more than one peer.
        qdr_delivery_ref_t *peer_ref = DEQ_HEAD(dlv->peers);

        // Save the next peer to dlv->next_peer_ref so we can use it when somebody calls qdr_delivery_next_peer_CT
        dlv->next_peer_ref = DEQ_NEXT(peer_ref);

        // Return the first peer.
        return peer_ref->dlv;
    }
}

qdr_delivery_t *qdr_delivery_next_peer_CT(qdr_delivery_t *dlv)
{
    if (dlv->peer) {
        // There is no next_peer if there is only one peer. If there is a non-zero dlv->peer, it is the only peer
        return 0;
    }
    else {
        // There is more than one peer to this delivery.
        qdr_delivery_ref_t *next_peer_ref = dlv->next_peer_ref;
        if (next_peer_ref) {
            // Save the next peer to dlv->next_peer_ref so we can use it when somebody calls qdr_delivery_next_peer_CT
            dlv->next_peer_ref = DEQ_NEXT(dlv->next_peer_ref);
            return next_peer_ref->dlv;
        }
        return 0;
    }
}


void qdr_delivery_decref_CT(qdr_core_t *core, qdr_delivery_t *dlv)
{
    uint32_t ref_count = sys_atomic_dec(&dlv->ref_count);

    assert(ref_count > 0);

    if (ref_count == 1)
        qdr_delete_delivery_internal_CT(core, dlv);
}


static void qdr_link_flow_CT(qdr_core_t *core, qdr_action_t *action, bool discard)
{
    if (discard)
        return;

    qdr_link_t *link      = action->args.connection.link;
    int  credit           = action->args.connection.credit;
    bool drain            = action->args.connection.drain;
    bool activate         = false;
    bool drain_was_set    = !link->drain_mode && drain;
    qdr_link_work_t *work = 0;
    
    link->drain_mode = drain;

    //
    // If this is an attach-routed link, propagate the flow data downrange.
    // Note that the credit value is incremental.
    //
    if (link->connected_link) {
        qdr_link_t *clink = link->connected_link;

        if (clink->link_direction == QD_INCOMING)
            qdr_link_issue_credit_CT(core, link->connected_link, credit, drain);
        else {
            work = new_qdr_link_work_t();
            ZERO(work);
            work->work_type = QDR_LINK_WORK_FLOW;
            work->value     = credit;
            if (drain)
                work->drain_action = QDR_LINK_WORK_DRAIN_ACTION_DRAINED;
            qdr_link_enqueue_work_CT(core, clink, work);
        }

        return;
    }

    //
    // Handle the replenishing of credit outbound
    //
    if (link->link_direction == QD_OUTGOING && (credit > 0 || drain_was_set)) {
        if (drain_was_set) {
            work = new_qdr_link_work_t();
            ZERO(work);
            work->work_type    = QDR_LINK_WORK_FLOW;
            work->drain_action = QDR_LINK_WORK_DRAIN_ACTION_DRAINED;
        }

        sys_mutex_lock(link->conn->work_lock);
        if (work)
            DEQ_INSERT_TAIL(link->work_list, work);
        if (DEQ_SIZE(link->undelivered) > 0 || drain_was_set) {
            qdr_add_link_ref(&link->conn->links_with_work, link, QDR_LINK_LIST_CLASS_WORK);
            activate = true;
        }
        sys_mutex_unlock(link->conn->work_lock);
    }

    //
    // Activate the connection if we have deliveries to send or drain mode was set.
    //
    if (activate)
        qdr_connection_activate_CT(core, link->conn);
}


/**
 * Return the number of outbound paths to destinations that this address has.
 * Note that even if there are more than zero paths, the destination still may
 * be unreachable (e.g. an rnode next hop with no link).
 */
static long qdr_addr_path_count_CT(qdr_address_t *addr)
{
    return (long) DEQ_SIZE(addr->subscriptions) + (long) DEQ_SIZE(addr->rlinks) +
        (long) qd_bitmask_cardinality(addr->rnodes);
}


static void qdr_link_forward_CT(qdr_core_t *core, qdr_link_t *link, qdr_delivery_t *dlv, qdr_address_t *addr)
{
    bool receive_complete = qd_message_receive_complete(qdr_delivery_message(dlv));
    if (addr && addr == link->owning_addr && qdr_addr_path_count_CT(addr) == 0) {
        //
        // We are trying to forward a delivery on an address that has no outbound paths
        // AND the incoming link is targeted (not anonymous).

        // If the owning_addr is a multicast addr and there are no outbound paths, we will release this delivery
        // and replenish the credit.

        // For non multicast addresses, put the delivery on the incoming link's undelivered list.  Note that it is safe
        // to do this because the undelivered list will be flushed once the number of
        // paths transitions from zero to one.
        //
        // Use the action-reference as the reference for undelivered rather
        // than decrementing and incrementing the delivery ref_count.
        //
        if (qdr_is_addr_treatment_multicast(link->owning_addr)) {
            qdr_delivery_release_CT(core, dlv);
            qdr_link_issue_credit_CT(core, link, 1, false);
        }
        else {
            DEQ_INSERT_TAIL(link->undelivered, dlv);
            dlv->where = QDR_DELIVERY_IN_UNDELIVERED;
        }
        return;
    }

    int fanout = 0;

    if (addr) {
        fanout = qdr_forward_message_CT(core, addr, dlv->msg, dlv, false, link->link_type == QD_LINK_CONTROL);
        if (link->link_type != QD_LINK_CONTROL && link->link_type != QD_LINK_ROUTER)
            addr->deliveries_ingress++;
        link->total_deliveries++;
    }
    //
    // There is no address that we can send this delivery to, which means the addr was not found in our hastable. This
    // can be because there were no receivers or because the address was not defined in the config file.
    // If the treatment for such addresses is set to be unavailable, we send back a rejected disposition and detach the link
    //
    else if (core->qd->default_treatment == QD_TREATMENT_UNAVAILABLE) {
        dlv->disposition = PN_REJECTED;
        dlv->error = qdr_error(QD_AMQP_COND_NOT_FOUND, "Deliveries cannot be sent to an unavailable address");
        qdr_delivery_push_CT(core, dlv);
        //
        // We will not detach this link because this could be anonymous sender. We don't know
        // which address the sender will be sending to next
        // If this was not an anonymous sender, the initial attach would have been rejected if the target address was unavailable.
        //
        return;
    }


    if (fanout == 0) {
        //
        // Message was not delivered, drop the delivery.
        //
        // If the delivery is not settled, release it.
        //
        if (!dlv->settled) {
            qdr_delivery_release_CT(core, dlv);

            //
            // Set the discard flag on the message only if the message is not completely received yet.
            //
            if (!receive_complete)
                qd_message_set_discard(dlv->msg, true);
        }

        //
        // Decrementing the delivery ref count for the action
        //
        qdr_delivery_decref_CT(core, dlv);
        qdr_link_issue_credit_CT(core, link, 1, false);
    } else if (fanout > 0) {
        if (dlv->settled || qdr_is_addr_treatment_multicast(addr)) {
            //
            // The delivery is settled.  Keep it off the unsettled list and issue
            // replacement credit for it now.
            //
            qdr_link_issue_credit_CT(core, link, 1, false);
            if (receive_complete) {
                //
                // This decref is for the action ref
                //
                qdr_delivery_decref_CT(core, dlv);
            }
            else {
                //
                // The message is still coming through since receive_complete is false. We have to put this delivery in the settled list.
                // We need to do this because we have linked this delivery to a peer.
                // If this connection goes down, we will have to unlink peer so that peer knows that its peer is not-existent anymore
                // and need to tell the other side that the message has been aborted.
                //

                //
                // Again, don't bother decrementing then incrementing the ref_count, we are still using the action ref count
                //
                DEQ_INSERT_TAIL(link->settled, dlv);
                dlv->where = QDR_DELIVERY_IN_SETTLED;
            }
        } else {
            //
            // Again, don't bother decrementing then incrementing the ref_count
            //
            DEQ_INSERT_TAIL(link->unsettled, dlv);
            dlv->where = QDR_DELIVERY_IN_UNSETTLED;

            //
            // If the delivery was received on an inter-router link, issue the credit
            // now.  We don't want to tie inter-router link flow control to unsettled
            // deliveries because it increases the risk of credit starvation if there
            // are many addresses sharing the link.
            //
            if (link->link_type == QD_LINK_ROUTER)
                qdr_link_issue_credit_CT(core, link, 1, false);
        }
    }
}


static void qdr_link_deliver_CT(qdr_core_t *core, qdr_action_t *action, bool discard)
{
    if (discard)
        return;

    qdr_delivery_t *dlv  = action->args.connection.delivery;
    qdr_link_t     *link = dlv->link;


    if (link->connected_link) {
        //
        // If this is an attach-routed link, put the delivery directly onto the peer link
        //
        qdr_delivery_t *peer = qdr_forward_new_delivery_CT(core, dlv, link->connected_link, dlv->msg);

        qdr_delivery_copy_extension_state(dlv, peer, true);

        //
        // Copy the delivery tag.  For link-routing, the delivery tag must be preserved.
        //
        peer->tag_length = action->args.connection.tag_length;
        memcpy(peer->tag, action->args.connection.tag, peer->tag_length);

        qdr_forward_deliver_CT(core, link->connected_link, peer);

        link->total_deliveries++;

        if (!dlv->settled) {
            DEQ_INSERT_TAIL(link->unsettled, dlv);
            dlv->where = QDR_DELIVERY_IN_UNSETTLED;

            //
            // Note, in this case the ref_count is left unchanged as we are transferring
            // the action's reference to the unsettled list's reference.
            //
        } else {
            //
            // If the delivery is settled, decrement the ref_count on the delivery.
            // This count was the owned-by-action count.
            //
            qdr_delivery_decref_CT(core, dlv);
        }
        return;
    }

    //
    // NOTE: The link->undelivered list does not need to be protected by the
    //       connection's work lock for incoming links.  This protection is only
    //       needed for outgoing links.
    //

    if (DEQ_IS_EMPTY(link->undelivered)) {
        qdr_address_t *addr = link->owning_addr;
        if (!addr && dlv->to_addr) {
            qdr_connection_t *conn = link->conn;
            if (conn && conn->tenant_space)
                qd_iterator_annotate_space(dlv->to_addr, conn->tenant_space, conn->tenant_space_len);
            qd_hash_retrieve(core->addr_hash, dlv->to_addr, (void**) &addr);
        }

        //
        // Give the action reference to the qdr_link_forward function. Don't decref/incref.
        //
        qdr_link_forward_CT(core, link, dlv, addr);
    } else {
        //
        // Take the action reference and use it for undelivered.  Don't decref/incref.
        //
        DEQ_INSERT_TAIL(link->undelivered, dlv);
        dlv->where = QDR_DELIVERY_IN_UNDELIVERED;
    }
}


static void qdr_send_to_CT(qdr_core_t *core, qdr_action_t *action, bool discard)
{
    qdr_field_t  *addr_field = action->args.io.address;
    qd_message_t *msg        = action->args.io.message;

    if (!discard) {
        qdr_address_t *addr = 0;

        qd_iterator_reset_view(addr_field->iterator, ITER_VIEW_ADDRESS_HASH);
        qd_hash_retrieve(core->addr_hash, addr_field->iterator, (void**) &addr);
        if (addr) {
            //
            // Forward the message.  We don't care what the fanout count is.
            //
            (void) qdr_forward_message_CT(core, addr, msg, 0, action->args.io.exclude_inprocess,
                                          action->args.io.control);
            addr->deliveries_from_container++;
        } else
            qd_log(core->log, QD_LOG_DEBUG, "In-process send to an unknown address");
    }

    qdr_field_free(addr_field);
    qd_message_free(msg);
}


static void qdr_update_delivery_CT(qdr_core_t *core, qdr_action_t *action, bool discard)
{
    qdr_delivery_t *dlv        = action->args.delivery.delivery;
    qdr_delivery_t *peer       = qdr_delivery_first_peer_CT(dlv);
    bool            push       = false;
    bool            peer_moved = false;
    bool            dlv_moved  = false;
    uint64_t        disp       = action->args.delivery.disposition;
    bool            settled    = action->args.delivery.settled;
    qdr_error_t    *error      = action->args.delivery.error;
    bool error_unassigned      = true;

    //
    // Logic:
    //
    // If disposition has changed and there is a peer link, set the disposition of the peer
    // If settled, the delivery must be unlinked and freed.
    // If settled and there is a peer, the peer shall be settled and unlinked.  It shall not
    //   be freed until the connection-side thread settles the PN delivery.
    //
    if (disp != dlv->disposition) {
        //
        // Disposition has changed, propagate the change to the peer delivery.
        //
        dlv->disposition = disp;
        if (peer) {
            peer->disposition = disp;
            peer->error       = error;
            push = true;
            error_unassigned = false;
            qdr_delivery_copy_extension_state(dlv, peer, false);
        }
    }

    if (settled) {
        if (peer) {
            peer->settled = true;
            if (peer->link) {
                peer_moved = qdr_delivery_settled_CT(core, peer);
                if (peer_moved)
                    push = true;
            }
            qdr_delivery_unlink_peers_CT(core, dlv, peer);
        }

        if (dlv->link)
            dlv_moved = qdr_delivery_settled_CT(core, dlv);
    }

    if (push)
        qdr_delivery_push_CT(core, peer);

    //
    // Release the action reference, possibly freeing the delivery
    //
    qdr_delivery_decref_CT(core, dlv);

    //
    // Release the unsettled references if the deliveries were moved
    //
    if (dlv_moved)
        qdr_delivery_decref_CT(core, dlv);
    if (peer_moved)
        qdr_delivery_decref_CT(core, peer);
    if (error_unassigned)
        qdr_error_free(error);
}


static void qdr_delete_delivery_CT(qdr_core_t *core, qdr_action_t *action, bool discard)
{
    if (!discard)
        qdr_delete_delivery_internal_CT(core, action->args.delivery.delivery);
}


static void qdr_deliver_continue_peers_CT(qdr_core_t *core, qdr_delivery_t *in_dlv)
{
    qdr_delivery_t *peer = qdr_delivery_first_peer_CT(in_dlv);
    while (peer) {
        qdr_link_work_t *work = peer->link_work;
        //
        // Determines if the peer connection can be activated.
        // For a large message, the peer delivery's link_work MUST be at the head of the peer link's work list. This link work is only removed
        // after the streaming message has been sent.
        //
        if (work) {
            sys_mutex_lock(peer->link->conn->work_lock);
            if (work == DEQ_HEAD(peer->link->work_list)) {
                qdr_add_link_ref(&peer->link->conn->links_with_work, peer->link, QDR_LINK_LIST_CLASS_WORK);
                sys_mutex_unlock(peer->link->conn->work_lock);

                //
                // Activate the outgoing connection for later processing.
                //
                qdr_connection_activate_CT(core, peer->link->conn);
            }
            else {
                sys_mutex_unlock(peer->link->conn->work_lock);

            }
        }

        peer = qdr_delivery_next_peer_CT(in_dlv);
    }
}


static void qdr_deliver_continue_CT(qdr_core_t *core, qdr_action_t *action, bool discard)
{
    if (discard)
        return;

    qdr_delivery_t *in_dlv  = action->args.connection.delivery;

    // This decref is for the action reference
    qdr_delivery_decref_CT(core, in_dlv);

    //
    // If it is already in the undelivered list or it has no peers, don't try to deliver this again.
    //
    if (in_dlv->where == QDR_DELIVERY_IN_UNDELIVERED || !qdr_delivery_has_peer_CT(in_dlv))
        return;

    qdr_deliver_continue_peers_CT(core, in_dlv);

    if (qd_message_receive_complete(qdr_delivery_message(in_dlv))) {
        //
        // The entire message has now been received. Check to see if there are in process subscriptions that need to
        // receive this message. in process subscriptions, at this time, can deal only with full messages.
        //
        qdr_subscription_t *sub = DEQ_HEAD(in_dlv->subscriptions);
        while (sub) {
            DEQ_REMOVE_HEAD(in_dlv->subscriptions);
            qdr_forward_on_message_CT(core, sub, in_dlv->link, in_dlv->msg);
            sub = DEQ_HEAD(in_dlv->subscriptions);
        }

        // This is a multicast delivery
        if (qdr_is_addr_treatment_multicast(in_dlv->link->owning_addr)) {
            assert(in_dlv->where == QDR_DELIVERY_IN_SETTLED);
            //
            // The router will settle on behalf of the receiver in the case of multicast and send out settled
            // deliveries to the receivers.
            //
            in_dlv->disposition = PN_ACCEPTED;
            qdr_delivery_push_CT(core, in_dlv);

            //
            // The in_dlv has one or more peers. These peers will have to be unlinked.
            //
            qdr_delivery_t *peer = qdr_delivery_first_peer_CT(in_dlv);
            qdr_delivery_t *next_peer = 0;
            while (peer) {
                next_peer = qdr_delivery_next_peer_CT(in_dlv);
                qdr_delivery_unlink_peers_CT(core, in_dlv, peer);
                peer = next_peer;
            }

            // Remove the delivery from the settled list and decref the in_dlv.
            in_dlv->where = QDR_DELIVERY_NOWHERE;
            DEQ_REMOVE(in_dlv->link->settled, in_dlv);
            qdr_delivery_decref_CT(core, in_dlv); // This decref is for removing the delivery from the settled list.
        }
    }
}


/**
 * Add link-work to provide credit to the link in an IO thread
 */
void qdr_link_issue_credit_CT(qdr_core_t *core, qdr_link_t *link, int credit, bool drain)
{
    assert(link->link_direction == QD_INCOMING);

    bool drain_changed = link->drain_mode |= drain;
    link->drain_mode   = drain;

    if (!drain_changed && credit == 0)
        return;

    if (credit > 0)
        link->flow_started = true;

    qdr_link_work_t *work = new_qdr_link_work_t();
    ZERO(work);

    work->work_type = QDR_LINK_WORK_FLOW;
    work->value     = credit;

    if (drain_changed)
        work->drain_action = drain ? QDR_LINK_WORK_DRAIN_ACTION_SET : QDR_LINK_WORK_DRAIN_ACTION_CLEAR;

    qdr_link_enqueue_work_CT(core, link, work);
}


/**
 * This function should be called after adding a new destination (subscription, local link,
 * or remote node) to an address.  If this address now has exactly one destination (i.e. it
 * transitioned from unreachable to reachable), make sure any unstarted in-links are issued
 * initial credit.
 *
 * Also, check the inlinks to see if there are undelivered messages.  If so, drain them to
 * the forwarder.
 */
void qdr_addr_start_inlinks_CT(qdr_core_t *core, qdr_address_t *addr)
{
    //
    // If there aren't any inlinks, there's no point in proceeding.
    //
    if (DEQ_SIZE(addr->inlinks) == 0)
        return;

    if (qdr_addr_path_count_CT(addr) == 1) {
        qdr_link_ref_t *ref = DEQ_HEAD(addr->inlinks);
        while (ref) {
            qdr_link_t *link = ref->link;

            //
            // Issue credit to stalled links
            //
            if (!link->flow_started)
                qdr_link_issue_credit_CT(core, link, link->capacity, false);

            //
            // Drain undelivered deliveries via the forwarder
            //
            if (DEQ_SIZE(link->undelivered) > 0) {
                //
                // Move all the undelivered to a local list in case not all can be delivered.
                // We don't want to loop here forever putting the same messages on the undelivered
                // list.
                //
                qdr_delivery_list_t deliveries;
                DEQ_MOVE(link->undelivered, deliveries);

                qdr_delivery_t *dlv = DEQ_HEAD(deliveries);
                while (dlv) {
                    DEQ_REMOVE_HEAD(deliveries);
                    qdr_link_forward_CT(core, link, dlv, addr);
                    dlv = DEQ_HEAD(deliveries);
                }
            }

            ref = DEQ_NEXT(ref);
        }
    }
}


void qdr_delivery_push_CT(qdr_core_t *core, qdr_delivery_t *dlv)
{
    if (!dlv || !dlv->link)
        return;

    qdr_link_t *link = dlv->link;
    bool activate = false;

    sys_mutex_lock(link->conn->work_lock);
    if (dlv->where != QDR_DELIVERY_IN_UNDELIVERED) {
        qdr_delivery_incref(dlv);
        qdr_add_delivery_ref_CT(&link->updated_deliveries, dlv);
        qdr_add_link_ref(&link->conn->links_with_work, link, QDR_LINK_LIST_CLASS_WORK);
        activate = true;
    }
    sys_mutex_unlock(link->conn->work_lock);

    //
    // Activate the connection
    //
    if (activate)
        qdr_connection_activate_CT(core, link->conn);
}

pn_data_t* qdr_delivery_extension_state(qdr_delivery_t *delivery)
{
    if (!delivery->extension_state) {
        delivery->extension_state = pn_data(0);
    }
    pn_data_rewind(delivery->extension_state);
    return delivery->extension_state;
}

void qdr_delivery_free_extension_state(qdr_delivery_t *delivery)
{
    if (delivery->extension_state) {
        pn_data_free(delivery->extension_state);
        delivery->extension_state = 0;
    }
}

void qdr_delivery_write_extension_state(qdr_delivery_t *dlv, pn_delivery_t* pdlv, bool update_disposition)
{
    if (dlv->disposition > PN_MODIFIED) {
        pn_data_copy(pn_disposition_data(pn_delivery_local(pdlv)), qdr_delivery_extension_state(dlv));
        if (update_disposition) pn_delivery_update(pdlv, dlv->disposition);
        qdr_delivery_free_extension_state(dlv);
    }
}

void qdr_delivery_export_transfer_state(qdr_delivery_t *dlv, pn_delivery_t* pdlv)
{
    qdr_delivery_write_extension_state(dlv, pdlv, true);
}

void qdr_delivery_export_disposition_state(qdr_delivery_t *dlv, pn_delivery_t* pdlv)
{
    qdr_delivery_write_extension_state(dlv, pdlv, false);
}

void qdr_delivery_copy_extension_state(qdr_delivery_t *src, qdr_delivery_t *dest, bool update_diposition)
{
    if (src->disposition > PN_MODIFIED) {
        pn_data_copy(qdr_delivery_extension_state(dest), qdr_delivery_extension_state(src));
        if (update_diposition) dest->disposition = src->disposition;
        qdr_delivery_free_extension_state(src);
    }
}

void qdr_delivery_read_extension_state(qdr_delivery_t *dlv, uint64_t disposition, pn_data_t* disposition_data, bool update_disposition)
{
    if (disposition > PN_MODIFIED) {
        pn_data_rewind(disposition_data);
        pn_data_copy(qdr_delivery_extension_state(dlv), disposition_data);
        if (update_disposition) dlv->disposition = disposition;
    }
}

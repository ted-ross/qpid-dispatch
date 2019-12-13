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

#include "module.h"
#include "router_core_private.h"
#include "core_events.h"
#include <qpid/dispatch/ctools.h>
#include <qpid/dispatch/log.h>
#include <qpid/dispatch/compose.h>
#include <qpid/dispatch/message.h>
#include <stdio.h>
#include <inttypes.h>

#define PROTOCOL_VERSION 1
static const char *OPCODE     = "opcode";
static const char *MAR        = "MAR";
static const char *MAU        = "MAU";
static const char *ID         = "id";
static const char *PV         = "pv";
static const char *AREA       = "area";
static const char *MOBILE_SEQ = "mobile_seq";
static const char *HINTS      = "hints";
static const char *ADD        = "add";
static const char *DEL        = "del";
static const char *EXIST      = "exist";
static const char *HAVE_SEQ   = "have_seq";

//
// Address.sync_mask bit values
//
#define ADDR_SYNC_DELETION_WAS_BLOCKED  0x00000001
#define ADDR_SYNC_IN_ADD_LIST           0x00000002
#define ADDR_SYNC_IN_DEL_LIST           0x00000004
#define ADDR_SYNC_TO_BE_DELETED         0x00000008

#define BIT_SET(M,B)   M |= B
#define BIT_CLEAR(M,B) M &= ~B
#define BIT_IS_SET(M,B) (M & B)

typedef struct {
    qdr_core_t                *core;
    qdrc_event_subscription_t *event_sub;
    qdr_core_timer_t          *timer;
    qdr_subscription_t        *message_sub1;
    qdr_subscription_t        *message_sub2;
    qd_log_source_t           *log;
    uint64_t                   mobile_seq;
    qdr_address_list_t         added_addrs;
    qdr_address_list_t         deleted_addrs;
} qdrm_mobile_sync_t;


//================================================================================
// Helper Functions
//================================================================================

static bool qcm_mobile_sync_addr_is_mobile(qdr_address_t *addr)
{
    const char *hash_key = (const char*) qd_hash_key_by_handle(addr->hash_handle);
    return !!strchr("MCDEFH", hash_key[0]);
}


qdr_node_t *qdc_mobile_sync_router_by_id(qdrm_mobile_sync_t *msync, qd_parsed_field_t *id_field)
{
    qd_iterator_t *id_iter = qd_parse_raw(id_field);
    qdr_node_t *router = DEQ_HEAD(msync->core->routers);
    while (!!router) {
        if (qd_iterator_equal(id_iter, qd_hash_key_by_handle(router->owning_addr->hash_handle) + 1))
            return router;
        router = DEQ_NEXT(router);
    }

    return 0;
}


/**
 * Set the 'block_deletion' flag on the address to ensure it is not deleted out from under
 * our list.  If the flag was already set, make note of that fact so we don't clear it later.
 */
static void qcm_mobile_sync_address_added_to_list(qdr_address_t *addr)
{
    if (addr->block_deletion) {
        BIT_SET(addr->sync_mask, ADDR_SYNC_DELETION_WAS_BLOCKED);
    } else {
        addr->block_deletion = true;
    }
}


/**
 * Clear the 'block_deletion' flag on the address if it was set by this module.
 * Check the address to have it deleted if it is no longer referenced anywhere.
 */
static void qcm_mobile_sync_address_removed_from_list(qdr_core_t *core, qdr_address_t *addr)
{
    if (!BIT_IS_SET(addr->sync_mask, ADDR_SYNC_DELETION_WAS_BLOCKED)) {
        addr->block_deletion = false;
        qdr_check_addr_CT(core, addr);
    } else {
        BIT_CLEAR(addr->sync_mask, ADDR_SYNC_DELETION_WAS_BLOCKED);
    }
}


static qd_composed_field_t *qcm_mobile_sync_message_headers(const char *address, const char *opcode)
{
    qd_composed_field_t *field = qd_compose(QD_PERFORMATIVE_HEADER, 0);
    qd_compose_start_list(field);
    qd_compose_insert_bool(field, 0); // durable
    qd_compose_end_list(field);

    field = qd_compose(QD_PERFORMATIVE_PROPERTIES, field);
    qd_compose_start_list(field);
    qd_compose_insert_null(field);            // message-id
    qd_compose_insert_null(field);            // user-id
    qd_compose_insert_string(field, address); // to
    qd_compose_insert_null(field);            // subject
    qd_compose_insert_null(field);            // reply-to
    qd_compose_insert_null(field);            // correlation-id
    qd_compose_end_list(field);

    field = qd_compose(QD_PERFORMATIVE_APPLICATION_PROPERTIES, field);
    qd_compose_start_map(field);
    qd_compose_insert_symbol(field, OPCODE);
    qd_compose_insert_string(field, opcode);
    qd_compose_end_map(field);

    return field;
}


static void qcm_mobile_sync_compose_diff_addr_list(qdrm_mobile_sync_t *msync, qd_composed_field_t *field, bool is_added)
{
    qdr_address_list_t *list = is_added ? &msync->added_addrs : &msync->deleted_addrs;

    qd_compose_start_list(field);
    qdr_address_t *addr = DEQ_HEAD(*list);
    while (addr) {
        const char *hash_key = (const char*) qd_hash_key_by_handle(addr->hash_handle);
        qd_compose_insert_string(field, hash_key);
        if (is_added) {
            DEQ_REMOVE_HEAD_N(SYNC_ADD, *list);
            BIT_CLEAR(addr->sync_mask, ADDR_SYNC_IN_ADD_LIST);
        } else {
            DEQ_REMOVE_HEAD_N(SYNC_DEL, *list);
            BIT_CLEAR(addr->sync_mask, ADDR_SYNC_IN_DEL_LIST);
        }
        qcm_mobile_sync_address_removed_from_list(msync->core, addr);
        addr = DEQ_HEAD(*list);
    }
    qd_compose_end_list(field);
}


static void qcm_mobile_sync_compose_diff_hint_list(qdrm_mobile_sync_t *msync, qd_composed_field_t *field)
{
    qd_compose_start_list(field);
    qdr_address_t *addr = DEQ_HEAD(msync->added_addrs);
    while (addr) {
        qd_compose_insert_int(field, addr->treatment);
        addr = DEQ_NEXT_N(SYNC_ADD, addr);
    }
    qd_compose_end_list(field);
}


static qd_message_t *qcm_mobile_sync_compose_differential_mau(qdrm_mobile_sync_t *msync, const char *address)
{
    qd_message_t        *msg     = qd_message();
    qd_composed_field_t *headers = qcm_mobile_sync_message_headers(address, MAU);
    qd_composed_field_t *body    = qd_compose(QD_PERFORMATIVE_BODY_AMQP_VALUE, 0);

    qd_compose_start_map(body);
    qd_compose_insert_symbol(body, ID);
    qd_compose_insert_string(body, msync->core->router_id);

    qd_compose_insert_symbol(body, PV);
    qd_compose_insert_long(body, PROTOCOL_VERSION);

    qd_compose_insert_symbol(body, AREA);
    qd_compose_insert_string(body, msync->core->router_area);

    qd_compose_insert_symbol(body, MOBILE_SEQ);
    qd_compose_insert_long(body, msync->mobile_seq);

    qd_compose_insert_symbol(body, HINTS);
    qcm_mobile_sync_compose_diff_hint_list(msync, body);

    qd_compose_insert_symbol(body, ADD);
    qcm_mobile_sync_compose_diff_addr_list(msync, body, true);

    qd_compose_insert_symbol(body, DEL);
    qcm_mobile_sync_compose_diff_addr_list(msync, body, false);

    qd_compose_end_map(body);

    qd_message_compose_3(msg, headers, body);
    return msg;
}


static qd_message_t *qcm_mobile_sync_compose_absolute_mau(qdrm_mobile_sync_t *msync, const char *address)
{
    qd_message_t        *msg     = qd_message();
    qd_composed_field_t *headers = qcm_mobile_sync_message_headers(address, MAU);
    qd_composed_field_t *body    = qd_compose(QD_PERFORMATIVE_BODY_AMQP_VALUE, 0);

    qd_compose_start_map(body);
    qd_compose_insert_symbol(body, ID);
    qd_compose_insert_string(body, msync->core->router_id);

    qd_compose_insert_symbol(body, PV);
    qd_compose_insert_long(body, PROTOCOL_VERSION);

    qd_compose_insert_symbol(body, AREA);
    qd_compose_insert_string(body, msync->core->router_area);

    qd_compose_insert_symbol(body, MOBILE_SEQ);
    qd_compose_insert_long(body, msync->mobile_seq);

    qd_compose_insert_symbol(body, EXIST);
    qd_compose_start_list(body);
    qdr_address_t *addr = DEQ_HEAD(msync->core->addrs);
    while (!!addr) {
        if (qcm_mobile_sync_addr_is_mobile(addr) && DEQ_SIZE(addr->rlinks) > 0) {
            const char *hash_key = (const char*) qd_hash_key_by_handle(addr->hash_handle);
            qd_compose_insert_string(body, hash_key);
        }
        addr = DEQ_NEXT(addr);
    }
    qd_compose_end_list(body);

    qd_compose_insert_symbol(body, HINTS);
    qd_compose_start_list(body);
    addr = DEQ_HEAD(msync->core->addrs);
    while (!!addr) {
        if (qcm_mobile_sync_addr_is_mobile(addr) && DEQ_SIZE(addr->rlinks) > 0)
            qd_compose_insert_int(body, addr->treatment);
        addr = DEQ_NEXT(addr);
    }
    qd_compose_end_list(body);

    qd_compose_end_map(body);

    qd_message_compose_3(msg, headers, body);
    return msg;
}


static qd_message_t *qcm_mobile_sync_compose_mar(qdrm_mobile_sync_t *msync, qdr_node_t *router)
{
    qd_message_t        *msg     = qd_message();
    qd_composed_field_t *headers = qcm_mobile_sync_message_headers(router->wire_address, MAR);
    qd_composed_field_t *body    = qd_compose(QD_PERFORMATIVE_BODY_AMQP_VALUE, 0);

    qd_compose_start_map(body);
    qd_compose_insert_symbol(body, ID);
    qd_compose_insert_string(body, msync->core->router_id);

    qd_compose_insert_symbol(body, PV);
    qd_compose_insert_long(body, PROTOCOL_VERSION);

    qd_compose_insert_symbol(body, AREA);
    qd_compose_insert_string(body, msync->core->router_area);

    qd_compose_insert_symbol(body, HAVE_SEQ);
    qd_compose_insert_long(body, router->mobile_seq);

    qd_compose_end_map(body);

    qd_message_compose_3(msg, headers, body);
    return msg;
}


//================================================================================
// Timer Handler
//================================================================================

static void qcm_mobile_sync_on_timer_CT(qdr_core_t *core, void *context)
{
    qdrm_mobile_sync_t *msync = (qdrm_mobile_sync_t*) context;

    //
    // Re-schedule the timer for the next go-around
    //
    qdr_core_timer_schedule_CT(core, msync->timer, 0);

    //
    // Check the add and delete lists.  If they are empty, nothing of note occured in the last
    // interval.  Exit the handler function.
    //
    size_t added_count   = DEQ_SIZE(msync->added_addrs);
    size_t deleted_count = DEQ_SIZE(msync->deleted_addrs);

    if (added_count == 0 && deleted_count == 0)
        return;

    //
    // Bump the mobile sequence number.
    //
    msync->mobile_seq++;

    //
    // Prepare a differential MAU for sending to all the other routers.
    //
    qd_message_t *mau = qcm_mobile_sync_compose_differential_mau(msync, "_topo/0/all/qdrouter.ma");

    //
    // Multicast the control message.  Set the exclude_inprocess and control flags.
    // Use the TOPOLOGICAL class address for sending.
    //
    int fanout = qdr_forward_message_CT(core, core->routerma_addr_T, mau, 0, true, true);

    //
    // Post the updated mobile sequence number to the Python router.  It is important that this be
    // done _after_ sending the differential MAU to prevent a storm of un-needed MAR requests from
    // the other routers.
    //
    qdr_post_set_mobile_seq_CT(core, msync->mobile_seq);

    //
    // Trace log the activity of this sequence update.
    //
    qd_log(msync->log, QD_LOG_INFO, "New mobile sequence: mobile_seq=%"PRIu64", addrs_added=%ld, addrs_deleted=%ld, fanout=%d",
           msync->mobile_seq, added_count, deleted_count, fanout);
}


//================================================================================
// Message Handler
//================================================================================

static void qcm_mobile_sync_on_mar_CT(qdrm_mobile_sync_t *msync, qd_iterator_t *body_iter)
{
    qd_parsed_field_t *body = qd_parse(body_iter);

    if (!!body && qd_parse_is_map(body)) {
        qd_parsed_field_t *id_field       = qd_parse_value_by_key(body, ID);
        qd_parsed_field_t *have_seq_field = qd_parse_value_by_key(body, HAVE_SEQ);
        uint64_t           have_seq       = qd_parse_as_ulong(have_seq_field);

        qdr_node_t *router = qdc_mobile_sync_router_by_id(msync, id_field);
        if (!!router) {
            qd_log(msync->log, QD_LOG_INFO, "Received MAR from %s, have_seq=%"PRIu64,
                   (const char*) qd_hash_key_by_handle(router->owning_addr->hash_handle) + 1, have_seq);

            if (have_seq < msync->mobile_seq) {
                //
                // The requestor's view of our mobile_seq is less than our actual mobile_sync.
                // Send them an absolute MAU to get them caught up to the present.
                //
                qd_message_t *mau = qcm_mobile_sync_compose_absolute_mau(msync, router->wire_address);
                (void) qdr_forward_message_CT(msync->core, router->owning_addr, mau, 0, true, true);

                //
                // Trace log the activity of this sequence update.
                //
                qd_log(msync->log, QD_LOG_INFO, "Sent MAU to requestor: mobile_seq=%"PRIu64, msync->mobile_seq);
            }
        }
    }
}


static void qcm_mobile_sync_on_mau_CT(qdrm_mobile_sync_t *msync, qd_iterator_t *body_iter)
{
}


static void qcm_mobile_sync_on_message_CT(void         *context,
                                          qd_message_t *msg,
                                          int           link_maskbit,
                                          int           inter_router_cost,
                                          uint64_t      conn_id)
{
    qdrm_mobile_sync_t *msync     = (qdrm_mobile_sync_t*) context;
    qd_iterator_t      *ap_iter   = qd_message_field_iterator(msg, QD_FIELD_APPLICATION_PROPERTIES);
    qd_iterator_t      *body_iter = qd_message_field_iterator(msg, QD_FIELD_BODY);
    qd_parsed_field_t  *ap_field  = qd_parse(ap_iter);

    if (!!ap_field && qd_parse_is_map(ap_field)) {
        qd_parsed_field_t *opcode_field = qd_parse_value_by_key(ap_field, OPCODE);

        if (qd_iterator_equal(qd_parse_raw(opcode_field), (const unsigned char*) MAR))
            qcm_mobile_sync_on_mar_CT(msync, body_iter);

        if (qd_iterator_equal(qd_parse_raw(opcode_field), (const unsigned char*) MAU))
            qcm_mobile_sync_on_mau_CT(msync, body_iter);
    }

    qd_parse_free(ap_field);
    qd_iterator_free(ap_iter);
    qd_iterator_free(body_iter);
}


//================================================================================
// Event Handlers
//================================================================================

static void qcm_mobile_sync_on_became_local_dest_CT(qdrm_mobile_sync_t *msync, qdr_address_t *addr)
{
    if (!qcm_mobile_sync_addr_is_mobile(addr))
        return;

    if (BIT_IS_SET(addr->sync_mask, ADDR_SYNC_IN_ADD_LIST)) {
        assert(false);
        return;
    }

    if (BIT_IS_SET(addr->sync_mask, ADDR_SYNC_IN_DEL_LIST)) {
        //
        // If the address was deleted since the last update, simply forget that it was deleted.
        //
        DEQ_REMOVE_N(SYNC_DEL, msync->deleted_addrs, addr);
        BIT_CLEAR(addr->sync_mask, ADDR_SYNC_IN_DEL_LIST);
        qcm_mobile_sync_address_removed_from_list(msync->core, addr);
    } else {
        DEQ_INSERT_TAIL_N(SYNC_ADD, msync->added_addrs, addr);
        BIT_SET(addr->sync_mask, ADDR_SYNC_IN_ADD_LIST);
        qcm_mobile_sync_address_added_to_list(addr);
    }
}


static void qcm_mobile_sync_on_no_longer_local_dest_CT(qdrm_mobile_sync_t *msync, qdr_address_t *addr)
{
    if (!qcm_mobile_sync_addr_is_mobile(addr))
        return;

    if (BIT_IS_SET(addr->sync_mask, ADDR_SYNC_IN_DEL_LIST)) {
        assert(false);
        return;
    }

    if (BIT_IS_SET(addr->sync_mask, ADDR_SYNC_IN_ADD_LIST)) {
        //
        // If the address was added since the last update, simply forget that it was added.
        //
        DEQ_REMOVE_N(SYNC_ADD, msync->added_addrs, addr);
        BIT_CLEAR(addr->sync_mask, ADDR_SYNC_IN_ADD_LIST);
        qcm_mobile_sync_address_removed_from_list(msync->core, addr);
    } else {
        DEQ_INSERT_TAIL_N(SYNC_DEL, msync->deleted_addrs, addr);
        BIT_SET(addr->sync_mask, ADDR_SYNC_IN_DEL_LIST);
        qcm_mobile_sync_address_added_to_list(addr);
    }
}


static void qcm_mobile_sync_on_router_flush_CT(qdrm_mobile_sync_t *msync, qdr_node_t *router)
{
    router->mobile_seq = 0;
}


static void qcm_mobile_sync_on_router_advanced_CT(qdrm_mobile_sync_t *msync, qdr_node_t *router)
{
    //
    // Prepare a MAR to be sent to the router
    //
    qd_message_t *mar = qcm_mobile_sync_compose_mar(msync, router);

    //
    // Send the control message.  Set the exclude_inprocess and control flags.
    //
    int fanout = qdr_forward_message_CT(msync->core, router->owning_addr, mar, 0, true, true);

    //
    // Trace log the activity of this sequence update.
    //
    qd_log(msync->log, QD_LOG_INFO, "Send MAR request to router %s, fanout=%d",
           (const char*) qd_hash_key_by_handle(router->owning_addr->hash_handle) + 1, fanout);
}


static void qcm_mobile_sync_on_addr_event_CT(void          *context,
                                             qdrc_event_t   event_type,
                                             qdr_address_t *addr)
{
    qdrm_mobile_sync_t *msync = (qdrm_mobile_sync_t*) context;

    switch (event_type) {
    case QDRC_EVENT_ADDR_BECAME_LOCAL_DEST:
        qcm_mobile_sync_on_became_local_dest_CT(msync, addr);
        break;
        
    case QDRC_EVENT_ADDR_NO_LONGER_LOCAL_DEST:
        qcm_mobile_sync_on_no_longer_local_dest_CT(msync, addr);
        break;
        
    default:
        break;
    }
}


static void qcm_mobile_sync_on_router_event_CT(void          *context,
                                               qdrc_event_t   event_type,
                                               qdr_node_t    *router)
{
    qdrm_mobile_sync_t *msync = (qdrm_mobile_sync_t*) context;

    switch (event_type) {
    case QDRC_EVENT_ROUTER_MOBILE_FLUSH:
        qcm_mobile_sync_on_router_flush_CT(msync, router);
        break;

    case QDRC_EVENT_ROUTER_MOBILE_SEQ_ADVANCED:
        qcm_mobile_sync_on_router_advanced_CT(msync, router);
        break;

    default:
        break;
    }
}


//================================================================================
// Module Handlers
//================================================================================

static bool qcm_mobile_sync_enable_CT(qdr_core_t *core)
{
    return core->router_mode == QD_ROUTER_MODE_INTERIOR;
}


static void qcm_mobile_sync_init_CT(qdr_core_t *core, void **module_context)
{
    qdrm_mobile_sync_t *msync = NEW(qdrm_mobile_sync_t);
    ZERO(msync);
    msync->core      = core;

    //
    // Subscribe to core events:
    //
    //  - ADDR_BECAME_LOCAL_DEST     - Indicates a new address needs to tbe sync'ed with other routers
    //  - ADDR_NO_LONGER_LOCAL_DEST  - Indicates an address needs to be un-sync'd with other routers
    //  - ROUTER_MOBILE_FLUSH        - All addresses associated with the router must be unmapped
    //  - ROUTER_MOBILE_SEQ_ADVANCED - A router has an advanced mobile-seq and needs to be queried
    //
    msync->event_sub = qdrc_event_subscribe_CT(core,
                                               QDRC_EVENT_ADDR_BECAME_LOCAL_DEST
                                               | QDRC_EVENT_ADDR_NO_LONGER_LOCAL_DEST
                                               | QDRC_EVENT_ROUTER_MOBILE_FLUSH
                                               | QDRC_EVENT_ROUTER_MOBILE_SEQ_ADVANCED,
                                               0,
                                               0,
                                               qcm_mobile_sync_on_addr_event_CT,
                                               qcm_mobile_sync_on_router_event_CT,
                                               msync);

    //
    // Create and schedule a one-second recurring timer to drive the sync protocol
    //
    msync->timer = qdr_core_timer_CT(core, qcm_mobile_sync_on_timer_CT, msync);
    qdr_core_timer_schedule_CT(core, msync->timer, 0);

    //
    // Subscribe to receive messages sent to the 'qdrouter.ma' addresses
    //
    msync->message_sub1 = qdr_core_subscribe(core, "qdrouter.ma", 'L', '0',
                                             QD_TREATMENT_MULTICAST_ONCE, true, qcm_mobile_sync_on_message_CT, msync);
    msync->message_sub2 = qdr_core_subscribe(core, "qdrouter.ma", 'T', '0',
                                             QD_TREATMENT_MULTICAST_ONCE, true, qcm_mobile_sync_on_message_CT, msync);

    //
    // Create a log source for mobile address sync
    //
    msync->log = qd_log_source("ROUTER_MA");

    *module_context = msync;
}


static void qcm_mobile_sync_final_CT(void *module_context)
{
    qdrm_mobile_sync_t *msync = (qdrm_mobile_sync_t*) module_context;

    qdrc_event_unsubscribe_CT(msync->core, msync->event_sub);
    qdr_core_timer_free_CT(msync->core, msync->timer);

    //
    // Don't explicitly unsubscribe the addresses, these are already gone at module-final time.
    //

    free(msync);
}


QDR_CORE_MODULE_DECLARE("mobile_sync", qcm_mobile_sync_enable_CT, qcm_mobile_sync_init_CT, qcm_mobile_sync_final_CT)

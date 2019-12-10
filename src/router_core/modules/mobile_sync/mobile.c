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
#include <stdio.h>
#include <inttypes.h>

//
// Address.sync_mask bit values
//
#define ADDR_SYNC_DELETION_WAS_BLOCKED  0x00000001
#define ADDR_SYNC_IN_ADD_LIST           0x00000002
#define ADDR_SYNC_IN_DEL_LIST           0x00000004

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

static qd_message_t *qcm_mobile_sync_compose_differential_mau(qdr_core_t *core, qdrm_mobile_sync_t *msync)
{
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
    qd_message_t *mau = qcm_mobile_sync_compose_differential_mau(core, msync);

    //
    // Send the control message.  Set the exclude_inprocess and control flags.
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
    qd_log(msync->log, QD_LOG_TRACE, "New mobile sequence: seq=%"PRIu64", addrs_added=%ld, addrs_deleted=%ld, fanout=%d",
           msync->mobile_seq, added_count, deleted_count, fanout);
}


//================================================================================
// Message Handler
//================================================================================

static void qcm_mobile_sync_on_message_CT(void         *context,
                                          qd_message_t *msg,
                                          int           link_maskbit,
                                          int           inter_router_cost,
                                          uint64_t      conn_id)
{
}


//================================================================================
// Event Handlers
//================================================================================

static void qcm_mobile_sync_on_became_local_dest_CT(qdrm_mobile_sync_t *msync, qdr_address_t *addr)
{
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
    if (BIT_IS_SET(addr->sync_mask, ADDR_SYNC_IN_DEL_LIST)) {
        assert(false);
        return;
    }

    if (BIT_IS_SET(addr->sync_mask, ADDR_SYNC_IN_ADD_LIST)) {
        //
        // If the address was added since the last update, simply forget that it was added.
        //
        DEQ_REMOVE_N(SYNC_ADD, msync->deleted_addrs, addr);
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
    msync->message_sub1 = qdr_core_subscribe(core, "qdrouter,ma", 'L', '0',
                                             QD_TREATMENT_MULTICAST_ONCE, true, qcm_mobile_sync_on_message_CT, msync);
    msync->message_sub2 = qdr_core_subscribe(core, "qdrouter,ma", 'T', '0',
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

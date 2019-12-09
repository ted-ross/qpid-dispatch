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
#include <stdio.h>

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
    uint64_t                   mobile_seq;
} qdrm_mobile_sync_t;

//================================================================================
// Timer Handler
//================================================================================

static void qcm_mobile_sync_on_timer_CT(qdr_core_t *core, void *context)
{
    qdrm_mobile_sync_t *msync = (qdrm_mobile_sync_t*) context;

    qdr_core_timer_schedule_CT(core, msync->timer, 0);
}


//================================================================================
// Event Handlers
//================================================================================

static void qcm_mobile_sync_on_became_local_dest(qdrm_mobile_sync_t *msync, qdr_address_t *addr)
{
}


static void qcm_mobile_sync_on_no_longer_local_dest(qdrm_mobile_sync_t *msync, qdr_address_t *addr)
{
}


static void qcm_mobile_sync_on_router_unreachable(qdrm_mobile_sync_t *msync, qdr_node_t *router)
{
}


static void qcm_mobile_sync_on_addr_event_CT(void          *context,
                                             qdrc_event_t   event_type,
                                             qdr_address_t *addr)
{
    qdrm_mobile_sync_t *msync = (qdrm_mobile_sync_t*) context;

    switch (event_type) {
    case QDRC_EVENT_ADDR_BECAME_LOCAL_DEST:
        qcm_mobile_sync_on_became_local_dest(msync, addr);
        break;
        
    case QDRC_EVENT_ADDR_NO_LONGER_LOCAL_DEST:
        qcm_mobile_sync_on_no_longer_local_dest(msync, addr);
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
    case QDRC_EVENT_ROUTER_UNREACHABLE:
        qcm_mobile_sync_on_router_unreachable(msync, router);
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
    msync->event_sub = qdrc_event_subscribe_CT(core,
                                               QDRC_EVENT_ADDR_BECAME_LOCAL_DEST
                                               | QDRC_EVENT_ADDR_NO_LONGER_LOCAL_DEST
                                               | QDRC_EVENT_ROUTER_UNREACHABLE,
                                               0,
                                               0,
                                               qcm_mobile_sync_on_addr_event_CT,
                                               qcm_mobile_sync_on_router_event_CT,
                                               msync);
    msync->timer     = qdr_core_timer_CT(core, qcm_mobile_sync_on_timer_CT, msync);

    qdr_core_timer_schedule_CT(core, msync->timer, 0);

    *module_context = msync;
}


static void qcm_mobile_sync_final_CT(void *module_context)
{
    qdrm_mobile_sync_t *msync = (qdrm_mobile_sync_t*) module_context;

    qdrc_event_unsubscribe_CT(msync->core, msync->event_sub);
    qdr_core_timer_free_CT(msync->core, msync->timer);

    free(msync);
}


QDR_CORE_MODULE_DECLARE("mobile_sync", qcm_mobile_sync_enable_CT, qcm_mobile_sync_init_CT, qcm_mobile_sync_final_CT)

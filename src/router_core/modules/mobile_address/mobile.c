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
#include "core_attach_address_lookup.h"
#include "router_core_private.h"
#include "core_events.h"
#include "core_client_api.h"
#include <qpid/dispatch/ctools.h>
#include <stdio.h>

//================================================================================
// Module Handlers
//================================================================================

static bool qcm_mobile_enable_CT(qdr_core_t *core)
{
    return core->router_mode == QD_ROUTER_MODE_INTERIOR;
}


static void qcm_mobile_init_CT(qdr_core_t *core, void **module_context)
{
    *module_context = 0;
}


static void qcm_mobile_final_CT(void *module_context)
{
}


QDR_CORE_MODULE_DECLARE("mobile_address", qcm_mobile_enable_CT, qcm_mobile_init_CT, qcm_mobile_final_CT)

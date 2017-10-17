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

#include <Python.h>
#include <qpid/dispatch/ctools.h>
#include <qpid/dispatch/threading.h>
#include <qpid/dispatch/log.h>
#include <qpid/dispatch/amqp.h>
#include <qpid/dispatch/server.h>
#include <qpid/dispatch/failoverlist.h>
#include <qpid/dispatch/alloc.h>

#include <proton/event.h>
#include <proton/listener.h>
#include <proton/netaddr.h>
#include <proton/proactor.h>
#include <proton/sasl.h>

#include "qpid/dispatch/python_embedded.h"
#include "entity.h"
#include "entity_cache.h"
#include "dispatch_private.h"
#include "policy.h"
#include "server_private.h"
#include "timer_private.h"
#include "config.h"
#include "remote_sasl.h"
#include <stdio.h>
#include <string.h>
#include <errno.h>
#include <inttypes.h>

struct qd_server_t {
    qd_dispatch_t            *qd;
    const int                 thread_count; /* Immutable */
    const char               *container_name;
    const char               *sasl_config_path;
    const char               *sasl_config_name;
    pn_proactor_t            *proactor;
    qd_container_t           *container;
    qd_log_source_t          *log_source;
    void                     *start_context;
    sys_cond_t               *cond;
    sys_mutex_t              *lock;
    qd_connection_list_t      conn_list;
    int                       pause_requests;
    int                       threads_paused;
    int                       pause_next_sequence;
    int                       pause_now_serving;
    uint64_t                  next_connection_id;
    void                     *py_displayname_obj;
    qd_http_server_t         *http;
};

#define HEARTBEAT_INTERVAL 1000

ALLOC_DEFINE(qd_listener_t);
ALLOC_DEFINE(qd_connector_t);
ALLOC_DEFINE(qd_deferred_call_t);
ALLOC_DEFINE(qd_connection_t);

const char *QD_CONNECTION_TYPE = "connection";
const char *MECH_EXTERNAL = "EXTERNAL";

//Allowed uidFormat fields.
const char CERT_COUNTRY_CODE = 'c';
const char CERT_STATE = 's';
const char CERT_CITY_LOCALITY = 'l';
const char CERT_ORGANIZATION_NAME = 'o';
const char CERT_ORGANIZATION_UNIT = 'u';
const char CERT_COMMON_NAME = 'n';
const char CERT_FINGERPRINT_SHA1 = '1';
const char CERT_FINGERPRINT_SHA256 = '2';
const char CERT_FINGERPRINT_SHA512 = '5';
char *COMPONENT_SEPARATOR = ";";

static const int BACKLOG = 50;  /* Listening backlog */

static void setup_ssl_sasl_and_open(qd_connection_t *ctx);

/**
 * This function is set as the pn_transport->tracer and is invoked when proton tries to write the log message to pn_transport->tracer
 */
static void transport_tracer(pn_transport_t *transport, const char *message)
{
    qd_connection_t *ctx = (qd_connection_t*) pn_transport_get_context(transport);
    if (ctx)
        qd_log(ctx->server->log_source, QD_LOG_TRACE, "[%"PRIu64"]:%s", ctx->connection_id, message);
}


/**
 * Save displayNameService object instance and ImportModule address
 * Called with qd_python_lock held
 */
qd_error_t qd_register_display_name_service(qd_dispatch_t *qd, void *displaynameservice)
{
    if (displaynameservice) {
        qd->server->py_displayname_obj = displaynameservice;
        Py_XINCREF((PyObject *)qd->server->py_displayname_obj);
        return QD_ERROR_NONE;
    }
    else {
        return qd_error(QD_ERROR_VALUE, "displaynameservice is not set");
    }
}


/**
 * Returns a char pointer to a user id which is constructed from components specified in the config->ssl_uid_format.
 * Parses through each component and builds a semi-colon delimited string which is returned as the user id.
 */
static const char *transport_get_user(qd_connection_t *conn, pn_transport_t *tport)
{
    const qd_server_config_t *config =
            conn->connector ? &conn->connector->config : &conn->listener->config;

    if (config->ssl_uid_format) {
        // The ssl_uid_format length cannot be greater that 7
        assert(strlen(config->ssl_uid_format) < 8);

        //
        // The tokens in the uidFormat strings are delimited by comma. Load the individual components of the uidFormat
        // into the components[] array. The maximum number of components that are allowed are 7 namely, c, s, l, o, u, n, (1 or 2 or 5)
        //
        char components[8];

        //The strcpy() function copies the string pointed to by src, including the terminating null byte ('\0'), to the buffer pointed to by dest.
        strncpy(components, config->ssl_uid_format, 7);

        const char *country_code = 0;
        const char *state = 0;
        const char *locality_city = 0;
        const char *organization = 0;
        const char *org_unit = 0;
        const char *common_name = 0;
        //
        // SHA1 is 20 octets (40 hex characters); SHA256 is 32 octets (64 hex characters).
        // SHA512 is 64 octets (128 hex characters)
        //
        char fingerprint[129] = "\0";

        int uid_length = 0;
        int semi_colon_count = -1;

        int component_count = strlen(components);

        for (int x = 0; x < component_count ; x++) {
            // accumulate the length into uid_length on each pass so we definitively know the number of octets to malloc.
            if (components[x] == CERT_COUNTRY_CODE) {
                country_code =  pn_ssl_get_remote_subject_subfield(pn_ssl(tport), PN_SSL_CERT_SUBJECT_COUNTRY_NAME);
                if (country_code) {
                    uid_length += strlen((const char *)country_code);
                    semi_colon_count++;
                }
            }
            else if (components[x] == CERT_STATE) {
                state =  pn_ssl_get_remote_subject_subfield(pn_ssl(tport), PN_SSL_CERT_SUBJECT_STATE_OR_PROVINCE);
                if (state) {
                    uid_length += strlen((const char *)state);
                    semi_colon_count++;
                }
            }
            else if (components[x] == CERT_CITY_LOCALITY) {
                locality_city =  pn_ssl_get_remote_subject_subfield(pn_ssl(tport), PN_SSL_CERT_SUBJECT_CITY_OR_LOCALITY);
                if (locality_city) {
                    uid_length += strlen((const char *)locality_city);
                    semi_colon_count++;
                }
            }
            else if (components[x] == CERT_ORGANIZATION_NAME) {
                organization =  pn_ssl_get_remote_subject_subfield(pn_ssl(tport), PN_SSL_CERT_SUBJECT_ORGANIZATION_NAME);
                if(organization) {
                    uid_length += strlen((const char *)organization);
                    semi_colon_count++;
                }
            }
            else if (components[x] == CERT_ORGANIZATION_UNIT) {
                org_unit =  pn_ssl_get_remote_subject_subfield(pn_ssl(tport), PN_SSL_CERT_SUBJECT_ORGANIZATION_UNIT);
                if(org_unit) {
                    uid_length += strlen((const char *)org_unit);
                    semi_colon_count++;
                }
            }
            else if (components[x] == CERT_COMMON_NAME) {
                common_name =  pn_ssl_get_remote_subject_subfield(pn_ssl(tport), PN_SSL_CERT_SUBJECT_COMMON_NAME);
                if(common_name) {
                    uid_length += strlen((const char *)common_name);
                    semi_colon_count++;
                }
            }
            else if (components[x] == CERT_FINGERPRINT_SHA1 || components[x] == CERT_FINGERPRINT_SHA256 || components[x] == CERT_FINGERPRINT_SHA512) {
                // Allocate the memory for message digest
                int out = 0;

                int fingerprint_length = 0;
                if(components[x] == CERT_FINGERPRINT_SHA1) {
                    fingerprint_length = 40;
                    out = pn_ssl_get_cert_fingerprint(pn_ssl(tport), fingerprint, fingerprint_length + 1, PN_SSL_SHA1);
                }
                else if (components[x] == CERT_FINGERPRINT_SHA256) {
                    fingerprint_length = 64;
                    out = pn_ssl_get_cert_fingerprint(pn_ssl(tport), fingerprint, fingerprint_length + 1, PN_SSL_SHA256);
                }
                else if (components[x] == CERT_FINGERPRINT_SHA512) {
                    fingerprint_length = 128;
                    out = pn_ssl_get_cert_fingerprint(pn_ssl(tport), fingerprint, fingerprint_length + 1, PN_SSL_SHA512);
                }

                (void) out;  // avoid 'out unused' compiler warnings if NDEBUG undef'ed
                assert (out != PN_ERR);

                uid_length += fingerprint_length;
                semi_colon_count++;
            }
            else {
                // This is an unrecognized component. log a critical error
                qd_log(conn->server->log_source, QD_LOG_CRITICAL, "Unrecognized component '%c' in uidFormat ", components[x]);
                return 0;
            }
        }

        if(uid_length > 0) {
            char *user_id = malloc((uid_length + semi_colon_count + 1) * sizeof(char)); // the +1 is for the '\0' character
            //
            // We have allocated memory for user_id. We are responsible for freeing this memory. Set conn->free_user_id
            // to true so that we know that we have to free the user_id
            //
            conn->free_user_id = true;
            memset(user_id, 0, uid_length + semi_colon_count + 1);

            // The components in the user id string must appear in the same order as it appears in the component string. that is
            // why we have this loop
            for (int x=0; x < component_count ; x++) {
                if (components[x] == CERT_COUNTRY_CODE) {
                    if (country_code) {
                        if(*user_id != '\0')
                            strcat(user_id, COMPONENT_SEPARATOR);
                        strcat(user_id, (char *) country_code);
                    }
                }
                else if (components[x] == CERT_STATE) {
                    if (state) {
                        if(*user_id != '\0')
                            strcat(user_id, COMPONENT_SEPARATOR);
                        strcat(user_id, (char *) state);
                    }
                }
                else if (components[x] == CERT_CITY_LOCALITY) {
                    if (locality_city) {
                        if(*user_id != '\0')
                            strcat(user_id, COMPONENT_SEPARATOR);
                        strcat(user_id, (char *) locality_city);
                    }
                }
                else if (components[x] == CERT_ORGANIZATION_NAME) {
                    if (organization) {
                        if(*user_id != '\0')
                            strcat(user_id, COMPONENT_SEPARATOR);
                        strcat(user_id, (char *) organization);
                    }
                }
                else if (components[x] == CERT_ORGANIZATION_UNIT) {
                    if (org_unit) {
                        if(*user_id != '\0')
                            strcat(user_id, COMPONENT_SEPARATOR);
                        strcat(user_id, (char *) org_unit);
                    }
                }
                else if (components[x] == CERT_COMMON_NAME) {
                    if (common_name) {
                        if(*user_id != '\0')
                            strcat(user_id, COMPONENT_SEPARATOR);
                        strcat(user_id, (char *) common_name);
                    }
                }
                else if (components[x] == CERT_FINGERPRINT_SHA1 || components[x] == CERT_FINGERPRINT_SHA256 || components[x] == CERT_FINGERPRINT_SHA512) {
                    if (strlen((char *) fingerprint) > 0) {
                        if(*user_id != '\0')
                            strcat(user_id, COMPONENT_SEPARATOR);
                        strcat(user_id, (char *) fingerprint);
                    }
                }
            }
            if (config->ssl_display_name_file) {
                // Translate extracted id into display name
                qd_python_lock_state_t lock_state = qd_python_lock();
                PyObject *result = PyObject_CallMethod((PyObject *)conn->server->py_displayname_obj, "query", "(ss)", config->ssl_profile, user_id );
                if (result) {
                    const char *res_string = PyString_AsString(result);
                    free(user_id);
                    user_id = malloc(strlen(res_string) + 1);
                    user_id[0] = '\0';
                    strcat(user_id, res_string);
                    Py_XDECREF(result);
                } else {
                    qd_log(conn->server->log_source, QD_LOG_DEBUG, "Internal: failed to read displaynameservice query result");
                }
                qd_python_unlock(lock_state);
            }
            qd_log(conn->server->log_source, QD_LOG_DEBUG, "User id is '%s' ", user_id);
            return user_id;
        }
    }
    else //config->ssl_uid_format not specified, just return the username provided by the proton transport.
        return pn_transport_get_user(tport);

    return 0;
}


void qd_connection_set_user(qd_connection_t *conn)
{
    pn_transport_t *tport = pn_connection_transport(conn->pn_conn);
    pn_sasl_t      *sasl  = pn_sasl(tport);
    if (sasl) {
        const char *mech = pn_sasl_get_mech(sasl);
        conn->user_id = pn_transport_get_user(tport);
        // We want to set the user name only if it is not already set and the selected sasl mechanism is EXTERNAL
        if (mech && strcmp(mech, MECH_EXTERNAL) == 0) {
            const char *user_id = transport_get_user(conn, tport);
            if (user_id)
                conn->user_id = user_id;
        }
    }
}


qd_error_t qd_entity_refresh_sslProfile(qd_entity_t* entity, void *impl)
{
    return QD_ERROR_NONE;
}

qd_error_t qd_entity_refresh_authServicePlugin(qd_entity_t* entity, void *impl)
{
    return QD_ERROR_NONE;
}


static qd_error_t listener_setup_ssl(qd_connection_t *ctx, const qd_server_config_t *config, pn_transport_t *tport)
{
    pn_ssl_domain_t *domain = pn_ssl_domain(PN_SSL_MODE_SERVER);
    if (!domain) return qd_error(QD_ERROR_RUNTIME, "No SSL support");

    // setup my identifying cert:
    if (pn_ssl_domain_set_credentials(domain,
                                      config->ssl_certificate_file,
                                      config->ssl_private_key_file,
                                      config->ssl_password)) {
        pn_ssl_domain_free(domain);
        return qd_error(QD_ERROR_RUNTIME, "Cannot set SSL credentials");
    }
    if (!config->ssl_required) {
        if (pn_ssl_domain_allow_unsecured_client(domain)) {
            pn_ssl_domain_free(domain);
            return qd_error(QD_ERROR_RUNTIME, "Cannot allow unsecured client");
        }
    }

    // for peer authentication:
    if (config->ssl_trusted_certificate_db) {
        if (pn_ssl_domain_set_trusted_ca_db(domain, config->ssl_trusted_certificate_db)) {
            pn_ssl_domain_free(domain);
            return qd_error(QD_ERROR_RUNTIME, "Cannot set trusted SSL CA" );
        }
    }

    if (config->ciphers) {
        if (pn_ssl_domain_set_ciphers(domain, config->ciphers)) {
            pn_ssl_domain_free(domain);
            return qd_error(QD_ERROR_RUNTIME, "Cannot set ciphers. The ciphers string might be invalid. Use openssl ciphers -v <ciphers> to validate");
        }
    }

    const char *trusted = config->ssl_trusted_certificate_db;
    if (config->ssl_trusted_certificates)
        trusted = config->ssl_trusted_certificates;

    // do we force the peer to send a cert?
    if (config->ssl_require_peer_authentication) {
        if (!trusted || pn_ssl_domain_set_peer_authentication(domain, PN_SSL_VERIFY_PEER, trusted)) {
            pn_ssl_domain_free(domain);
            return qd_error(QD_ERROR_RUNTIME, "Cannot set peer authentication");
        }
    }

    ctx->ssl = pn_ssl(tport);
    if (!ctx->ssl || pn_ssl_init(ctx->ssl, domain, 0)) {
        pn_ssl_domain_free(domain);
        return qd_error(QD_ERROR_RUNTIME, "Cannot initialize SSL");
    }

    pn_ssl_domain_free(domain);
    return QD_ERROR_NONE;
}


static void decorate_connection(qd_server_t *qd_server, pn_connection_t *conn, const qd_server_config_t *config)
{
    size_t clen = strlen(QD_CAPABILITY_ANONYMOUS_RELAY);

    //
    // Set the container name
    //
    pn_connection_set_container(conn, qd_server->container_name);

    //
    // Offer ANONYMOUS_RELAY capability
    //
    pn_data_put_symbol(pn_connection_offered_capabilities(conn), pn_bytes(clen, (char*) QD_CAPABILITY_ANONYMOUS_RELAY));

    //
    // Create the connection properties map
    //
    pn_data_put_map(pn_connection_properties(conn));
    pn_data_enter(pn_connection_properties(conn));

    pn_data_put_symbol(pn_connection_properties(conn),
                       pn_bytes(strlen(QD_CONNECTION_PROPERTY_PRODUCT_KEY), QD_CONNECTION_PROPERTY_PRODUCT_KEY));
    pn_data_put_string(pn_connection_properties(conn),
                       pn_bytes(strlen(QD_CONNECTION_PROPERTY_PRODUCT_VALUE), QD_CONNECTION_PROPERTY_PRODUCT_VALUE));

    pn_data_put_symbol(pn_connection_properties(conn),
                       pn_bytes(strlen(QD_CONNECTION_PROPERTY_VERSION_KEY), QD_CONNECTION_PROPERTY_VERSION_KEY));
    pn_data_put_string(pn_connection_properties(conn),
                       pn_bytes(strlen(QPID_DISPATCH_VERSION), QPID_DISPATCH_VERSION));

    if (config && config->inter_router_cost > 1) {
        pn_data_put_symbol(pn_connection_properties(conn),
                           pn_bytes(strlen(QD_CONNECTION_PROPERTY_COST_KEY), QD_CONNECTION_PROPERTY_COST_KEY));
        pn_data_put_int(pn_connection_properties(conn), config->inter_router_cost);
    }

    if (config) {
        qd_failover_list_t *fol = config->failover_list;
        if (fol) {
            pn_data_put_symbol(pn_connection_properties(conn),
                               pn_bytes(strlen(QD_CONNECTION_PROPERTY_FAILOVER_LIST_KEY), QD_CONNECTION_PROPERTY_FAILOVER_LIST_KEY));
            pn_data_put_list(pn_connection_properties(conn));
            pn_data_enter(pn_connection_properties(conn));
            int fol_count = qd_failover_list_size(fol);
            for (int i = 0; i < fol_count; i++) {
                pn_data_put_map(pn_connection_properties(conn));
                pn_data_enter(pn_connection_properties(conn));
                pn_data_put_symbol(pn_connection_properties(conn),
                                   pn_bytes(strlen(QD_CONNECTION_PROPERTY_FAILOVER_NETHOST_KEY), QD_CONNECTION_PROPERTY_FAILOVER_NETHOST_KEY));
                pn_data_put_string(pn_connection_properties(conn),
                                   pn_bytes(strlen(qd_failover_list_host(fol, i)), qd_failover_list_host(fol, i)));

                pn_data_put_symbol(pn_connection_properties(conn),
                                   pn_bytes(strlen(QD_CONNECTION_PROPERTY_FAILOVER_PORT_KEY), QD_CONNECTION_PROPERTY_FAILOVER_PORT_KEY));
                pn_data_put_string(pn_connection_properties(conn),
                                   pn_bytes(strlen(qd_failover_list_port(fol, i)), qd_failover_list_port(fol, i)));

                if (qd_failover_list_scheme(fol, i)) {
                    pn_data_put_symbol(pn_connection_properties(conn),
                                       pn_bytes(strlen(QD_CONNECTION_PROPERTY_FAILOVER_SCHEME_KEY), QD_CONNECTION_PROPERTY_FAILOVER_SCHEME_KEY));
                    pn_data_put_string(pn_connection_properties(conn),
                                       pn_bytes(strlen(qd_failover_list_scheme(fol, i)), qd_failover_list_scheme(fol, i)));
                }

                if (qd_failover_list_hostname(fol, i)) {
                    pn_data_put_symbol(pn_connection_properties(conn),
                                       pn_bytes(strlen(QD_CONNECTION_PROPERTY_FAILOVER_HOSTNAME_KEY), QD_CONNECTION_PROPERTY_FAILOVER_HOSTNAME_KEY));
                    pn_data_put_string(pn_connection_properties(conn),
                                       pn_bytes(strlen(qd_failover_list_hostname(fol, i)), qd_failover_list_hostname(fol, i)));
                }
                pn_data_exit(pn_connection_properties(conn));
            }
            pn_data_exit(pn_connection_properties(conn));
        }
    }

    pn_data_exit(pn_connection_properties(conn));
}

/* Wake function for proactor-manaed connections */
static void connection_wake(qd_connection_t *ctx) {
    if (ctx->pn_conn) pn_connection_wake(ctx->pn_conn);
}

/* Construct a new qd_connection. Thread safe. */
qd_connection_t *qd_server_connection(qd_server_t *server, qd_server_config_t *config)
{
    qd_connection_t *ctx = new_qd_connection_t();
    if (!ctx) return NULL;
    ZERO(ctx);
    ctx->pn_conn       = pn_connection();
    ctx->deferred_call_lock = sys_mutex();
    ctx->role = strdup(config->role);
    if (!ctx->pn_conn || !ctx->deferred_call_lock || !ctx->role) {
        if (ctx->pn_conn) pn_connection_free(ctx->pn_conn);
        if (ctx->deferred_call_lock) sys_mutex_free(ctx->deferred_call_lock);
        free(ctx->role);
        free(ctx);
        return NULL;
    }
    ctx->server = server;
    ctx->wake = connection_wake; /* Default, over-ridden for HTTP connections */
    pn_connection_set_context(ctx->pn_conn, ctx);
    DEQ_ITEM_INIT(ctx);
    DEQ_INIT(ctx->deferred_calls);
    sys_mutex_lock(server->lock);
    ctx->connection_id = server->next_connection_id++;
    DEQ_INSERT_TAIL(server->conn_list, ctx);
    sys_mutex_unlock(server->lock);
    decorate_connection(ctx->server, ctx->pn_conn, config);
    return ctx;
}


static void on_accept(pn_event_t *e)
{
    assert(pn_event_type(e) == PN_LISTENER_ACCEPT);
    pn_listener_t *pn_listener = pn_event_listener(e);
    qd_listener_t *listener = pn_listener_get_context(pn_listener);
    qd_connection_t *ctx = qd_server_connection(listener->server, &listener->config);
    if (!ctx) {
        qd_log(listener->server->log_source, QD_LOG_CRITICAL,
               "Allocation failure during accept to %s", listener->config.host_port);
        return;
    }
    ctx->listener = listener;
    qd_log(listener->server->log_source, QD_LOG_TRACE,
           "[%"PRIu64"] Accepting incoming connection from %s to %s",
           ctx->connection_id, qd_connection_name(ctx), ctx->listener->config.host_port);
    /* Asynchronous accept, configure the transport on PN_CONNECTION_BOUND */
    pn_listener_accept(pn_listener, ctx->pn_conn);
 }


/* Log the description, set the transport condition (name, description) close the transport tail. */
void connect_fail(qd_connection_t *ctx, const char *name, const char *description, ...)
{
    va_list ap;
    va_start(ap, description);
    qd_verror(QD_ERROR_RUNTIME, description, ap);
    va_end(ap);
    if (ctx->pn_conn) {
        pn_transport_t *t = pn_connection_transport(ctx->pn_conn);
        /* Normally this is closing the transport but if not bound close the connection. */
        pn_condition_t *cond = t ? pn_transport_condition(t) : pn_connection_condition(ctx->pn_conn);
        if (cond && !pn_condition_is_set(cond)) {
            va_start(ap, description);
            pn_condition_vformat(cond, name, description, ap);
            va_end(ap);
        }
        if (t) {
            pn_transport_close_tail(t);
        } else {
            pn_connection_close(ctx->pn_conn);
        }
    }
}


/* Get the host IP address for the remote end */
static void set_rhost_port(qd_connection_t *ctx) {
    pn_transport_t *tport  = pn_connection_transport(ctx->pn_conn);
    const struct sockaddr* sa = pn_netaddr_sockaddr(pn_netaddr_remote(tport));
    size_t salen = pn_netaddr_socklen(pn_netaddr_remote(tport));
    if (sa && salen) {
        char rport[NI_MAXSERV] = "";
        int err = getnameinfo(sa, salen,
                              ctx->rhost, sizeof(ctx->rhost), rport, sizeof(rport),
                              NI_NUMERICHOST | NI_NUMERICSERV);
        if (!err) {
            snprintf(ctx->rhost_port, sizeof(ctx->rhost_port), "%s:%s", ctx->rhost, rport);
        }
    }
}


/* Configure the transport once it is bound to the connection */
static void on_connection_bound(qd_server_t *server, pn_event_t *e) {
    pn_connection_t *pn_conn = pn_event_connection(e);
    qd_connection_t *ctx = pn_connection_get_context(pn_conn);
    pn_transport_t *tport  = pn_connection_transport(pn_conn);
    pn_transport_set_context(tport, ctx); /* for transport_tracer */

    //
    // Proton pushes out its trace to transport_tracer() which in turn writes a trace
    // message to the qdrouter log If trace level logging is enabled on the router set
    //
    if (qd_log_enabled(ctx->server->log_source, QD_LOG_TRACE)) {
        pn_transport_trace(tport, PN_TRACE_FRM);
        pn_transport_set_tracer(tport, transport_tracer);
    }

    const qd_server_config_t *config = NULL;
    if (ctx->listener) {        /* Accepting an incoming connection */
        config = &ctx->listener->config;
        const char *name = config->host_port;
        pn_transport_set_server(tport);
        set_rhost_port(ctx);

        sys_mutex_lock(server->lock); /* Policy check is not thread safe */
        ctx->policy_counted = qd_policy_socket_accept(server->qd->policy, ctx->rhost);
        sys_mutex_unlock(server->lock);
        if (!ctx->policy_counted) {
            pn_transport_close_tail(tport);
            pn_transport_close_head(tport);
            return;
        }

        // Set up SSL
        if (config->ssl_profile)  {
            qd_log(ctx->server->log_source, QD_LOG_TRACE, "Configuring SSL on %s", name);
            if (listener_setup_ssl(ctx, config, tport) != QD_ERROR_NONE) {
                connect_fail(ctx, QD_AMQP_COND_INTERNAL_ERROR, "%s on %s", qd_error_message(), name);
                return;
            }
        }
        //
        // Set up SASL
        //
        sys_mutex_lock(ctx->server->lock);
        pn_sasl_t *sasl = pn_sasl(tport);
        if (ctx->server->sasl_config_path)
            pn_sasl_config_path(sasl, ctx->server->sasl_config_path);
        pn_sasl_config_name(sasl, ctx->server->sasl_config_name);
        if (config->sasl_mechanisms)
            pn_sasl_allowed_mechs(sasl, config->sasl_mechanisms);
        if (config->auth_service) {
            qd_log(server->log_source, QD_LOG_INFO, "enabling remote authentication service %s", config->auth_service);
            qdr_use_remote_authentication_service(tport, config->auth_service, config->sasl_init_hostname, config->auth_ssl_conf);
        }
        pn_transport_require_auth(tport, config->requireAuthentication);
        pn_transport_require_encryption(tport, config->requireEncryption);
        pn_sasl_set_allow_insecure_mechs(sasl, config->allowInsecureAuthentication);
        sys_mutex_unlock(ctx->server->lock);

        qd_log(ctx->server->log_source, QD_LOG_INFO, "Accepted connection to %s from %s",
               name, ctx->rhost_port);
    } else if (ctx->connector) { /* Establishing an outgoing connection */
        config = &ctx->connector->config;
        setup_ssl_sasl_and_open(ctx);
    } else {                    /* No connector and no listener */
        connect_fail(ctx, QD_AMQP_COND_INTERNAL_ERROR, "unknown Connection");
        return;
    }

    //
    // Common transport configuration.
    //
    pn_transport_set_max_frame(tport, config->max_frame_size);
    pn_transport_set_channel_max(tport, config->max_sessions - 1);
    pn_transport_set_idle_timeout(tport, config->idle_timeout_seconds * 1000);
}


static void invoke_deferred_calls(qd_connection_t *conn, bool discard)
{
    if (!conn)
        return;

    // Lock access to deferred_calls, other threads may concurrently add to it.  Invoke
    // the calls outside of the critical section.
    //
    sys_mutex_lock(conn->deferred_call_lock);
    qd_deferred_call_t *dc;
    while ((dc = DEQ_HEAD(conn->deferred_calls))) {
        DEQ_REMOVE_HEAD(conn->deferred_calls);
        sys_mutex_unlock(conn->deferred_call_lock);
        dc->call(dc->context, discard);
        free_qd_deferred_call_t(dc);
        sys_mutex_lock(conn->deferred_call_lock);
    }
    sys_mutex_unlock(conn->deferred_call_lock);
}

void qd_container_handle_event(qd_container_t *container, pn_event_t *event);

static void handle_listener(pn_event_t *e, qd_server_t *qd_server) {
    qd_log_source_t *log = qd_server->log_source;
    qd_listener_t *li = (qd_listener_t*) pn_listener_get_context(pn_event_listener(e));
    const char *host_port = li->config.host_port;

    switch (pn_event_type(e)) {

    case PN_LISTENER_OPEN:
        qd_log(log, QD_LOG_NOTICE, "Listening on %s", host_port);
        break;

    case PN_LISTENER_ACCEPT:
        qd_log(log, QD_LOG_TRACE, "Accepting connection on %s", host_port);
        on_accept(e);
        break;

    case PN_LISTENER_CLOSE: {
        pn_condition_t *cond = pn_listener_condition(li->pn_listener);
        if (pn_condition_is_set(cond)) {
            qd_log(log, QD_LOG_ERROR, "Listener error on %s: %s (%s)", host_port,
                   pn_condition_get_description(cond),
                   pn_condition_get_name(cond));
            if (li->exit_on_error) {
                qd_log(log, QD_LOG_CRITICAL, "Shutting down, required listener failed %s",
                       host_port);
                exit(1);
            }
        } else {
            qd_log(log, QD_LOG_TRACE, "Listener closed on %s", host_port);
        }
        qd_listener_decref(li);
        break;
    }
    default:
        break;
    }
}


bool qd_connector_has_failover_info(qd_connector_t* ct)
{
    if (ct && DEQ_SIZE(ct->conn_info_list) > 1)
        return true;
    return false;
}


void qd_connection_free(qd_connection_t *ctx)
{
    qd_server_t *qd_server = ctx->server;

    qd_entity_cache_remove(QD_CONNECTION_TYPE, ctx); /* Removed management entity */

    // If this is a dispatch connector, schedule the re-connect timer
    if (ctx->connector) {
        sys_mutex_lock(ctx->connector->lock);
        ctx->connector->ctx = 0;
        // Increment the connection index by so that we can try connecting to the failover url (if any).
        bool has_failover = qd_connector_has_failover_info(ctx->connector);
        if (has_failover) {
            if (DEQ_SIZE(ctx->connector->conn_info_list) == ctx->connector->conn_index)
                // Start round robin again
                ctx->connector->conn_index = 1;
            else
                ctx->connector->conn_index += 1;

            // Go thru the failover list round robin.
            // IMPORTANT: Note here that we set the re-try timer to 1 second.
            // We want to quickly keep cycling thru the failover urls every second.
            qd_timer_schedule(ctx->connector->timer, 1000);
        }

        ctx->connector->state = CXTR_STATE_CONNECTING;
        sys_mutex_unlock(ctx->connector->lock);
        if (!has_failover)
            qd_timer_schedule(ctx->connector->timer, ctx->connector->delay);
    }

    sys_mutex_lock(qd_server->lock);
    DEQ_REMOVE(qd_server->conn_list, ctx);

    // If counted for policy enforcement, notify it has closed
    if (ctx->policy_counted) {
        qd_policy_socket_close(qd_server->qd->policy, ctx);
    }
    sys_mutex_unlock(qd_server->lock);

    invoke_deferred_calls(ctx, true);  // Discard any pending deferred calls
    sys_mutex_free(ctx->deferred_call_lock);

    if (ctx->policy_settings) {
        if (ctx->policy_settings->sources)
            free(ctx->policy_settings->sources);
        if (ctx->policy_settings->targets)
            free(ctx->policy_settings->targets);
        free (ctx->policy_settings);
        ctx->policy_settings = 0;
    }

    if (ctx->free_user_id) free((char*)ctx->user_id);
    if (ctx->timer) qd_timer_free(ctx->timer);
    free(ctx->name);
    free(ctx->role);
    free_qd_connection_t(ctx);

    /* Note: pn_conn is freed by the proactor */
}


static void timeout_on_handhsake(void *context, bool discard)
{
    if (discard)
        return;

    qd_connection_t *ctx   = (qd_connection_t*) context;
    pn_transport_t  *tport = pn_connection_transport(ctx->pn_conn);
    pn_transport_close_head(tport);
    connect_fail(ctx, QD_AMQP_COND_NOT_ALLOWED, "Timeout waiting for initial handshake");
}


static void startup_timer_handler(void *context)
{
    //
    // This timer fires for a connection if it has not had a REMOTE_OPEN
    // event in a time interval from the CONNECTION_INIT event.  Close
    // down the transport in an IO thread reserved for that connection.
    //
    qd_connection_t *ctx = (qd_connection_t*) context;
    qd_timer_free(ctx->timer);
    ctx->timer = 0;
    qd_connection_invoke_deferred(ctx, timeout_on_handhsake, context);
}


/* Events involving a connection or listener are serialized by the proactor so
 * only one event per connection / listener will be processed at a time.
 */
static bool handle(qd_server_t *qd_server, pn_event_t *e) {
    pn_connection_t *pn_conn = pn_event_connection(e);
    if (pn_conn && qdr_is_authentication_service_connection(pn_conn)) {
        qdr_handle_authentication_service_connection_event(e);
        return true;
    }
    qd_connection_t *ctx = pn_conn ? (qd_connection_t*) pn_connection_get_context(pn_conn) : NULL;

    switch (pn_event_type(e)) {

    case PN_PROACTOR_INTERRUPT:
        /* Interrupt the next thread */
        pn_proactor_interrupt(qd_server->proactor);
        /* Stop the current thread */
        return false;

    case PN_PROACTOR_TIMEOUT:
        qd_timer_visit();
        break;

    case PN_LISTENER_OPEN:
    case PN_LISTENER_ACCEPT:
    case PN_LISTENER_CLOSE:
        handle_listener(e, qd_server);
        break;

    case PN_CONNECTION_INIT: {
        const qd_server_config_t *config = ctx && ctx->listener ? &ctx->listener->config : 0;
        if (config && config->initial_handshake_timeout_seconds > 0) {
            ctx->timer = qd_timer(qd_server->qd, startup_timer_handler, ctx);
            qd_timer_schedule(ctx->timer, config->initial_handshake_timeout_seconds * 1000);
        }
        break;
    }
        
    case PN_CONNECTION_BOUND:
        on_connection_bound(qd_server, e);
        break;

    case PN_CONNECTION_REMOTE_OPEN:
        // If we are transitioning to the open state, notify the client via callback.
        if (ctx && ctx->timer) {
            qd_timer_free(ctx->timer);
            ctx->timer = 0;
        }
        if (ctx && !ctx->opened) {
            ctx->opened = true;
            if (ctx->connector) {
                ctx->connector->delay = 2000;  // Delay re-connect in case there is a recurring error
            }
        }
        break;

    case PN_CONNECTION_WAKE:
        invoke_deferred_calls(ctx, false);
        break;

    case PN_TRANSPORT_ERROR:
        {
        pn_transport_t *transport = pn_event_transport(e);
        pn_condition_t* condition = transport ? pn_transport_condition(transport) : NULL;
        if (ctx && ctx->connector) { /* Outgoing connection */
            const qd_server_config_t *config = &ctx->connector->config;
            if (condition  && pn_condition_is_set(condition)) {
                qd_log(qd_server->log_source, QD_LOG_INFO, "Connection to %s failed: %s %s", config->host_port,
                       pn_condition_get_name(condition), pn_condition_get_description(condition));
            } else {
                qd_log(qd_server->log_source, QD_LOG_INFO, "Connection to %s failed", config->host_port);
            }
        } else if (ctx && ctx->listener) { /* Incoming connection */
            if (condition && pn_condition_is_set(condition)) {
                qd_log(ctx->server->log_source, QD_LOG_INFO, "Connection from %s (to %s) failed: %s %s",
                       ctx->rhost_port, ctx->listener->config.host_port, pn_condition_get_name(condition),
                       pn_condition_get_description(condition));
            }
        }
        }
        break;

    default:
        break;
    } // Switch event type

    /* TODO aconway 2017-04-18: fold the container handler into the server */
    qd_container_handle_event(qd_server->container, e);

    /* Free the connection after all other processing is complete */
    if (ctx && pn_event_type(e) == PN_TRANSPORT_CLOSED) {
        pn_connection_set_context(pn_conn, NULL);
        qd_connection_free(ctx);
    }
    return true;
}

static void *thread_run(void *arg)
{
    qd_server_t      *qd_server = (qd_server_t*)arg;
    bool running = true;
    while (running) {
        pn_event_batch_t *events = pn_proactor_wait(qd_server->proactor);
        pn_event_t * e;
        while (running && (e = pn_event_batch_next(events))) {
            running = handle(qd_server, e);
        }
        pn_proactor_done(qd_server->proactor, events);
    }
    return NULL;
}


qd_failover_item_t *qd_connector_get_conn_info(qd_connector_t *ct) {

    qd_failover_item_t *item = DEQ_HEAD(ct->conn_info_list);

    if (DEQ_SIZE(ct->conn_info_list) > 1) {
        for (int i=1; i < ct->conn_index; i++) {
            item = DEQ_NEXT(item);
        }
    }
    return item;
}


/* Timer callback to try/retry connection open */
static void try_open_lh(qd_connector_t *ct)
{
    if (ct->state != CXTR_STATE_CONNECTING) {
        /* No longer referenced by pn_connection or timer */
        qd_connector_decref(ct);
        return;
    }

    qd_connection_t *ctx = qd_server_connection(ct->server, &ct->config);
    if (!ctx) {                 /* Try again later */
        qd_log(ct->server->log_source, QD_LOG_CRITICAL, "Allocation failure connecting to %s",
               ct->config.host_port);
        ct->delay = 10000;
        qd_timer_schedule(ct->timer, ct->delay);
        return;
    }
    ctx->connector    = ct;
    const qd_server_config_t *config = &ct->config;

    //
    // Set the hostname on the pn_connection. This hostname will be used by proton as the
    // hostname in the open frame.
    //

    qd_failover_item_t *item = qd_connector_get_conn_info(ct);

    char *current_host = item->host;
    char *host_port = item->host_port;

    pn_connection_set_hostname(ctx->pn_conn, current_host);

    // Set the sasl user name and password on the proton connection object. This has to be
    // done before pn_proactor_connect which will bind a transport to the connection
    if(config->sasl_username)
        pn_connection_set_user(ctx->pn_conn, config->sasl_username);
    if (config->sasl_password)
        pn_connection_set_password(ctx->pn_conn, config->sasl_password);

    ctx->connector->state = CXTR_STATE_OPEN;
    ct->ctx   = ctx;
    ct->delay = 5000;

    qd_log(ct->server->log_source, QD_LOG_TRACE,
           "[%"PRIu64"] Connecting to %s", ctx->connection_id, host_port);
    /* Note: the transport is configured in the PN_CONNECTION_BOUND event */
    pn_proactor_connect(ct->server->proactor, ctx->pn_conn, host_port);
}

static void setup_ssl_sasl_and_open(qd_connection_t *ctx)
{
    qd_connector_t *ct = ctx->connector;
    const qd_server_config_t *config = &ct->config;
    pn_transport_t *tport  = pn_connection_transport(ctx->pn_conn);

    //
    // Set up SSL if appropriate
    //
    if (config->ssl_profile) {
        pn_ssl_domain_t *domain = pn_ssl_domain(PN_SSL_MODE_CLIENT);

        if (!domain) {
            qd_error(QD_ERROR_RUNTIME, "SSL domain failed for connection to %s:%s",
                     ct->config.host, ct->config.port);
            return;
        }

        // set our trusted database for checking the peer's cert:
        if (config->ssl_trusted_certificate_db) {
            if (pn_ssl_domain_set_trusted_ca_db(domain, config->ssl_trusted_certificate_db)) {
                qd_log(ct->server->log_source, QD_LOG_ERROR,
                       "SSL CA configuration failed for %s:%s",
                       ct->config.host, ct->config.port);
            }
        }
        // should we force the peer to provide a cert?
        if (config->ssl_require_peer_authentication) {
            const char *trusted = (config->ssl_trusted_certificates)
                ? config->ssl_trusted_certificates
                : config->ssl_trusted_certificate_db;
            if (pn_ssl_domain_set_peer_authentication(domain,
                                                      PN_SSL_VERIFY_PEER,
                                                      trusted)) {
                qd_log(ct->server->log_source, QD_LOG_ERROR,
                       "SSL peer auth configuration failed for %s:%s",
                       config->host, config->port);
            }
        }

        // configure our certificate if the peer requests one:
        if (config->ssl_certificate_file) {
            if (pn_ssl_domain_set_credentials(domain,
                                              config->ssl_certificate_file,
                                              config->ssl_private_key_file,
                                              config->ssl_password)) {
                qd_log(ct->server->log_source, QD_LOG_ERROR,
                       "SSL local configuration failed for %s:%s",
                       config->host, config->port);
            }
        }

        if (config->ciphers) {
            if (pn_ssl_domain_set_ciphers(domain, config->ciphers)) {
                qd_log(ct->server->log_source, QD_LOG_ERROR,
                       "SSL cipher configuration failed for %s:%s",
                       config->host, config->port);
            }
        }

        //If ssl is enabled and verify_host_name is true, instruct proton to verify peer name
        if (config->verify_host_name) {
            if (pn_ssl_domain_set_peer_authentication(domain, PN_SSL_VERIFY_PEER_NAME, NULL)) {
                    qd_log(ct->server->log_source, QD_LOG_ERROR,
                           "SSL peer host name verification failed for %s:%s",
                           config->host, config->port);
            }
        }

        ctx->ssl = pn_ssl(tport);
        pn_ssl_init(ctx->ssl, domain, 0);
        pn_ssl_domain_free(domain);
    }

    //
    // Set up SASL
    //
    sys_mutex_lock(ct->server->lock);
    pn_sasl_t *sasl = pn_sasl(tport);
    if (config->sasl_mechanisms)
        pn_sasl_allowed_mechs(sasl, config->sasl_mechanisms);
    pn_sasl_set_allow_insecure_mechs(sasl, config->allowInsecureAuthentication);
    sys_mutex_unlock(ct->server->lock);

    pn_connection_open(ctx->pn_conn);
}

static void try_open_cb(void *context) {
    qd_connector_t *ct = (qd_connector_t*) context;
    sys_mutex_lock(ct->lock);   /* TODO aconway 2017-05-09: this lock looks too big */
    try_open_lh(ct);
    sys_mutex_unlock(ct->lock);
}


qd_server_t *qd_server(qd_dispatch_t *qd, int thread_count, const char *container_name,
                       const char *sasl_config_path, const char *sasl_config_name)
{
    /* Initialize const members, 0 initialize all others. */
    qd_server_t tmp = { .thread_count = thread_count };
    qd_server_t *qd_server = NEW(qd_server_t);
    if (qd_server == 0)
        return 0;
    memcpy(qd_server, &tmp, sizeof(tmp));

    qd_server->qd               = qd;
    qd_server->log_source       = qd_log_source("SERVER");
    qd_server->container_name   = container_name;
    qd_server->sasl_config_path = sasl_config_path;
    qd_server->sasl_config_name = sasl_config_name;
    qd_server->proactor         = pn_proactor();
    qd_server->container        = 0;
    qd_server->start_context    = 0;
    qd_server->lock             = sys_mutex();
    qd_server->cond             = sys_cond();
    DEQ_INIT(qd_server->conn_list);

    qd_timer_initialize(qd_server->lock);

    qd_server->pause_requests         = 0;
    qd_server->threads_paused         = 0;
    qd_server->pause_next_sequence    = 0;
    qd_server->pause_now_serving      = 0;
    qd_server->next_connection_id     = 1;
    qd_server->py_displayname_obj     = 0;

    qd_server->http = qd_http_server(qd_server, qd_server->log_source);

    qd_log(qd_server->log_source, QD_LOG_INFO, "Container Name: %s", qd_server->container_name);

    return qd_server;
}


void qd_server_free(qd_server_t *qd_server)
{
    if (!qd_server) return;
    qd_connection_t *ctx = DEQ_HEAD(qd_server->conn_list);
    while (ctx) {
        DEQ_REMOVE_HEAD(qd_server->conn_list);
        if (ctx->free_user_id) free((char*)ctx->user_id);
        sys_mutex_free(ctx->deferred_call_lock);
        free(ctx->name);
        free(ctx->role);
        free_qd_connection_t(ctx);
        ctx = DEQ_HEAD(qd_server->conn_list);
    }
    qd_http_server_free(qd_server->http);
    qd_timer_finalize();
    pn_proactor_free(qd_server->proactor);
    sys_mutex_free(qd_server->lock);
    sys_cond_free(qd_server->cond);
    Py_XDECREF((PyObject *)qd_server->py_displayname_obj);
    free(qd_server);
}

void qd_server_set_container(qd_dispatch_t *qd, qd_container_t *container)
{
    qd->server->container              = container;
}


void qd_server_run(qd_dispatch_t *qd)
{
    qd_server_t *qd_server = qd->server;
    int i;
    assert(qd_server);
    assert(qd_server->container); // Server can't run without a container
    qd_log(qd_server->log_source,
           QD_LOG_NOTICE, "Operational, %d Threads Running (process ID %ld)",
           qd_server->thread_count, (long)getpid());
#ifndef NDEBUG
    qd_log(qd_server->log_source, QD_LOG_INFO, "Running in DEBUG Mode");
#endif
    int n = qd_server->thread_count - 1; /* Start count-1 threads + use current thread */
    sys_thread_t **threads = (sys_thread_t **)calloc(n, sizeof(sys_thread_t*));
    for (i = 0; i < n; i++) {
        threads[i] = sys_thread(thread_run, qd_server);
    }
    thread_run(qd_server);      /* Use the current thread */
    for (i = 0; i < n; i++) {
        sys_thread_join(threads[i]);
        sys_thread_free(threads[i]);
    }
    free(threads);

    qd_log(qd_server->log_source, QD_LOG_NOTICE, "Shut Down");
}


void qd_server_stop(qd_dispatch_t *qd)
{
    /* Interrupt the proactor, async-signal-safe */
    pn_proactor_interrupt(qd->server->proactor);
}

void qd_server_activate(qd_connection_t *ctx)
{
    if (ctx) ctx->wake(ctx);
}


void qd_connection_set_context(qd_connection_t *conn, void *context)
{
    conn->user_context = context;
}


void *qd_connection_get_context(qd_connection_t *conn)
{
    return conn->user_context;
}


void *qd_connection_get_config_context(qd_connection_t *conn)
{
    return conn->context;
}


void qd_connection_set_link_context(qd_connection_t *conn, void *context)
{
    conn->link_context = context;
}


void *qd_connection_get_link_context(qd_connection_t *conn)
{
    return conn->link_context;
}


pn_connection_t *qd_connection_pn(qd_connection_t *conn)
{
    return conn->pn_conn;
}


bool qd_connection_inbound(qd_connection_t *conn)
{
    return conn->listener != 0;
}


uint64_t qd_connection_connection_id(qd_connection_t *conn)
{
    return conn->connection_id;
}


const qd_server_config_t *qd_connection_config(const qd_connection_t *conn)
{
    if (conn->listener)
        return &conn->listener->config;
    return &conn->connector->config;
}


void qd_connection_invoke_deferred(qd_connection_t *conn, qd_deferred_t call, void *context)
{
    if (!conn)
        return;

    qd_deferred_call_t *dc = new_qd_deferred_call_t();
    DEQ_ITEM_INIT(dc);
    dc->call    = call;
    dc->context = context;

    sys_mutex_lock(conn->deferred_call_lock);
    DEQ_INSERT_TAIL(conn->deferred_calls, dc);
    sys_mutex_unlock(conn->deferred_call_lock);

    qd_server_activate(conn);
}


qd_listener_t *qd_server_listener(qd_server_t *server)
{
    qd_listener_t *li = new_qd_listener_t();
    if (!li) return 0;
    ZERO(li);
    sys_atomic_init(&li->ref_count, 1);
    li->server      = server;
    li->http = NULL;
    return li;
}

static bool qd_listener_listen_pn(qd_listener_t *li) {
   li->pn_listener = pn_listener();
    if (li->pn_listener) {
        pn_listener_set_context(li->pn_listener, li);
        pn_proactor_listen(li->server->proactor, li->pn_listener, li->config.host_port,
                           BACKLOG);
        sys_atomic_inc(&li->ref_count); /* In use by proactor, PN_LISTENER_CLOSE will dec */
        /* Listen is asynchronous, log "listening" message on PN_LISTENER_OPEN event */
    } else {
        qd_log(li->server->log_source, QD_LOG_CRITICAL, "No memory listening on %s",
               li->config.host_port);
     }
    return li->pn_listener;
}

static bool qd_listener_listen_http(qd_listener_t *li) {
    if (li->server->http) {
        /* qd_http_listener holds a reference to li, will decref when closed */
        qd_http_server_listen(li->server->http, li);
        return li->http;
    } else {
        qd_log(li->server->log_source, QD_LOG_ERROR, "No HTTP support to listen on %s",
               li->config.host_port);
        return false;
    }
}


bool qd_listener_listen(qd_listener_t *li) {
    if (li->pn_listener || li->http) /* Already listening */
        return true;
    return li->config.http ? qd_listener_listen_http(li) : qd_listener_listen_pn(li);
}


void qd_listener_decref(qd_listener_t* li)
{
    if (li && sys_atomic_dec(&li->ref_count) == 1) {
        qd_server_config_free(&li->config);
        free_qd_listener_t(li);
    }
}


qd_connector_t *qd_server_connector(qd_server_t *server)
{
    qd_connector_t *ct        = new_qd_connector_t();
    if (!ct) return 0;
    sys_atomic_init(&ct->ref_count, 1);
    ct->server  = server;
    qd_failover_item_list_t conn_info_list;
    DEQ_INIT(conn_info_list);
    ct->conn_info_list = conn_info_list;
    ct->conn_index = 0;
    ct->lock = sys_mutex();
    ct->timer = qd_timer(ct->server->qd, try_open_cb, ct);
    if (!ct->lock || !ct->timer) {
        qd_connector_decref(ct);
        return 0;
    }
    return ct;
}


bool qd_connector_connect(qd_connector_t *ct)
{
    sys_mutex_lock(ct->lock);
    ct->state   = CXTR_STATE_CONNECTING;
    ct->ctx     = 0;
    ct->delay   = 0;
    /* Referenced by timer */
    sys_atomic_inc(&ct->ref_count);
    qd_timer_schedule(ct->timer, ct->delay);
    sys_mutex_unlock(ct->lock);
    return true;
}


void qd_connector_decref(qd_connector_t* ct)
{
    if (ct && sys_atomic_dec(&ct->ref_count) == 1) {
        sys_mutex_lock(ct->lock);
        if (ct->ctx) {
            ct->ctx->connector = 0;
        }
        sys_mutex_unlock(ct->lock);
        qd_server_config_free(&ct->config);
        qd_timer_free(ct->timer);

        qd_failover_item_t *item = DEQ_HEAD(ct->conn_info_list);
        while (item) {
            DEQ_REMOVE_HEAD(ct->conn_info_list);
            free(item->scheme);
            free(item->host);
            free(item->port);
            free(item->hostname);
            free(item->host_port);
            free(item);
            item = DEQ_HEAD(ct->conn_info_list);
        }
        free_qd_connector_t(ct);
    }
}

void qd_server_timeout(qd_server_t *server, qd_duration_t duration) {
    pn_proactor_set_timeout(server->proactor, duration);
}

qd_dispatch_t* qd_server_dispatch(qd_server_t *server) { return server->qd; }

const char* qd_connection_name(const qd_connection_t *c) {
    if (c->connector) {
        return c->connector->config.host_port;
    } else {
        return c->rhost_port;
    }
}

qd_connector_t* qd_connection_connector(const qd_connection_t *c) {
    return c->connector;
}

const qd_server_config_t *qd_connector_config(const qd_connector_t *c) {
    return &c->config;
}

qd_http_listener_t *qd_listener_http(qd_listener_t *li) {
    return li->http;
}

const char* qd_connection_remote_ip(const qd_connection_t *c) {
    return c->rhost;
}

/* Expose event handling for HTTP connections */
void qd_connection_handle(qd_connection_t *c, pn_event_t *e) {
    handle(c->server, e);
}

bool qd_connection_strip_annotations_in(const qd_connection_t *c) {
    return c->strip_annotations_in;
}

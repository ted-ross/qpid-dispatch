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

#include "log_private.h"
#include "entity.h"
#include "aprintf.h"
#include <qpid/dispatch/ctools.h>
#include <qpid/dispatch/dispatch.h>
#include <qpid/dispatch/alloc.h>
#include <qpid/dispatch/threading.h>
#include <qpid/dispatch/log.h>
#include <stdarg.h>
#include <stdio.h>
#include <string.h>
#include <time.h>
#include <syslog.h>

#define TEXT_MAX QD_LOG_TEXT_MAX
#define LOG_MAX (QD_LOG_TEXT_MAX+128)
#define LIST_MAX 1000

static qd_log_source_t      *default_log_source=0;
static qd_log_source_t      *logging_log_source=0;

int qd_log_max_len() { return TEXT_MAX; }

typedef struct qd_log_entry_t qd_log_entry_t;

struct qd_log_entry_t {
    DEQ_LINKS(qd_log_entry_t);
    const char     *module;
    int             level;
    char           *file;
    int             line;
    time_t          time;
    char            text[TEXT_MAX];
};

ALLOC_DECLARE(qd_log_entry_t);
ALLOC_DEFINE(qd_log_entry_t);

DEQ_DECLARE(qd_log_entry_t, qd_log_list_t);
static qd_log_list_t         entries = {0};

static void qd_log_entry_free_lh(qd_log_entry_t* entry) {
    DEQ_REMOVE(entries, entry);
    free(entry->file);
    free_qd_log_entry_t(entry);
}

// Ref-counted log sink, may be shared by several sources.
typedef struct log_sink_t {
    int refcount;
    char *name;
    bool syslog;
    FILE *file;
    DEQ_LINKS(struct log_sink_t);
} log_sink_t;

DEQ_DECLARE(log_sink_t, log_sink_list_t);

static log_sink_list_t sink_list = {0};

static const char* SINK_STDERR = "stderr";
static const char* SINK_SYSLOG = "syslog";
static const char* SOURCE_DEFAULT = "DEFAULT";
static const char* SOURCE_LOGGING = "LOGGING";

static log_sink_t* find_log_sink_lh(const char* name) {
    log_sink_t* sink = DEQ_HEAD(sink_list);
    DEQ_FIND(sink, strcmp(sink->name, name) == 0);
    return sink;
}

static log_sink_t* log_sink_lh(const char* name) {
    log_sink_t* sink = find_log_sink_lh(name);
    if (sink)
        sink->refcount++;
    else {
        sink = NEW(log_sink_t);
        *sink = (log_sink_t){ 1, strdup(name), };
        if (strcmp(name, SINK_STDERR) == 0) {
            sink->file = stderr;
        }
        else if (strcmp(name, SINK_SYSLOG) == 0) {
            openlog(0, 0, LOG_DAEMON);
            sink->syslog = true;
        }
        else {
            sink->file = fopen(name, "w");
            if (!sink->file) {
                char msg[TEXT_MAX];
                snprintf(msg, sizeof(msg), "Failed to open log file '%s'", name);
                perror(msg);
                exit(1);
            }
        }
        DEQ_INSERT_TAIL(sink_list, sink);
    }
    return sink;
}

// Must hold the log_source_lock
static void log_sink_free_lh(log_sink_t* sink) {
    if (!sink) return;
    assert(sink->refcount);
    if (--sink->refcount == 0) {
        DEQ_REMOVE(sink_list, sink);
        free(sink->name);
        if (sink->file && sink->file != stderr)
            fclose(sink->file);
        if (sink->syslog) closelog();
        free(sink);
    }
}

struct qd_log_source_t {
    DEQ_LINKS(qd_log_source_t);
    const char *module;
    int mask;
    int timestamp;              /* boolean or -1 means not set */
    int source;                 /* boolean or -1 means not set */
    bool syslog;
    log_sink_t *sink;
};

DEQ_DECLARE(qd_log_source_t, qd_log_source_list_t);

static sys_mutex_t          *log_lock = 0;
static sys_mutex_t          *log_source_lock = 0;
static qd_log_source_list_t  source_list = {0};

typedef enum {NONE, TRACE, DEBUG, INFO, NOTICE, WARNING, ERROR, CRITICAL, N_LEVELS} level_index_t;
typedef struct level {
    const char* name;
    int bit;     // QD_LOG bit
    int mask;    // Bit or higher
    const int syslog;
} level;
#define ALL_BITS (QD_LOG_CRITICAL | (QD_LOG_CRITICAL-1))
#define LEVEL(name, QD_LOG, SYSLOG) { name, QD_LOG,  ALL_BITS & ~(QD_LOG-1), SYSLOG }
static level levels[] = {
    {"none", 0, -1, 0},
    LEVEL("trace",    QD_LOG_TRACE, LOG_DEBUG), /* syslog has no trace level */
    LEVEL("debug",    QD_LOG_DEBUG, LOG_DEBUG),
    LEVEL("info",     QD_LOG_INFO, LOG_INFO),
    LEVEL("notice",   QD_LOG_NOTICE, LOG_NOTICE),
    LEVEL("warning",  QD_LOG_WARNING, LOG_WARNING),
    LEVEL("error",    QD_LOG_ERROR, LOG_ERR),
    LEVEL("critical", QD_LOG_CRITICAL, LOG_CRIT)
};
static const char level_names[TEXT_MAX]; /* Set up in qd_log_initialize */

static const level* level_for_bit(int bit) {
    level_index_t i = 0;
    while (i < N_LEVELS && levels[i].bit != bit) ++i;
    if (i == N_LEVELS) {
        qd_log(logging_log_source, QD_LOG_ERROR, "'%d' is not a valid log level bit. Defaulting to %s", bit, levels[INFO].name);
        i = INFO;
        assert(0);
    }
    return &levels[i];
}

static const level* level_for_name(const char *name) {
    level_index_t i = 0;
    while (i < N_LEVELS && strcasecmp(levels[i].name, name) != 0) ++i;
    if (i == N_LEVELS) {
        qd_log(logging_log_source, QD_LOG_ERROR, "'%s' is not a valid log level. Should be one of {%s}. Defaulting to %s", name, level_names, levels[INFO].name);
        i = INFO;
    }
    return &levels[i];
}

/// Caller must hold log_source_lock
static qd_log_source_t* lookup_log_source_lh(const char *module)
{
    if (strcasecmp(module, SOURCE_DEFAULT) == 0)
        return default_log_source;
    qd_log_source_t *src = DEQ_HEAD(source_list);
    DEQ_FIND(src, strcasecmp(module, src->module) == 0);
    return src;
}

static bool default_bool(int value, int default_value) {
    return value == -1 ? default_value : value;
}

static void write_log(qd_log_source_t *log_source, qd_log_entry_t *entry)
{
    log_sink_t* sink = log_source->sink ? log_source->sink : default_log_source->sink;
    if (!sink) return;

    char log_str[LOG_MAX];
    char *begin = log_str;
    char *end = log_str + LOG_MAX;

    if (default_bool(log_source->timestamp, default_log_source->timestamp)) {
        char buf[100];
        buf[0] = '\0';
        ctime_r(&entry->time, buf);
        buf[strlen(buf)-1] = '\0'; /* Get rid of trailng \n */
        aprintf(&begin, end, "%s ", buf);
    }
    aprintf(&begin, end, "%s (%s) %s", entry->module, level_for_bit(entry->level)->name, entry->text);
    if (default_bool(log_source->source, default_log_source->source))
        aprintf(&begin, end, " (%s:%d)", entry->file, entry->line);
    aprintf(&begin, end, "\n");

    if (sink->file) {
        if (fputs(log_str, sink->file) == EOF) {
            char msg[TEXT_MAX];
            snprintf(msg, sizeof(msg), "Cannot write log output to '%s'", sink->name);
            perror(msg);
            exit(1);
        };
        fflush(sink->file);
    }
    if (sink->syslog) {
        int syslog_level = level_for_bit(entry->level)->syslog;
        if (syslog_level != -1)
            syslog(syslog_level, "%s", log_str);
    }
}

/// Reset the log source to the default state
static void qd_log_source_defaults(qd_log_source_t *log_source) {
    log_source->mask = -1;
    log_source->timestamp = -1;
    log_source->source = -1;
    log_source->sink = 0;
}

/// Caller must hold the log_source_lock
static qd_log_source_t *qd_log_source_lh(const char *module)
{
    qd_log_source_t *log_source = lookup_log_source_lh(module);
    if (!log_source)
    {
        log_source = NEW(qd_log_source_t);
        memset(log_source, 0, sizeof(qd_log_source_t));
        DEQ_ITEM_INIT(log_source);
        log_source->module = module;
        qd_log_source_defaults(log_source);
        DEQ_INSERT_TAIL(source_list, log_source);
    }
    return log_source;
}

qd_log_source_t *qd_log_source(const char *module)
{
    sys_mutex_lock(log_source_lock);
    qd_log_source_t* src = qd_log_source_lh(module);
    sys_mutex_unlock(log_source_lock);
    return src;
}

qd_log_source_t *qd_log_source_reset(const char *module)
{
    sys_mutex_lock(log_source_lock);
    qd_log_source_t* src = qd_log_source_lh(module);
    qd_log_source_defaults(src);
    sys_mutex_unlock(log_source_lock);
    return src;
}

static void qd_log_source_free_lh(qd_log_source_t* src) {
    DEQ_REMOVE_HEAD(source_list);
    log_sink_free_lh(src->sink);
    free(src);
}

bool qd_log_enabled(qd_log_source_t *source, int level) {
    if (!source) return false;
    int mask = source->mask == -1 ? default_log_source->mask : source->mask;
    return level & mask;
}

void qd_log_impl(qd_log_source_t *source, int level, const char *file, int line, const char *fmt, ...)
{
    if (!qd_log_enabled(source, level)) return;

    qd_log_entry_t *entry = new_qd_log_entry_t();
    DEQ_ITEM_INIT(entry);
    entry->module = source->module;
    entry->level  = level;
    entry->file   = file ? strdup(file) : 0;
    entry->line   = line;
    time(&entry->time);
    va_list ap;
    va_start(ap, fmt);
    vsnprintf(entry->text, TEXT_MAX, fmt, ap);
    va_end(ap);

    write_log(source, entry);

    sys_mutex_lock(log_lock);
    DEQ_INSERT_TAIL(entries, entry);
    if (DEQ_SIZE(entries) > LIST_MAX)
        qd_log_entry_free_lh(entry);
    sys_mutex_unlock(log_lock);
}

void qd_log_initialize(void)
{
    DEQ_INIT(entries);
    DEQ_INIT(source_list);
    DEQ_INIT(sink_list);
    strcpy((char*)level_names, levels[NONE].name);
    for (level_index_t i = NONE+1; i < N_LEVELS; ++i) {
        strcat((char*)level_names, ", ");
        strcat((char*)level_names, levels[i].name);
    }
    log_lock = sys_mutex();
    log_source_lock = sys_mutex();

    default_log_source = qd_log_source(SOURCE_DEFAULT);
    default_log_source->mask = levels[INFO].mask;
    default_log_source->timestamp = true;
    default_log_source->source = 0;
    default_log_source->sink = log_sink_lh(SINK_STDERR);
    logging_log_source = qd_log_source(SOURCE_LOGGING);
}


void qd_log_finalize(void) {
    while (DEQ_HEAD(source_list))
        qd_log_source_free_lh(DEQ_HEAD(source_list));
    while (DEQ_HEAD(entries))
        qd_log_entry_free_lh(DEQ_HEAD(entries));
    while (DEQ_HEAD(sink_list))
        log_sink_free_lh(DEQ_HEAD(sink_list));
}

qd_error_t qd_log_entity(qd_entity_t *entity) {

    qd_error_clear();
    char* module = qd_entity_get_string(entity, "module"); QD_ERROR_RET();
    sys_mutex_lock(log_source_lock);
    qd_log_source_t *src = qd_log_source_lh(module);
    assert(src);
    qd_log_source_t copy = *src;
    sys_mutex_unlock(log_source_lock);
    free(module);

    if (qd_entity_has(entity, "level")) {
        char *level = qd_entity_get_string(entity, "level");
        copy.mask = level_for_name(level)->mask;
        free(level);
    }
    QD_ERROR_RET();

    if (qd_entity_has(entity, "timestamp"))
        copy.timestamp = qd_entity_get_bool(entity, "timestamp");
    QD_ERROR_RET();

    if (qd_entity_has(entity, "source"))
        copy.source = qd_entity_get_bool(entity, "source");
    QD_ERROR_RET();

    if (qd_entity_has(entity, "output")) {
         log_sink_free_lh(copy.sink); /* DEFAULT source may already have a sink */
        char* output = qd_entity_get_string(entity, "output"); QD_ERROR_RET();
        copy.sink = log_sink_lh(output);
        free(output);
        if (copy.sink->syslog) /* Timestamp off for syslog. */
            copy.timestamp = 0;
    }

    sys_mutex_lock(log_source_lock);
    *src = copy;
    sys_mutex_unlock(log_source_lock);
    return qd_error_code();
}

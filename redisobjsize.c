/*
 * Copyright (c) 2009-2012, Wang Renjun <wangrj1981 at gmail dot com>
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 *
 *   * Redistributions of source code must retain the above copyright notice,
 *     this list of conditions and the following disclaimer.
 *   * Redistributions in binary form must reproduce the above copyright
 *     notice, this list of conditions and the following disclaimer in the
 *     documentation and/or other materials provided with the distribution.
 *   * Neither the name of Redis nor the names of its contributors may be used
 *     to endorse or promote products derived from this software without
 *     specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
 * AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS BE
 * LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
 * CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
 * SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
 * INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
 * CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
 * ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
 * POSSIBILITY OF SUCH DAMAGE.
 */

#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <getopt.h>
#include <hiredis.h>

#define MAX_KEYS        (1024)
#define MAX_PATTERNS    (1024)
#define CONNECT_TIMEOUT { 1, 0 }
#define SIZE_T_FMT      "zd"
#define BUFSIZE         (128)

static const char *hostip_ = "127.0.0.1", *hostpath_, *passwd_;
static int hostport_ = 6379, dbid_, interval_, verbose_;
static long long count_;
static const char *keys_[MAX_KEYS], *patterns_[MAX_PATTERNS];
static size_t nkey_, npattern_;
static redisContext *context_;

static struct option long_options[] = {
    { "key", required_argument, 0, 'k' },
    { "scan", optional_argument, 0, '$' },
    { "count", required_argument, 0, 'c' },
    { "verbose", no_argument, &verbose_, 1 },
    { 0, 0, 0, 0 }
};

static void show_usage(const char *prog);
static void objsize();
static size_t key_count();
static size_t scan_count();
static size_t debug_object(const char *key);
static size_t parse_length_field(const char *str);
static void connect();
static redisReply *reconnectingRedisCommand(const char *fmt, ...);
static char *bytesToHuman(size_t n);

int main(int argc, char **argv)
{
    int rv;
    while (1) {
        int option_index = 0;
        rv = getopt_long(argc, argv, "h:p:s:a:n:i:k:", long_options, &option_index);
        if (rv < 0)
            break;
        switch (rv) {
        case 0:
            break;
        case 'h':
            hostip_ = optarg;
            break;
        case 'p':
            hostport_ = (int)strtol(optarg, NULL, 10);
            break;
        case 's':
            hostpath_ = optarg;
            break;
        case 'a':
            passwd_ = optarg;
            break;
        case 'n':
            dbid_ = (int)strtol(optarg, NULL, 10);
            break;
        case 'i':
            interval_ = (int)strtol(optarg, NULL, 10);
            interval_ = interval_ * 1000000;
            break;
        case 'k':
            keys_[nkey_++] = optarg;
            break;
        case '$':
            if (optarg)
                patterns_[npattern_++] = optarg;
            break;
        case 'c':
            count_ = strtoll(optarg, NULL, 10);
            break;
        case '?':
            show_usage(argv[0]);
            break;
        default:
            abort();
            break;
        }
        if (nkey_ > MAX_KEYS) {
            fprintf(stderr, "Too many keys\n");
            exit(1);
        }
        if (npattern_ > MAX_PATTERNS) {
            fprintf(stderr, "Too many patterns\n");
            exit(1);
        }
    }
    if (nkey_ == 0 && npattern_ == 0)
        patterns_[npattern_++] = "*";
    objsize();
    return 0;
}

static void show_usage(const char *prog)
{
    fprintf(stderr,
        "Usage: %s [OPTIONS]\n"
        "  -h <hostname>        Server hostname (default: 127.0.0.1).\n"
        "  -p <port>            Server port (default: 6379).\n"
        "  -s <socket>          Server socket (overrides hostname and port).\n"
        "  -a <password>        Password to use when connecting to the server.\n"
        "  -n <db>              Database number.\n"
        "  -i <interval>        When scan is used, waits <interval> seconds per command.\n"
        "  -k <key>             Specified the key name.\n"
        "  --key <key>          Same as above.\n"
        "  --scan <pat>         Iterate the DB using the specified pattern.\n"
        "  --count <count>      When iterating the key space, the server will usually return count or a bit more than count elements per call(default: 10).\n"
        "  --verbose            Enable the verbose output.\n"
        "  --help               Output this help and exit.\n",
        prog);
    exit(0);
}

#define IF_ERROR_REPLY(r, desc, next)                                           \
    do {                                                                        \
        if (context_ == NULL                                                    \
            || context_->err                                                    \
            || (r) == NULL                                                      \
            || (r)->type == REDIS_REPLY_ERROR) {                                \
            if (context_ && context_->err) {                                    \
                fprintf(stderr, desc ": %s\n", context_->errstr);               \
                redisFree(context_);                                            \
                context_ = NULL;                                                \
            } else if ((r)) {                                                   \
                fprintf(stderr, desc ": %s\n", (r)->str);                       \
                freeReplyObject((r));                                           \
            }                                                                   \
            next;                                                               \
        }                                                                       \
    } while (0)

#define IF_WRONG_REPLY(r, t, desc, next)                                        \
    do {                                                                        \
        if ((r)->type != (t)) {                                                 \
            fprintf(stderr, desc "\n");                                         \
            freeReplyObject((r));                                               \
        }                                                                       \
        next;                                                                   \
    } while (0)

static void objsize()
{
    size_t total = key_count();
    total += scan_count();
    printf("Total size: %s\n", bytesToHuman(total));
}

static size_t key_count()
{
    redisReply *reply = NULL;
    size_t total = 0, sl = 0;
    size_t i = 0;
    if (nkey_ == 0) return 0;
    for (; i < nkey_; i++) {
        total += sl = debug_object(keys_[i]);
        printf("\tKey: %s, Size: %s\n", keys_[i], bytesToHuman(sl));
    }
    printf("All the size of the key is: %s\n", bytesToHuman(total));
    return total;
}

static size_t scan_count()
{
    redisReply *reply, *keys;
    size_t total = 0, pattotal, i = 0, lines = 0;
    long long cursor = 0;
    char fmtbuf[BUFSIZE];
    if (npattern_ == 0) return 0;
    for (; i < npattern_; i++) {
        size_t pattotal = 0;
        size_t n = snprintf(fmtbuf, BUFSIZE, "SCAN %%lld");
        if (patterns_[i] && !(patterns_[i][0] == '*' && patterns_[i][1] == '\0'))
            n += snprintf(fmtbuf + n, BUFSIZE - n, " MATCH %s", patterns_[i]);
        if (count_)
            snprintf(fmtbuf + n, BUFSIZE - n, " COUNT %lld", count_);
        do {
            size_t j = 0;
            reply = reconnectingRedisCommand(fmtbuf, cursor);
            IF_ERROR_REPLY(reply, "SCAN error", continue);
            IF_WRONG_REPLY(reply, REDIS_REPLY_ARRAY, "Non ARRAY response from SCAN", continue);
            keys = reply->element[1];
            for (j = 0; j < keys->elements; j++) {
                size_t sl = debug_object(keys->element[i]->str);
                if (verbose_) {
                    printf("\t%3" SIZE_T_FMT ") Key: %s, Size: %s\n", ++lines, keys->element[i]->str, bytesToHuman(sl));
                }
                pattotal += sl;
                total += sl;
            }
            cursor = strtoll(reply->element[0]->str, NULL, 10);
            if (interval_)
                usleep(interval_);
        } while (cursor != 0);
        printf("\tPattern: %s, Size: %s\n", patterns_[i], bytesToHuman(pattotal));
    }
    printf("All the size of the pattern is: %s\n", bytesToHuman(total));
    return total;
}

static size_t debug_object(const char *key)
{
    redisReply *reply;
    size_t sl;
    reply = reconnectingRedisCommand("DEBUG OBJECT %s", key);
    IF_ERROR_REPLY(reply, "DEBUG OBJECT error", return 0);
    IF_WRONG_REPLY(reply, REDIS_REPLY_STATUS, "Non STRING response from DEBUG OBJECT", return 0);
    sl = parse_length_field(reply->str);
    freeReplyObject(reply);
    return sl;
}

static size_t parse_length_field(const char *str)
{
#if 0
    size_t sl = 0;
    sscanf(str, "%*[^ ] %*[^ ] %*[^ ] %*[^ ] serializedlength:%" SIZE_T_FMT, &sl);
    return sl;
#else
    const char *l = strstr(str, "serializedlength");
    if (l == NULL)
        return 0;
    l += strlen("serializedlength:");
    return (size_t)strtoull(l, NULL, 10);
#endif
}

static void connect()
{
    redisReply *reply;
    struct timeval timeout = CONNECT_TIMEOUT;
    if (hostpath_ == NULL)
        context_ = redisConnectWithTimeout(hostip_, hostport_, timeout);
    else
        context_ = redisConnectUnixWithTimeout(hostpath_, timeout);
    if (context_ == NULL || context_->err) {
        if (context_)
            redisFree(context_);
        context_ = NULL;
        return;
    }
    if (passwd_) {
        reply = redisCommand(context_, "AUTH %s", passwd_);
        if (context_->err || reply == NULL || reply->type == REDIS_REPLY_ERROR) {
            freeReplyObject(reply);
            redisFree(context_);
            context_ = NULL;
            return;
        }
        freeReplyObject(reply);
    }
    if (dbid_) {
        reply = redisCommand(context_, "SELECT %d", dbid_);
        if (context_->err || reply == NULL || reply->type == REDIS_REPLY_ERROR) {
            freeReplyObject(reply);
            redisFree(context_);
            context_ = NULL;
            return;
        }
        freeReplyObject(reply);
    }
    return;
}

/* Send a command reconnecting the link if needed. */
static redisReply *reconnectingRedisCommand(const char *fmt, ...)
{
    redisReply *reply = NULL;
    int tries = 0;
    va_list ap;

    while (reply == NULL) {
        while (context_ == NULL || context_->err & (REDIS_ERR_IO | REDIS_ERR_EOF)) {
            printf("\r\x1b[0K"); /* Cursor to left edge + clear line. */
            printf("Reconnecting... %d\r", ++tries);
            fflush(stdout);
            if (context_)
                redisFree(context_);
            connect();
            usleep(1000000);
        }

        va_start(ap, fmt);
        reply = redisvCommand(context_, fmt, ap);
        va_end(ap);

        if (context_->err && !(context_->err & (REDIS_ERR_IO | REDIS_ERR_EOF))) {
            fprintf(stderr, "Error: %s\n", context_->errstr);
            exit(1);
        } else if (tries > 0) {
            printf("\r\x1b[0K"); /* Cursor to left edge + clear line. */
        }
    }
    return reply;
}

/* Convert number of bytes into a human readable string of the form:
 * 100B, 2G, 100M, 4K, and so forth. */
static char *bytesToHuman(size_t n)
{
    static char readable[16];
    double d;
    if (n < 1024) {
        /* Bytes */
        sprintf(readable, "%" SIZE_T_FMT "B", n);
    } else if (n < (1024 * 1024)) {
        d = (double) n / (1024);
        sprintf(readable, "%.2fK", d);
    } else if (n < (1024LL * 1024 * 1024)) {
        d = (double) n / (1024 * 1024);
        sprintf(readable, "%.2fM", d);
    } else if (n < (1024LL * 1024 * 1024 * 1024)) {
        d = (double) n / (1024LL * 1024 * 1024);
        sprintf(readable, "%.2fG", d);
    }
    return readable;
}

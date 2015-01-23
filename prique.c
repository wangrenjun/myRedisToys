#include "prique.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <signal.h>
#include <errno.h>
#include <stdarg.h>
#include <sys/stat.h>
#include <assert.h>

static int load_script(const char *script_file, char **script, size_t *scriptsize);
static void *memdup(const void *p, size_t n);
static int execute_command(redisContext *c,
    int op,
    const char *sha1,
    const char *path,
    redisReply **reply,
    const char *name,
    ...);
static int execute_commandv(redisContext *c,
    int op,
    const char *sha1,
    const char *path,
    redisReply **reply,
    const char *name,
    va_list ap);

enum {
    PUSH = 0,
    POP,
    BPOP,
    OTHER,
};

int prique_push(redisContext *c,
    const char *enqueue_sha1,
    const char *enqueue_path,
    const char *name,
    unsigned int priority,
    unsigned int expire,
    const unsigned char *val,
    size_t val_size)
{
    redisReply *reply = NULL;
    int rv;

    rv = execute_command(c, PUSH, enqueue_sha1, enqueue_path, &reply, name, priority, expire, val, val_size);
    if (rv)
        return rv;
    if (reply->type == REDIS_REPLY_INTEGER
        && reply->integer == 1)
        rv = 0;
    else
        rv = -1;
    if (reply)
        freeReplyObject(reply);

    return rv;
}

int prique_pop(redisContext *c,
    const char *dequeue_sha1,
    const char *dequeue_path,
    const char *name,
    unsigned char **val,
    size_t *val_size)
{
    redisReply *reply = NULL;
    int rv;

    rv = execute_command(c, POP, dequeue_sha1, dequeue_path, &reply, name);
    if (rv)
        return rv;
    if (reply->str && reply->len > 0) {
        *val = (unsigned char *)memdup(reply->str, reply->len);
        *val_size = reply->len;
    }
    if (reply)
        freeReplyObject(reply);

    return rv;
}

int prique_bpop(redisContext *c,
    const char *dequeue_sha1,
    const char *dequeue_path,
    const char *name,
    unsigned int timeout,
    unsigned char **val,
    size_t *val_size)
{
    redisReply *reply = NULL;
    int rv;

    rv = execute_command(c, BPOP, dequeue_sha1, dequeue_path, &reply, name, timeout);
    if (rv)
        return rv;
    if (reply->str && reply->len > 0) {
        *val = (unsigned char *)memdup(reply->str, reply->len);
        *val_size = reply->len;
    }
    if (reply)
        freeReplyObject(reply);

    return rv;
}

int prique_len(redisContext *c,
    const char *lenqueue_sha1,
    const char *lenqueue_path,
    const char *name)
{
    redisReply *reply = NULL;
    int rv;

    rv = execute_command(c, OTHER, lenqueue_sha1, lenqueue_path, &reply, name);
    if (rv)
        return rv;
    rv = reply->integer;
    if (reply)
        freeReplyObject(reply);

    return rv;
}

int prique_remove(redisContext *c,
    const char *rmqueue_sha1,
    const char *rmqueue_path,
    const char *name)
{
    redisReply *reply = NULL;
    int rv;

    rv = execute_command(c, OTHER, rmqueue_sha1, rmqueue_path, &reply, name);
    if (rv)
        return rv;
    rv = reply->integer;
    if (reply)
        freeReplyObject(reply);

    return rv;
}

static int load_script(const char *script_file, char **script, size_t *scriptsize)
{
    size_t filesize;
    struct stat statbuff;
    char *s;
    FILE *fp;

    if (stat(script_file, &statbuff) < 0)
        return errno;
    else
        filesize = statbuff.st_size;
    s = (char *)malloc(filesize + 1);
    if (s == NULL)
        return ENOMEM;
    fp = fopen(script_file, "rb");
    if (fp == NULL) {
        free(s);
        return errno;
    }
    fread(s, 1, filesize, fp);
    fclose(fp);
    s[filesize] = '\0';
    *script = s;
    *scriptsize = filesize;

    return 0;
}

static void *memdup(const void *p, size_t n)
{
    void *q;

    assert(p != NULL);
    assert(n > 0);
    q = malloc(n);
    if (q == NULL)
        return NULL;
    else
        memcpy(q, p, n);
    return q;
}

static int execute_command(redisContext *c,
    int op,
    const char *sha1,
    const char *path,
    redisReply **reply,
    const char *name,
    ...)
{
    va_list ap;
    int rv;

    va_start(ap, name);
    rv = execute_commandv(c, op, sha1, path, reply, name, ap);
    va_end(ap);

    return rv;
}

static int execute_commandv(redisContext *c,
    int op,
    const char *sha1,
    const char *path,
    redisReply **reply,
    const char *name,
    va_list ap)
{
    char *script = NULL;
    size_t scriptsize;
    int rv;
    unsigned int priority, expire, timeout;
    unsigned char *val;
    size_t val_size;
    va_list cpy;
    
    assert(sha1 || path);
    if (!sha1) {
        rv = load_script(path, &script, &scriptsize);
        if (rv)
            return rv;
    }

    va_copy(cpy, ap);
    switch (op) {
    case PUSH:
        priority = va_arg(ap, unsigned int);
        expire = va_arg(ap, unsigned int);
        val = va_arg(ap, unsigned char *);
        val_size = va_arg(ap, size_t);
        if (sha1)
            *reply = (redisReply *)redisCommand(c, "EVALSHA %s 0 %s %u %u %b", sha1, name, priority, expire, val, val_size);
        else
            *reply = (redisReply *)redisCommand(c, "EVAL %s 0 %s %u %u %b", script, name, priority, expire, val, val_size);
        break;

    case BPOP:
        timeout = va_arg(ap, unsigned int);
    case POP:
        if (op == BPOP) {
            char sigque[128];

            snprintf(sigque, sizeof(sigque), "%s:sigque", name);
            *reply = (redisReply *)redisCommand(c, "BRPOP %s %d", sigque, timeout);
            if (c->err != REDIS_OK)
                return -1;
        }
    default:
        if (sha1)
            *reply = (redisReply *)redisCommand(c, "EVALSHA %s 0 %s", sha1, name);
        else
            *reply = (redisReply *)redisCommand(c, "EVAL %s 0 %s", script, name);
        break;
    }
    va_end(cpy);
    if (script)
    	free(script);
    if (c->err != REDIS_OK)
        return -1;

    return 0;
}

#ifndef __PRIQUE_H__
#define __PRIQUE_H__

#include "hiredis/hiredis.h"

int prique_push(redisContext *c, 
    const char *enqueue_sha1,
    const char *enqueue_path,
    const char *name,
    unsigned int priority,
    unsigned int expire,
    const unsigned char *val, 
    size_t valsize);

int prique_pop(redisContext *c,
    const char *dequeue_sha1,
    const char *dequeue_path,
    const char *name,
    unsigned char **val,
    size_t *valsize);

int prique_bpop(redisContext *c,
    const char *dequeue_sha1,
    const char *dequeue_path,
    const char *name,
    unsigned int timeout,
    unsigned char **val,
    size_t *valsize);

int prique_len(redisContext *c, 
    const char *lenqueue_sha1,
    const char *lenqueue_path,
    const char *name);

int prique_remove(redisContext *c, 
    const char *rmqueue_sha1,
    const char *rmqueue_path,
    const char *name);

#endif /* __PRIQUE_H__ */

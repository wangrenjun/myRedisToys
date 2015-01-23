#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <signal.h>
#include "hiredis/hiredis.h"
#include "hiredis/async.h"
#include "hiredis/adapters/libev.h"

#define NAME "prique-async"

void signalled_cb(redisAsyncContext *c, void *r, void *privdata);

void dequeued_cb(redisAsyncContext *c, void *r, void *privdata)
{
    redisReply *reply = r;
    const char *sha = (const char *)privdata;

    if (reply == NULL)
        return;
    switch (reply->type) {
    case REDIS_REPLY_INTEGER:
        printf("[dequeued_cb] int: %d\n", reply->integer);
        break;

    case REDIS_REPLY_NIL:
        printf("[dequeued_cb] nil\n");
        break;

    case REDIS_REPLY_ERROR:
    case REDIS_REPLY_STRING:
        printf("[dequeued_cb] str: %s\n", reply->str);
        printf("[dequeued_cb] len: %d\n", reply->len);
        break;

    default:
        break;
    }
    redisAsyncCommand(c, signalled_cb, (void *)sha, "BRPOP %s %s", NAME, "0");
}

void signalled_cb(redisAsyncContext *c, void *r, void *privdata)
{
    redisReply *reply = r;
    const char *sha = (const char *)privdata;

    if (reply == NULL)
        return;
    switch (reply->type) {
    case REDIS_REPLY_INTEGER:
        printf("[signalled_cb] int: %d\n", reply->integer);
        break;

    case REDIS_REPLY_ERROR:
    case REDIS_REPLY_STRING:
        printf("[signalled_cb] str: %s\n", reply->str);
        printf("[signalled_cb] len: %d\n", reply->len);
        break;

    default:
        break;
    }
    redisAsyncCommand(c, dequeued_cb, (void *)sha, "EVALSHA %s %d %s", sha, 0, NAME);
}

void connect_cb(const redisAsyncContext *ac, int status)
{
    if (status != REDIS_OK) {
        fprintf(stderr, "Connect error: %s\n", ac->errstr);
        return;
    }
    fprintf(stderr, "Connected...\n");
}

void disconnect_cb(const redisAsyncContext *ac, int status)
{
    fprintf(stderr, "Disconnected...\n");
}

int main (int argc, char **argv) {
 	redisAsyncContext *ac;
	int i;
	
    if (argc != 4) {
		fprintf(stderr, "Usage: %s <Redis addr> <Redis port> <dequeue.lua sha1>\n", argv[0]);
		return 0;
	}
    signal(SIGPIPE, SIG_IGN);
    ac = redisAsyncConnect(argv[1], atoi(argv[2]));
    if (ac->err) {
        fprintf(stderr, "Error: %s\n", ac->errstr);
        redisAsyncFree(ac);
        return -1;
    }
    redisLibevAttach(EV_DEFAULT_ ac);
    redisAsyncSetConnectCallback(ac, connect_cb);
    redisAsyncSetDisconnectCallback(ac, disconnect_cb);
    redisAsyncCommand(ac, signalled_cb, (void *)argv[3], "BRPOP %s %s", NAME, "0");
    ev_loop(EV_DEFAULT_ 0);
    redisAsyncFree(ac);
    
    return 0;
}

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <signal.h>
#include "hiredis/hiredis.h"
#include "hiredis/async.h"
#include "hiredis/adapters/libev.h"

#define EXTREMA 9999
#define NAME "prique-async"

void enqueued_cb(redisAsyncContext *ac, void *r, void *privdata)
{
    redisReply *reply = r;
    int i = (int)privdata;

    if (reply == NULL)
        return;
    switch (reply->type) {
    case REDIS_REPLY_INTEGER:
        printf("[%d] int: %d\n", i, reply->integer);
        break;

    case REDIS_REPLY_ERROR:
    case REDIS_REPLY_STRING:
        printf("[%d] str: %s\n", i, reply->str);
        printf("[%d] len: %d\n", i, reply->len);
        break;

    default:
        break;
    }
    if (i == EXTREMA)
        redisAsyncDisconnect(ac);
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

int main(int argc, char **argv) {
    redisAsyncContext *ac;
    int i;

    if (argc != 4) {
        fprintf(stderr, "Usage: %s <Redis addr> <Redis port> <enqueue.lua sha1>\n", argv[0]);
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

    for (i = 0; i < EXTREMA + 1; i++) {
        char msg[16];
        unsigned int priority = i % 10;

        snprintf(msg, sizeof(msg), "msg-%d", i);
        redisAsyncCommand(ac, enqueued_cb, (void *)i, "EVALSHA %s %d %s %u %u %b", argv[3], 0, NAME, priority, 0, msg, strlen(msg));
    }
    ev_loop(EV_DEFAULT_ 0);

    return 0;
}

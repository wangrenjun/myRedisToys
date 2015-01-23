#include "prique.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#define EXTREMA 99
#define NAME "prique"
#define REDIS_CONNECT_TIMEOUT {1, 500000}

int main(int argc, char **argv) {
    struct timeval timeout = REDIS_CONNECT_TIMEOUT;
    redisContext *c;
    int rv, i;
    char *enqueue_sha1, *dequeue_sha1, *lenqueue_sha1, *rmqueue_sha1;

    if (argc != 7) {
        fprintf(stderr, "Usage: %s <Redis addr> <Redis port> <enqueue.lua sha1> <dequeue.lua sha1> <lenqueue.lua sha1> <rmqueue.lua sha1>\n", argv[0]);
        return 0;
    }
    enqueue_sha1 = argv[3];
    dequeue_sha1 = argv[4];
    lenqueue_sha1 = argv[5];
    rmqueue_sha1 = argv[6];

    c = redisConnectWithTimeout(argv[1], atoi(argv[2]), timeout);
    if (c == NULL || c->err)
        return -1;

    for (i = 0; i < EXTREMA + 1; i++) {
        char msg[16];
        unsigned int priority = i % 10;

        snprintf(msg, sizeof(msg), "msg-%d", i);
        rv = prique_push(c, enqueue_sha1, NULL, NAME, priority, 0, msg, strlen(msg));
        printf("[prique_push] rv: %d\n", rv);
    }
    rv = prique_len(c, lenqueue_sha1, NULL, NAME);
    printf(NAME " length: %d\n", rv);

    for (i = 0; i < EXTREMA + 1; i++) {
        unsigned char *msg = NULL;
        size_t msgsize = 0;
        rv = prique_pop(c, dequeue_sha1, NULL, NAME, &msg, &msgsize);
        if (msgsize > 0) {
            char buf[64];
            memcpy(buf, msg, msgsize);
            buf[msgsize] = '\0';
            printf("[prique_pop] str: %s\n", buf);
            printf("[prique_pop] len: %zd\n", (int)msgsize);
        }
        free(msg);
    }
    rv = prique_len(c, lenqueue_sha1, NULL, NAME);
    printf(NAME " length: %d\n", rv);

    redisFree(c);

    return 0;
}

myRedisToys
=================

some toy codes. just for fun..
No license

Priqueue
--------
一个基于[Redis script](https://redis.readthedocs.org/en/latest/script/index.html)实现的优先级队列，支持阻塞/非阻塞读、队列结点数据超时等；

enqueue.lua：入队操作；

dequeue.lua：出队操作；

lenqueue.lua：队列长度；

rmqueue.lua：清空队列；

prique.c：C封装的同步接口；

RWLock
--------
分布式读写锁，lua script实现，具体见rwlock.php、example-rwlock.php

* wangrj1981@gmail.com

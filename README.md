myRedisToys
=================

some toy codes, just for fun.

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
分布式读写锁，lua script实现，见rwlock.php、example-rwlock.php

redisobjsize.c
--------------
统计Redis的键占用内存空间大小的命令行工具

用法：

redisobjsize --help

查看键尺寸：

redisobjsize -h 127.0.0.1 -p 6379 -k USER:10000 -k USER:10001 -k USER:10002

查看整个库尺寸：

redisobjsize -h 127.0.0.1 -p 6379 --scan=*

查看所有用户尺寸：

redisobjsize -h 127.0.0.1 -p 6379 --scan=USER:*

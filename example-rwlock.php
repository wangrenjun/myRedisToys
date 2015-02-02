<?php
    
require_once 'rwlock.php';

$servers = array(array("127.0.0.1", 6379, 5), array("127.0.0.1", 6380, 5));

$resource = "test2";
$ttl = 60 * 1000;
$timeout = 5 * 1000;
$sleep_us = 10 * 1000 * 1000;

$rwlock = new rwlock($servers);

##############################

$rv = $rwlock->rlock($resource, $ttl, $timeout);
if ($rv)
    echo "rlock success\n";
else {
    echo "rlock failed\n";
    exit;
}

usleep($sleep_us);

$rwlock->unlock($resource);

##############################

$rv = $rwlock->wlock($resource, $ttl, $timeout);
if ($rv != false)
    echo "wlock success: " . $rv . "\n";
else {
    echo "wlock failed\n";
    exit;
}

usleep($sleep_us);

$rwlock->unlock($resource, $rv);

##############################
echo "enter the rlock: ";
fgetc(STDIN);

$rv = $rwlock->rlock($resource, $ttl, $timeout);
if ($rv)
    echo "rlock success\n";
else {
    echo "rlock failed\n";
    exit;
}

echo "leave the rlock: ";
fgetc(STDIN);

$rwlock->unlock($resource);

##############################
echo "enter the wlock: ";
fgetc(STDIN);

$rv = $rwlock->wlock($resource, $ttl, $timeout);
if ($rv != false)
    echo "wlock success: " . $rv . "\n";
else {
    echo "wlock failed\n";
    exit;
}

echo "leave the wlock: ";
fgetc(STDIN);

$rwlock->unlock($resource, $rv);

##############################

?>

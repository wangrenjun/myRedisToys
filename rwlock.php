<?php
class rwlock
{
    public function __construct(array $redis_servers)
    {
        $this->servers = $redis_servers;
        $this->quorum = count($redis_servers) / 2 + 1;
    }
    
    # 参数：
    #   $redis_servers：
    #     host、port、timeout；
    #   $resource：
    #     名称；
    #   $ttl：
    #     锁生存时间，毫秒；
    #   $timeout：
    #     > 0，等待时间，毫秒；
    #     <= 0，不等待；
    #   $token：
    #     写锁Token；
    
    # 返回值：
    #   true，成功
    #   false，失败
    public function rlock($resource, $ttl, $timeout = 0)
    {
        if ($ttl <= 0)
            return false;
        $this->initialize();
        $n = 0;
        $instances = $this->instances;
        $expiration_time = microtime(true) * 1000 + $timeout;
        do {
            $retry_instances = array();
            foreach($instances as $instance) {
                if ($this->do_rlock($instance, $resource, $ttl)) {
                    if (++$n >= $this->quorum)
                        return true;
                } else
                    $retry_instances[] = $instance;
            }
            $instances = $retry_instances;
        } while (!$this->is_timeout($expiration_time));
        $this->unlock($resource);
        return false;
    }
    
    # 返回值：
    #   Token，成功，写锁Token
    #   false，失败
    public function wlock($resource, $ttl, $timeout = 0)
    {
        if ($ttl <= 0)
            return false;
        $this->initialize();
        $n = 0;
        $token = uniqid();
        $instances = $this->instances;
        $expiration_time = microtime(true) * 1000 + $timeout;
        do {
            $retry_instances = array();
            foreach($instances as $instance) {
                if ($this->do_wlock($instance, $resource, $ttl, $token)) {
                    if (++$n >= $this->quorum)
                        return $token;
                } else
                    $retry_instances[] = $instance;
            }
            $instances = $retry_instances;
        } while (!$this->is_timeout($expiration_time));
        $this->unlock($resource);
        return false;
    }
    
    public function unlock($resource, $token = null)
    {
        $this->initialize();
        if (empty($token)) {
            foreach($this->instances as $instance) {
                $this->do_unrlock($instance, $resource);
            }
        } else {
            foreach($this->instances as $instance) {
                $this->do_unwlock($instance, $resource, $token);
            }
        }
    }
    
    private $servers = array();
    
    private $instances = array();
    
    private $quorum;
    
    const TRY_RLOCK_SCRIPT = '
	    local rdkey = KEYS[1]
	    local wrkey = KEYS[2]
	    local ttl = ARGV[1]
	    local rv
	    if redis.call("EXISTS", wrkey) == 1 then
	        return 0
	    end
	    rv = redis.call("INCR", rdkey)
	    if redis.call("PTTL", rdkey) < tonumber(ttl) then
	        redis.call("PEXPIRE", rdkey, ttl)
	    end
	    return rv
    ';
    
    const TRY_WLOCK_SCRIPT = '
        local wrkey = KEYS[1]
        local rdkey = KEYS[2]
        local ttl = ARGV[1]
        local token = ARGV[2]
        if redis.call("EXISTS", wrkey) == 1 then
            return 0
        end
        if redis.call("EXISTS", rdkey) == 0 then
           redis.call("PSETEX", wrkey, ttl, token)
           return 1
        end
        return 0
    ';
    
    const TRY_UNRLOCK_SCRIPT = '
        local rdkey = KEYS[1]
        local rv
        if redis.call("EXISTS", rdkey) == 0 then
            return 0
        end
        rv = redis.call("DECR", rdkey)
        if rv <= 0 then
            redis.call("DEL", rdkey)
        end
        return rv
    ';
    
    const TRY_UNWLOCK_SCRIPT = '
        local wrkey = KEYS[1]
        local token = ARGV[1]
        if redis.call("GET", wrkey) == token then
            return redis.call("DEL", wrkey)
        end
        return 0
    ';
    
    private function initialize()
    {
        if (!empty($this->instances))
            return false;
        foreach ($this->servers as $serv) {
            list($host, $port, $timeout) = $serv;
            $instance = new Redis();
            if (!$instance->connect($host, $port, $timeout))
                return false;
            $this->instances[] = $instance;
        }
        return true;
    }
    
    private function is_timeout($timeout)
    {
        $curtime = microtime(true) * 1000;
        return ($curtime > $timeout) ? true : false;
    }
    
    private function get_rdkey($resource)
    {
        return $resource . ":rd";
    }
    
    private function get_wrkey($resource)
    {
        return $resource . ":wr";
    }
    
    private function do_rlock($instance, $resource, $ttl)
    {
        $rdkey = $this->get_rdkey($resource);
        $wrkey = $this->get_wrkey($resource);
        return $instance->eval(rwlock::TRY_RLOCK_SCRIPT, [$rdkey, $wrkey, $ttl], 2);
    }
    
    private function do_wlock($instance, $resource, $ttl, $token)
    {
        $rdkey = $this->get_rdkey($resource);
        $wrkey = $this->get_wrkey($resource);
        return $instance->eval(rwlock::TRY_WLOCK_SCRIPT, [$wrkey, $rdkey, $ttl, $token], 2);
    }
    
    private function do_unrlock($instance, $resource)
    {
        $rdkey = $this->get_rdkey($resource);
        return $instance->eval(rwlock::TRY_UNRLOCK_SCRIPT, [$rdkey], 1);
    }
    
    private function do_unwlock($instance, $resource, $token)
    {
        $rdkey = $this->get_wrkey($resource);
        return $instance->eval(rwlock::TRY_UNWLOCK_SCRIPT, [$rdkey, $token], 1);
    }
    
    private function pause()
    {
        usleep(1000);
    }
}

?>

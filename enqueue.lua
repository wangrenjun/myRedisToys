local priqueue_prefix = ARGV[1];
local priority = ARGV[2];
local expire = ARGV[3];
local value = ARGV[4];
local counter = priqueue_prefix .. ':cnt';
local key = priqueue_prefix .. ':i';
local priority_queue = priqueue_prefix .. ':' .. priority;
local priority_set = priqueue_prefix .. ':priset';
local signal_queue = priqueue_prefix;
local rv, cnt;

cnt = redis.call('INCR', counter);
key = key .. ':' .. cnt;
if tonumber(expire) > 0 then
    rv = redis.call('SETEX', key, expire, value);
else
    rv = redis.call('SET', key, value);
end

rv = redis.call('LPUSH', priority_queue, cnt);
if rv <= 0 then
    return redis.error_reply('LPUSH ' .. priority_queue .. ' failed');
end

rv = redis.call('ZADD', priority_set, priority, priority);
rv = redis.call('LPUSH', signal_queue, 1);
if rv <= 0 then
    return redis.error_reply('LPUSH ' .. signal_queue .. ' failed');
end

return 1;

local priqueue_prefix = ARGV[1];
local priority_set = priqueue_prefix .. ':priset';
local key = priqueue_prefix .. ':i';
local rv;
local len = 0;
rv = redis.call('ZREVRANGE', priority_set, 0, -1);
for i = 1, #rv do
    local priority = rv[i];
    local priority_queue = priqueue_prefix .. ':' .. priority;
    local cnt = redis.call('LLEN', priority_queue);
    len = len + cnt;
end

return len;

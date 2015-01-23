local priqueue_prefix = ARGV[1];
local priority_set = priqueue_prefix .. ':priset';
local key = priqueue_prefix .. ':i';
local rv;

rv = redis.call('ZREVRANGE', priority_set, 0, -1);
for i = 1, #rv do
    local priority = rv[i];
    local priority_queue = priqueue_prefix .. ':' .. priority;
    local cnt = true;
    while cnt ~= false do
        cnt = redis.call('RPOP', priority_queue);
        if cnt ~= false then
            key = key .. ':' .. cnt;
            local value = redis.call('GET', key);
            if value ~= false then
                local len = redis.call('LLEN', priority_queue);
                if len <= 0 then
                    redis.call('ZREM', priority_set, priority);
                end
                return value;
            end
        end
    end
    redis.call('ZREM', priority_set, priority);
end

return nil;

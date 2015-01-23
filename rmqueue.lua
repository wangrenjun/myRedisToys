local priqueue_prefix = ARGV[1];
local signal_queue = priqueue_prefix;
local priority_set = priqueue_prefix .. ':priset';
local key = priqueue_prefix .. ':i';
local counter = priqueue_prefix .. ':cnt';
local rv, res = 0;

rv = redis.call('ZREVRANGE', priority_set, 0, -1);
for i = 1, #rv do
    local priority = rv[i];
    local priority_queue = priqueue_prefix .. ':' .. priority;
    local cnt = true;
    while cnt ~= false do
        cnt = redis.call('RPOP', priority_queue);
        if cnt ~= false then
            key = key .. ':' .. cnt;
            local e = redis.call('EXISTS', key);
            if e ~= 0 then
                redis.call('DEL', key);
            end
        end
    end
    redis.call('DEL', priority_queue);
    res = 1;
end
redis.call('DEL', priority_set);
redis.call('DEL', signal_queue);
redis.call('DEL', counter);

return res;

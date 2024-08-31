local key = KEYS[1]
local expect_token = ARGV[1]
local extend = ARGV[2]

local store_token = redis.call("get", key)
if store_token == nil or store_token ~= expect_token then
    return false
end

local expiration = tonumber(redis.call("pttl", key))

if  expiration <= 0 then
    return false
end

redis.call("pexpire", key, tostring(expiration + extend))

return true
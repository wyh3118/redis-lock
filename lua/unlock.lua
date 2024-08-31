local key = KEYS[1]
local expect_token = ARGV[1]

local store_token = redis.call("get", key)

if store_token == nil or store_token ~= expect_token then
    return false
end

redis.call("del", key)

return true
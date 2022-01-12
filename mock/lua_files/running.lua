local redis = require "resty.redis"
local red = redis:new()
red:set_timeouts(1000, 1000, 1000)
local ok, err = red:connect("127.0.0.1", 6379)
if not ok then
    log_to_file(ngx.DEBUG, "failed to connect to redis: " .. sock_id + " " + err)
end

red:set(tostring(ngx.var.msec)..":NA:NA:Command", "HERAMock is up and Running")
ngx.say('ok')
ngx.eof()

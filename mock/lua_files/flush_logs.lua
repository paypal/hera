local M = {}
package.path = '/usr/local/openresty/nginx/lua_files/?.lua;' .. package.path

local netstring = require("netstring")
local file_utils = require("file_utils")
local redis = require "resty.redis"


function M.flush_logs_tcp(port, sock)
    local red = redis:new()
    red:set_timeouts(1000, 1000, 1000)
    local ok, err = red:connect("127.0.0.1", 6379)
    if not ok then
        file_utils.log_to_file(ngx.DEBUG, "failed to connect to redis:  " .. err)
    end
    local keys =  ngx.shared.redis_req_res:get_keys();
    local resp = "false"
    for _, s in pairs(keys) do
        local k = ngx.shared.redis_req_res:get(s)
        if (string.find(k, ":" .. port .. ":")) then
            local r_response = ngx.shared.redis_response:get(k)
            if (r_response ~= nil ) then
                local d = r_response .. ' OCCMOCK_END_TIME ' .. tostring(ngx.var.msec)
                d = d .. ' OCC_MOCK_PORT ' .. port
                red:set(k, d)
                red:expire(k, 60*60*24)
                resp = "true"
            end
            ngx.shared.redis_req_res:delete(s)
            ngx.shared.redis_response:delete(k)
        end
    end
    netstring.send(sock, "flush_logs", resp)
    return resp
end

return M
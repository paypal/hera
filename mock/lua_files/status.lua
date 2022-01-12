--
-- Created by IntelliJ IDEA.
-- User: radhikesavan
-- Date: 6/20/19
-- Time: 12:03 PM
-- To change this template use File | Settings | File Templates.
--

local function log_to_file(level, data)
    if ((ngx.shared.mock_response:get("DISABLE_LOG") == nil and ngx.shared.mock_response:get("DISABLE_FILE_LOG") == nil)
            or (level == ngx.ERR)) then
        local logdata = debug.getinfo(2).currentline .. ':' .. data
        while(#logdata > 0) do
            local temp = string.sub(logdata, 0, 4000)
            ngx.log(level, temp)
            logdata = string.sub(logdata, 4001)
        end
    end
end


local function read_data(socket_obj, socket_obj_name)
    local delim = ":"
    local readline = socket_obj:receiveuntil(delim)
    local size, _, _ = readline()
    if(not size) then
        ngx.log(ngx.ERR, "data recv failed to " .. socket_obj_name)
        return nil, nil
    end
    local data = socket_obj:receive( tonumber(size))
    return size, data
end

local function starts_with(str, start)
    return str:sub(1, #start) == start
end

local function send_data(socket_obj, socket_obj_name, datasize, data)
    if not datasize then
        return 'ok', nil
    end
    local data_to_send = datasize .. ":" .. data
    local _, err = socket_obj:send(data_to_send)
    if err then
        ngx.log(ngx.ERR, "data send failed to " .. socket_obj_name)
        return nil, 'err'
    else
        return 'ok', nil
    end

end


package.path = '/usr/local/openresty/nginx/lua_files/?.lua;' .. package.path

local sock = assert(ngx.req.socket(true))

local _, d = read_data(sock, "response");
local sp, _ = string.find(d, "=");

local d2 = string.sub(d, sp+1)

local resp = "success"

log_to_file(ngx.DEBUG, "status id " ..  d2)
if (ngx.shared.response_status:get(d2) ~= nil) then
    resp = ngx.shared.response_status:get(d2)
    ngx.shared.response_status:delete(d2)
end


send_data(sock, "upstream_request", string.len(resp), resp);


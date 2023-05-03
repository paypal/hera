local M = {}

function M.read(socket, debug_log_name)
    package.path = '/usr/local/openresty/nginx/lua_files/?.lua;' .. package.path
    local delim = ":"
    local readline = socket:receiveuntil(delim)
    local size, _, _ = readline()
    if(not size) then
        local utils = require("file_utils")
        utils.log_to_file(ngx.ERR, "failed during read operation " .. debug_log_name)
        return nil, nil
    end
    local data = socket:receive( tonumber(size))
    return size, data
end


function M.send(socket, debug_log_name, data_to_send)
    local datasize = string.len(data_to_send)
    if not datasize then
        return 'ok', nil
    end
    local _, err = socket:send(datasize .. ":" .. data_to_send)
    if err then
        ngx.log(ngx.ERR, "data send failed to " .. debug_log_name)
        return nil, 'err'
    else
        return 'ok', nil
    end

end

return M
local M = {}
package.path = '/usr/local/openresty/nginx/lua_files/?.lua;' .. package.path

local string_utils = require("string_utils")
local netstring = require("netstring")

function M.list_mock()
    local sock = assert(ngx.req.socket(true))

    local keys =  ngx.shared.mock_response:get_keys()

    local resp = ""
    if keys then
        for _, v in pairs(keys) do
            local value = ngx.shared.mock_response:get(v)
            if string.find(v, "DALHERAMOCK_SERVERIP") then
                local backed_by = os.getenv("BACKEDBY_ENV_NAME")
                if backed_by ~= nil then
                    value = backed_by
                end
            end
            v = string_utils.escape_special_chars(v)
            value = string_utils.escape_special_chars(value)

            resp = resp .. v .. "=" .. value .. " NEXT_LINE ";
        end
    end
    netstring.send(sock, "upstream_request", resp);
    return resp
end

return M
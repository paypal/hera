local M = {}
package.path = '/usr/local/openresty/nginx/lua_files/?.lua;' .. package.path

local nginx_utils = require("nginx_utils")
local netstring = require("netstring")
local file_utils = require("file_utils")

function M.list_mock_http()
    local up_sock = nginx_utils.get_upstream_socket("127.0.0.1", 8002);

    local _, d = netstring.read(up_sock, "upstream_response");
    file_utils.log_to_file(ngx.INFO, d)
    return d
end


return M

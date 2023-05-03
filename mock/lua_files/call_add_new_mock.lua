local M = {}

package.path = '/usr/local/openresty/nginx/lua_files/?.lua;' .. package.path
local nginx_utils = require("nginx_utils")
local file_utils = require("file_utils")
local netstring = require("netstring")

function M.add_mock_http()
    local up_sock = nginx_utils.get_upstream_socket("127.0.0.1", 8003);

    ngx.req.read_body()
    local args, err = ngx.req.get_post_args()

    if not args then
        nginx_utils.say_message("failed to get post args: " .. err)
        return
    end

    local p = ""
    local expire_time = 100 -- seconds
    local div = "heramockdiv"
    for key, val in pairs(args) do
        if key == "expire_time_in_sec" then
            expire_time = val;
        elseif key == "ip" then
            p = p .. ngx.var.remote_addr .. div .. val;
        elseif type(val) == "table" then
            p = p .. key .. div .. table.concat(val, ", ");
        else
            p = p .. key .. div .. val;
        end
    end

    file_utils.log_to_file(ngx.DEBUG, "sending " .. p)

    local d = "Please provide the key and the value for mock in data";
    local s;

    if p and string.len(p) > 0 then
        netstring.send(up_sock, "upstream_request", p);
        p = "expire_time_in_sec=" .. expire_time;
        netstring.send(up_sock, "expire_time", p);
        s, d = netstring.read(up_sock, "upstream_response");
    end

    return d
end

return M
package.path = '/usr/local/openresty/nginx/lua_files/?.lua;' .. package.path
local nginx_utils = require("nginx_utils")
local file_utils = require("file_utils")
local netstring = require("netstring")

local function flush_logs_http()

    local up_sock = nginx_utils.get_upstream_socket("127.0.0.1", 8007);

    ngx.req.read_body()
    local p = ""
    local args = ngx.req.get_uri_args()

    for key, val in pairs(args) do
        if key == "port" then
            p = val ;
        end
    end


    file_utils.log_to_file(ngx.DEBUG, "sending " .. p)

    local d = "Please provide port details for flushing";
    local s;

    if p and string.len(p) > 0 then
        netstring.send(up_sock, "upstream_request", p);
        s, d = netstring.read(up_sock, "upstream_response");
    end

    return d

end

ngx.say(flush_logs_http());
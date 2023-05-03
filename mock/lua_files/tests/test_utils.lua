local M = {}

local list_mock_port = 8002
local add_mock_port = 8003

package.path = '/usr/local/openresty/nginx/lua_files/?.lua;' .. package.path

local nginx_utils = require("nginx_utils")
local netstring = require("netstring")

function M.log_to_file(level, data)
    local log_data = debug.getinfo(2).currentline .. ':' .. data
    while(#log_data > 0) do
        local temp = ngx.var.server_port .. ':' .. string.sub(log_data, 0, 4000)
        ngx.log(level, temp)
        log_data = string.sub(log_data, 4001)
    end
end

function M.get_add_mock_socket(port)
    local up_sock = assert(ngx.socket.tcp());
    local up_ok, up_err = up_sock:connect("127.0.0.1", port);
    if not up_ok then
        local msg = "connection failure during add mock " .. up_err
        return false, msg;
    end
    return true, up_sock;
end

function M.list_mock()
    local up_sock = nginx_utils.get_upstream_socket("127.0.0.1", list_mock_port);
    netstring.send(up_sock, "1234", "1234")
    local _, data = netstring.read(up_sock, "upstream_response");
    return data
end

function M.read_from_server(socket_obj, log_id)
    local delimiter = ":"
    if  socket_obj == nil then
        return 0, nil
    end
    local readline = socket_obj:receiveuntil(delimiter)
    local size, _, _ = readline()
    if(not size) then
        M.log_to_file(ngx.DEBUG, log_id .. " no data recv to ")
        return false, 'error in reading back data'
    end
    local data = socket_obj:receive( tonumber(size) + 1)
    return true, data
end

function M.send_to_server(socket_obj, data_to_send, log_id)
    local _, err = socket_obj:send(data_to_send)
    if err then
        M.log_to_file(ngx.ERR, log_id .. " no data send to err: " .. err)
        return false, 'err in sending data ' .. data_to_send
    else
        return true, 'ok'
    end

end

function M.get_id(o)
    if type(o) == 'table' then
        for _, v in pairs(o) do
            local resp = M.get_id(v)
            if resp ~= nil and resp ~= "unknown" then
                return string.gsub(resp, "%s+", "")
            end
        end
    else
        if tostring(o) == "mocked" then
            return "mocked"
        elseif string.find(tostring(o), "userdata") then
            local resp = split(tostring(o), ":")
            return string.gsub(resp[2], "%s+", "") .. "_" .. ngx.var.connection .. "_" .. ngx.var.remote_port
        end
    end
    return "unknown"
end

function M.add_mock(key, value)
    local resp, sock = M.get_add_mock_socket(add_mock_port)
    local div = "heramockdiv"
    local payload = key .. div .. value;

    local expire_time = 100 -- seconds

    local msg = ''
    resp, msg = M.send_to_server(sock, string.len(payload) .. ':' .. payload, '')

    payload = "expire_time_in_sec=" .. expire_time;
    resp, msg = M.send_to_server(sock, string.len(payload) .. ':' .. payload, '')

    M.read_from_server(sock, '')
    sock:close()
    return resp
end

function M.set_connect_mock()
    local resp, sock = M.get_add_mock_socket(add_mock_port)
    if not resp then
        return resp, sock
    end
    local expire_time = 100 -- seconds
    local div = "heramockdiv"
    local payload = "DALHERAMOCK_SERVERIP" .. div .. "MOCK_SERVER";
    local msg = ''
    resp, msg = M.send_to_server(sock, string.len(payload) .. ':' .. payload, '')

    payload = "expire_time_in_sec=" .. expire_time;
    resp, msg = M.send_to_server(sock, string.len(payload) .. ':' .. payload, '')
    sock:close()
    return resp, msg
end

function M.do_initial_handshake(up_sock)
    M.log_to_file(ngx.DEBUG, "set_connect_mock done " .. tostring(resp))
    local start = "43:0 10:2002 occ 1,23:2003 XXXXXX_trialclient,,"
    local resp, msg = M.send_to_server(up_sock, start, 'sock_id')
    M.log_to_file(ngx.DEBUG,  "intro done " .. tostring(resp))

    if resp then
        resp, msg = M.read_from_server(up_sock, 'sock_id')
        M.log_to_file(ngx.DEBUG, "intro resp done " .. tostring(resp))
    end

    if resp then
        local client_hello = "119:11 PID: 50942,HOST: AB-CDE-11111111, EXEC: 50942@AB-CDE-11111111, Poolname: exampleservice, Command: init, null, Name: ,"
        resp, msg = M.send_to_server(up_sock, client_hello, 'sock_id')
        M.log_to_file(ngx.DEBUG, "client hello done " .. tostring(resp))
    end

    if resp then
        resp, msg = M.read_from_server(up_sock, 'sock_id')
        M.log_to_file(ngx.DEBUG, "server hello done " .. tostring(resp))
    end
    return resp, msg
end

function M.make_connection()
    local up_sock = assert(ngx.socket.tcp())

    local msg = 'ok'
    local status = true

    local sock_id = M.get_id(up_sock)

    local up_ok, up_err = up_sock:connect("127.0.0.1", "10102")
    M.log_to_file(ngx.DEBUG, "connection done")
    if up_ok then
        local ssl_disable = os.getenv("HERA_DISABLE_SSL")
        local ssl_ok = true
        local u_err
        if ssl_disable ~= nil and ssl_disable == "true" then
            M.log_to_file(ngx.DEBUG, " SSL Disabled for connection ")
        else
            M.log_to_file(ngx.DEBUG, " SSL enabled")
            ssl_ok, u_err = up_sock:sslhandshake()
            M.log_to_file(ngx.DEBUG, "sslhandshake done")
        end
        if ssl_ok then
            M.log_to_file(ngx.DEBUG, "starting do_initial_handshake")
            local resp, err = M.do_initial_handshake(up_sock)
            if not resp then
                msg = 'failed during client intro '
                if err then
                    msg = msg .. err
                    status = false
                end
            end
        else
            msg = 'failed during ssl ' .. u_err
            status = false
        end
    else
        M.log_to_file(ngx.DEBUG, "connection failed ".. up_err)
        msg = "failed during connect " .. up_err
        status = false
    end
    return status, up_sock, sock_id
end

function M.merge_tables(a, b)
    local c = {}
    for k,v in pairs(a) do c[k] = v end
    for k,v in pairs(b) do c[k] = v end
    return c
end

function M.get_uri_params(key, default)
    local args, err = ngx.req.get_uri_args()

    if err == "truncated" then
        return "all"
    end

    for uri_key, val in pairs(args) do
        if uri_key == key then
            return val
        end
    end

    return default
end

return M
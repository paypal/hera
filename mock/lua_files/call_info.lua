local function get_upstream_socket()
    local upsock = assert(ngx.socket.tcp());
    local port = 8008;
    local upok, uperr = upsock:connect("127.0.0.1", port);
    if not upok then
        ngx.log(ngx.ERR, "upstream connection failure " .. uperr);
        return nil;
    end
    return upsock;
end

local function read_data(socket_obj, socket_obj_name)
    local delim = ":";
    local readline = socket_obj:receiveuntil(delim);
    local size, _, _ = readline();
    if(not size) then
        ngx.log(ngx.ERR, "data recv failed to " .. socket_obj_name);
        return nil, nil;
    end
    local data = socket_obj:receive( tonumber(size));
    return size, data;
end

local up_sock = get_upstream_socket();



local _, d = read_data(up_sock, "upstream_response");
ngx.header["Content-type"] = 'application/json'
ngx.say(d);
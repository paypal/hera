local function get_upstream_socket()
    local upsock = assert(ngx.socket.tcp());
    local port = 8002;
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

local upsock = get_upstream_socket();

local _, d = read_data(upsock, "upstream_response");
for line in (d.." NEXT_LINE "):gmatch("(.-)".." NEXT_LINE ") do
    if (line ~= nil and line ~= "") then
        ngx.say(line);
    end
end

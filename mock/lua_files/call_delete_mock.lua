local function get_upstream_socket()
    local upsock = assert(ngx.socket.tcp());
    local port = 8004;
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

local function send_data(socket_obj, socket_obj_name, datasize, data)
    local data_to_send = datasize .. ":" .. data;
    local _, err = socket_obj:send(data_to_send);
    if err then
        ngx.log(ngx.ERR, "data send failed to " .. socket_obj_name);
        return nil, 'err';
    else
        return 'ok', nil;
    end

end

local upsock = get_upstream_socket();

ngx.req.read_body()
local p = ""
local args = ngx.req.get_uri_args()
local div = "heramockdiv"

for key, val in pairs(args) do
    if type(val) == "table" then
        p = p .. key .. div .. table.concat(val, ", ");
    elseif val == "ip" then
        p = p .. key .. div .. ngx.var.remote_addr;
    else
        p = p .. key .. div .. val;
    end
end

p = p:gsub( "HERA_MOCK_SPACE_H", " H")

local d = "Please provide the key to delete mock data";
local s;

if p and string.len(p) > 0 then
  send_data(upsock, "upstream_request", string.len(p), p);
  s, d = read_data(upsock, "upstream_response");
end

ngx.say(d);
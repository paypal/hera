local function get_upstream_socket()
    local upsock = assert(ngx.socket.tcp());
    local port = 8003;
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
    --ngx.log(ngx.INFO, "sending ..." .. data_to_send)
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
local args, err = ngx.req.get_post_args()

if not args then
  ngx.say("failed to get post args: ", err)
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

local d = "Please provide the key and the value for mock in data";
local s;

if p and string.len(p) > 0 then
  send_data(upsock, "upstream_request", string.len(p), p);
  p = "expire_time_in_sec=" .. expire_time;
  send_data(upsock, "expire_time", string.len(p), p);
  s, d = read_data(upsock, "upstream_response");
end

ngx.say(d);
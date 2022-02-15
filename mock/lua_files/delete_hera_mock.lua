local function read_data(socket_obj, socket_obj_name)
    local delim = ":"
    local readline = socket_obj:receiveuntil(delim)
    local size, _, _ = readline()
    if(not size) then
        ngx.log(ngx.ERR, "data recv failed to " .. socket_obj_name)
        return nil, nil
    end
    local data = socket_obj:receive( tonumber(size))
    return size, data
end

local function send_data(socket_obj, socket_obj_name, datasize, data)
    if not datasize then
      return 'ok', nil
    end
    local data_to_send = datasize .. ":" .. data
    local _, err = socket_obj:send(data_to_send)
    if err then
        ngx.log(ngx.ERR, "data send failed to " .. socket_obj_name)
        return nil, 'err'
    else
        return 'ok', nil
    end

end

package.path = '/usr/local/openresty/nginx/lua_files/?.lua;' .. package.path

local function split(s, delimiter)
    local result = {};
    for match in (s..delimiter):gmatch("(.-)"..delimiter) do
        table.insert(result, match);
    end
    return result;
end


local function get_id(o)
    if type(o) == 'table' then
        for _,v in pairs(o) do
            local resp = get_id(v)
            if resp ~= nil and resp ~= "unknown" then
                return string.gsub(resp, "%s+", "")
            end
        end
    else
        if string.find(tostring(o), "userdata") then
            local resp = split(tostring(o), ":")
            return string.gsub(resp[2], "%s+", "")
        end
    end
    return "unknown"
end


local sock = assert(ngx.req.socket(true))

local ip = ngx.var.remote_addr

local _, d = read_data(sock, "response");
local resp = ""
local payload = ""
local div = "heramockdiv"
if d ~= nil then
    local sp, _ = string.find(d, div);
    local key = string.sub(d, sp+10)
    key = key:gsub("heraMockEqual", "=")
    key = key:gsub("heraMockUnaryAnd", "&")
    local tmp_val = ngx.shared.mock_response:get(key)
    if tmp_val == nil then
        tmp_val = 'nil'
    end
    ngx.log(ngx.INFO, "Removing Mock for " .. key .. ": " .. tmp_val)
    payload = "key=" .. string.sub(d, sp+1)

    ngx.shared.mock_response:delete(key);
    ngx.shared.object_mock_cache:delete(key);
    ngx.shared.object_mock_cache_meta:delete(key);
    if ngx.shared.load_based_mock:get(key) ~= nil then
        for _, v in pairs(ngx.shared.load_based_mock:get_keys()) do
            ngx.shared.load_based_mock:delete(v);
        end
    end
    local keys =  ngx.shared.mock_response:get_keys();
    for _, v in pairs(keys) do
        if string.find(v, key .. "_") then
            ngx.shared.mock_response:delete(v);
        end
    end
    local pid = ngx.shared.mock_corr_connection:get(key)
    ngx.shared.mock_connection:delete(pid)
    ngx.shared.worker_sql_state:delete(key)

    ngx.shared.random_failure_state:delete(key)
    ngx.shared.random_response_store:delete(key)

    local keys =  ngx.shared.mock_response:get_keys();
    for _, v in pairs(keys) do
        resp = resp .. v .. "=" .. ngx.shared.mock_response:get(v) .. ", ";
    end
end

if string.len(resp) == 0 then
  resp = "mock data is empty";
end

send_data(sock, "upstream_request", string.len(resp), resp);
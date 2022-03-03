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
        v = v:gsub("=", "heraMockEqual")
        value = value:gsub("=", "heraMockEqual")
        resp = resp .. v .. "=" .. value .. " NEXT_LINE ";
    end
end

send_data(sock, "upstream_request", string.len(resp), resp);

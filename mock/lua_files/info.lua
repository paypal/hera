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


package.path = '/usr/local/openresty/nginx/lua_files/?.lua;' .. package.path

local sock = assert(ngx.req.socket(true))


local resp = "{"

local backed_by = ngx.shared.mock_response:get("DALHERAMOCK_SERVERIP")
local port_specific_ip = ngx.shared.mock_response:get("PORT_DALHERAMOCK_SERVERIP")

resp = resp .. "\"DALHERAMOCK_SERVERIP\":\"" .. backed_by .. "\""
resp = resp .. ",\"PORT_DALHERAMOCK_SERVERIP\":\""  .. port_specific_ip .. "\""

if os.getenv("OCC_TO_MOCK") ~= nil then
resp = resp .. ",\"OCC_TO_MOCK\":\""  .. os.getenv("OCC_TO_MOCK") .. "\""
end

if os.getenv("nsvip") ~= nil then
resp = resp .. ",\"nsvip\":\"" .. os.getenv("nsvip") .. "\""
end

if os.getenv("pool") ~= nil then
resp = resp .. ",\"pool\":\"" .. os.getenv("pool") .. "\""
end



if os.getenv("BACKEDBY_ENV_NAME") ~= nil then
resp = resp .. ",\"BACKEDBY_ENV_NAME\":\"" .. os.getenv("BACKEDBY_ENV_NAME") .. "\""
end


resp = resp .. "}"

send_data(sock, "upstream_request", string.len(resp), resp);
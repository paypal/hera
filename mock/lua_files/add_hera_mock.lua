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

local function log_to_file(level, data)
    if (ngx.shared.mock_response:get("DISABLE_LOG") == nil) then
        local logdata = debug.getinfo(2).currentline .. ':' .. data
        while(#logdata > 0) do
            local temp = string.sub(logdata, 0, 4000)
            ngx.log(level, temp)
            logdata = string.sub(logdata, 4001)
        end
    end
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

local function split(s, delimiter)
    local result = {};
    for match in (s..delimiter):gmatch("(.-)"..delimiter) do
        table.insert(result, match);
    end
    return result;
end

local function starts_with(str, start)
    return str:sub(1, #start) == start
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

local function reload()
    ngx.sleep(1)
    os.execute("ps -eaf | grep 'nginx: worker process' | grep nobody | grep -v grep | awk '{print $2}' | xargs kill -9")
end

local function findLast(haystack, needle)
    --Set the third arg to false to allow pattern matching
    local found = haystack:reverse():find(needle:reverse(), nil, true)
    if found then
        return haystack:len() - needle:len() - found + 2
    else
        return found
    end
end

local ip = ngx.var.remote_addr

local sock = assert(ngx.req.socket(true))
local div = "heramockdiv"
local _, d = read_data(sock, "response");
local sp = findLast(d, div)
local _, t = read_data(sock, "expire_time_in_sec");
local et, _ = string.find(t, "=");
ngx.config.debug = true

local d1 = string.sub(d, 1, sp-1)
local d2 = string.sub(d, sp+11)
local t1 = string.sub(t, et+1)
d1 = d1:gsub("heraMockEqual", "=")
d2 = d2:gsub("heraMockEqual", "=")

local data = ""

if d1 == "DALHERAMOCK_SERVERIP" or d1 == "PORT_DALHERAMOCK_SERVERIP" then
    ngx.shared.mock_response:set(d1, d2);
    data = "setting mock server to " .. d2;
    local data_to_write = "";
    local fh, err = io.open("/usr/local/openresty/nginx/conf/server_ip.txt");
    if err then
        ngx.log(ngx.ERR, "unable to read the mock response");
        return;
    end
    while true do
        local line = fh:read();
        if line == nil then
            break;
        end
        local s, e = string.find(line, "=");
        if d1 ~= string.sub(line, 1, s-1) then
            data_to_write = data_to_write .. string.sub(line, 1, s-1) .. "=" .. string.sub(line, s+1) .. "\n";
        end
    end
    data_to_write = data_to_write .. d1 .. "=" .. d2
    local f1 = io.open("/usr/local/openresty/nginx/conf/server_ip.txt", "w+")
    f1:write(data_to_write)
    f1:close()
else
    if starts_with(d2, " DALHERAMOCK_NEW_SOCK ") then
        if t1 == "-1" then
            local ds = split(d2, " DALHERAMOCK_NEW_SOCK ")
            data = ""
            for i,v in pairs(ds) do
                if v ~= "" then
                    v = v:gsub(" DALHERAMOCK_NEW_SOCK ", "")
                    local k = d1 .. "_" .. i-1
                    data = data .. "Setting new Mock for " .. k .." as " .. v .. " with exp time as forever\n"
                    ngx.shared.mock_response:set(k, v);
                end
            end
        else
            local ds = split(d2, " DALHERAMOCK_NEW_SOCK ")
            for i,v in pairs(ds) do
                if v ~= "" then
                    v = v:gsub(" DALHERAMOCK_NEW_SOCK ", "")
                    local k = d1 .. "_" .. i-1
                    data = data .. "Setting new Mock for " .. k .. " as " .. v .. " with exp time as " .. t1 .. '\n'
                    ngx.shared.mock_response:set(k, v, tonumber(t1));
                end
            end
        end
    elseif starts_with(d2, "LOAD_BASED_MOCK ") then
        if t1 == "-1" then
            ngx.shared.load_based_mock:set(d1, d2)
            data = "Setting new LOAD_BASED_MOCK Mock for " .. d1 .. " as " .. d2 .. " with exp time as forever"
        else
            ngx.shared.load_based_mock:set(d1, d2, tonumber(t1))
            data = "Setting new Mock for " .. d1 .. " as " .. d2 .. " with exp time as " .. t1
        end
    else
        if t1 == "-1" then
            data = "Setting new Mock for " .. d1 .. " as " .. d2 .. " with exp time as forever"
            ngx.shared.mock_response:set(d1, d2);
        else
            data = "Setting new Mock for " .. d1 .. " as " .. d2 .. " with exp time as " .. t1
            ngx.shared.mock_response:set(d1, d2, tonumber(t1));
        end
    end
    log_to_file(ngx.INFO, data)
    ngx.shared.worker_sql_state:delete(d1)
    ngx.shared.random_failure_state:delete(d1)
    ngx.shared.random_response_store:delete(d1)
end

send_data(sock, "upstream_request", string.len("ok"), "ok");

if d1 == "DALHERAMOCK_SERVERIP" or d1 == "PORT_DALHERAMOCK_SERVERIP" then
    log_to_file(ngx.INFO, "reloading nginx")
    ngx.eof()
    if ngx.shared.active_conn_count ~= nil then
        local keys =  ngx.shared.active_conn_count:get_keys();
        for x, value in pairs(keys) do
            ngx.shared.active_conn_count:set(value, 0)
        end
    end
    ngx.thread.spawn(reload)
end
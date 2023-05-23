local M = {}
package.path = '/usr/local/openresty/nginx/lua_files/?.lua;' .. package.path

local nginx_utils = require("nginx_utils")
local file_utils = require("file_utils")
local string_utils = require("string_utils")
local netstring = require("netstring")

function M.update_mock_server_ip(key, value)
    local server_ip_file = "/usr/local/openresty/nginx/conf/server_ip.txt"
    -- update the mock with new server ip
    ngx.shared.mock_response:set(key, value);

    -- write the new server ip into the local file
    local existing_data = file_utils.read_file(server_ip_file)
    local data_to_write = existing_data .. key .. "=" .. value
    file_utils.write_file(server_ip_file, data_to_write)
end

function M.update_socket_based_mock(value, expiration_time_in_seconds, keyword)
    local data = ""

    local ds = string_utils.split(value, " DALHERAMOCK_NEW_SOCK ")
    for i,v in pairs(ds) do
        if v ~= "" then
            v = v:gsub(" DALHERAMOCK_NEW_SOCK ", "")
            local k = key .. "_" .. i-1
            ngx.shared.mock_response:set(k, v, expiration_time_in_seconds);
            data = data .. "Setting new Mock for " .. k .." as " .. v .. " with exp time as " .. keyword .. "\n"
        end
    end

    return data
end

function M.add_mock_tcp()

    -- open socket from client to read add mock request
    local sock = assert(ngx.req.socket(true))
    local div = "heramockdiv"

    -- read add mock request which has key, value and expiration time
    -- request will be like
    -- 1. KEY<heramockdiv>VALUE
    -- 2. expire_time_in_sec=<NumberOfSeconds>
    -- Example
    --AcctMap._PrimaryKeyLookup.-2heramockdivresponse_timeout
    -- expire_time_in_sec=120

    -- read key and value in request
    local _, d = netstring.read(sock, "add_mock_request");

    -- find the position of keyword "heramockdiv" in reverse (from last)
    local sp = string_utils.find_last(d, div)

    -- read expire_time_in_sec
    local _, t = netstring.read(sock, "add_mock_expire_time_in_sec");

    -- find position of "="
    local et, _ = string.find(t, "=");


    local key = string.sub(d, 1, sp-1)
    local value = string.sub(d, sp+string.len(div))
    local expiration_time_in_seconds = string.sub(t, et+1)
    local keyword = "forever"
    local restart_nginx = false

    if expiration_time_in_seconds == "-1" then
        expiration_time_in_seconds = 0
    else
        keyword = expiration_time_in_seconds
        expiration_time_in_seconds = tonumber(expiration_time_in_seconds)
    end

    key = string_utils.escape_special_chars(key)
    value = string_utils.escape_special_chars(value)
    local data = ""

    -- server ip is mocked they special keys
    -- note values can have special keys to tell there is no server present

    if key == "DALHERAMOCK_SERVERIP" or key == "PORT_DALHERAMOCK_SERVERIP" then
        M.update_mock_server_ip(key, value)
        data = "setting mock server to " .. value;
        restart_nginx = true
    elseif string_utils.starts_with(value, " DALHERAMOCK_NEW_SOCK ") then
        data = M.update_socket_based_mock(value, expiration_time_in_seconds, keyword)
    elseif string_utils.starts_with(value, "LOAD_BASED_MOCK ") then
        ngx.shared.load_based_mock:set(key, value, expiration_time_in_seconds)
        data = "Setting new LOAD_BASED_MOCK Mock for " .. key .. " as " .. value .. " with exp time as " .. keyword .. "\n"
    else
        data = "Setting new Mock for " .. key .. " as " .. value .. " with exp time as " .. keyword .. "\n"
        ngx.shared.mock_response:set(key, value, expiration_time_in_seconds);
    end
    file_utils.log_to_file(ngx.INFO, data)
    ngx.shared.worker_sql_state:delete(key)
    ngx.shared.random_failure_state:delete(key)
    ngx.shared.random_response_store:delete(key)

    if restart_nginx then
        file_utils.log_to_file(ngx.INFO, "reloading nginx")
        ngx.eof()
        if ngx.shared.active_conn_count ~= nil then
            local keys =  ngx.shared.active_conn_count:get_keys();
            for _, v in pairs(keys) do
                ngx.shared.active_conn_count:set(v, 0);
            end
        end
        ngx.thread.spawn(nginx_utils.reload_nginx);
    end

    netstring.send(sock, "add_mock", "ok")
end

return M
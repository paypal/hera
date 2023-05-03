local M = {}

function M.generate_hash_for_socket(o)
    if type(o) == 'table' then
        for _,v in pairs(o) do
            local resp = M.generate_hash_for_socket(v)
            if resp ~= nil and resp ~= "unknown" then
                return string.gsub(resp, "%s+", "")
            end
        end
    else
        if string.find(tostring(o), "userdata") then
            local resp = M.split(tostring(o), ":")
            return string.gsub(resp[2], "%s+", "")
        end
    end
    return "unknown"
end

function M.reload_nginx()
    ngx.sleep(1)
    os.execute("ps -eaf | grep 'nginx: worker process' | grep nobody | grep -v grep | awk '{print $2}' | xargs kill -9")
end

function M.say_message(output)
    local msg = "";
    if type(output) == 'table' then
        for key, val in pairs(output) do
            if msg ~= "" then
                msg = msg .. "\n"
            end
            msg = msg .. key .. ": " .. val
        end
    else
        msg = output
    end
    ngx.say(msg)
    ngx.eof()
end

function M.get_upstream_socket(ip, port)
    local up_sock = assert(ngx.socket.tcp());
    local up_ok, up_err = up_sock:connect(ip, port);
    if not up_ok then
        M.log_to_file(ngx.ERR, "upstream connection failure " .. up_err)
        return nil;
    end
    return up_sock;
end

return M
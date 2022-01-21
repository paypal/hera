local function reload()
    ngx.log(ngx.INFO, "Sleeping before killing the worker for reload " .. ngx.worker.pid())
    ngx.sleep(1)
    ngx.log(ngx.INFO, "Killing the worker now " .. ngx.worker.pid())
    os.execute("ps -eaf | grep 'nginx: worker process' | grep nobody | grep -v grep | awk '{print $2}' | xargs kill -9")
end

local ip = ngx.var.remote_addr
ngx.eof()
if ngx.shared.active_conn_count ~= nil then
    local keys =  ngx.shared.active_conn_count:get_keys();
    for x, value in pairs(keys) do
        ngx.shared.active_conn_count:set(value, 0)
        log_to_file(ngx.ERR, "load based match reset " .. value)

    end
end
ngx.thread.spawn(reload)


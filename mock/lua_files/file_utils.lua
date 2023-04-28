local M = {}

function M.read_file(file_name)
    local read_data = "";
    local fh, err = io.open(file_name);
    if err then
        utils.log_to_file(ngx.ERR, "unable to open server_ip file")
        return;
    end
    while true do
        local line = fh:read();
        if line == nil then
            break;
        end
        local s, _ = string.find(line, "=");
        if key ~= string.sub(line, 1, s-1) then
            read_data = read_data .. string.sub(line, 1, s-1) .. "=" .. string.sub(line, s+1) .. "\n";
        end
    end
    return read_data
end

function M.write_file(file_name, data_to_write)
    local f1 = io.open(file_name, "w+")
    f1:write(data_to_write)
    f1:close()
end

function M.log_to_file(level, data)
    local log_data = debug.getinfo(2).currentline .. ':' .. data
    while(#log_data > 0) do
        local temp = string.sub(log_data, 0, 4000)
        ngx.log(level, temp)
        log_data = string.sub(log_data, 4001)
    end
end

return M
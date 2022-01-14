local function capture(cmd)
    local f = assert(io.popen(cmd, 'r'))
    local s = assert(f:read('*a'))
    f:close()
    return s
end

local function trim(s)
    return (s:gsub("^%s*(.-)%s*$", "%1"))
end

package.path = '/usr/local/openresty/nginx/lua_files/?.lua;' .. package.path
local ilogger = require("ilogger")

if (ilogger ~= nill) then
    local host =  trim(capture("/sbin/ip route|awk '/src/ { print $9 }'"))
    local ns = os.getenv("namespace")
    if ns ~= nil then
        host=ns
    end
    local parent_host = os.getenv("HOST_HOSTNAME")
    if parent_host ~= nil then
        host=parent_host
    end

    local mocked=""
    if os.getenv("HERA_TO_MOCK") ~= nil then
        mocked=os.getenv("HERA_TO_MOCK")
    end
    if os.getenv("OCC_TO_MOCK") ~= nil then
        mocked=os.getenv("OCC_TO_MOCK")
    end

    ilogger.log(host, mocked)
end

ngx.say('ok')
ngx.eof()

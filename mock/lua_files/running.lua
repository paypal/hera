local function capture(cmd)
    local f = assert(io.popen(cmd, 'r'))
    local s = assert(f:read('*a'))
    f:close()
    return s
end

local function trim(s)
    return (s:gsub("^%s*(.-)%s*$", "%1"))
end

local function load()
    local data = ""
    local ip =  trim(capture("/sbin/ip route|awk '/src/ { print $9 }'"))
    data = "ip=" .. ip
    local ns = os.getenv("namespace")
    if ns ~= nil then
        data = data .. "&ns=" .. ns
    end
    local parent_host = os.getenv("HOST_HOSTNAME")
    if parent_host ~= nil then
        data = data .. "&parent_host=" .. parent_host
    end
    if os.getenv("nsvip") ~= nil then
        data = data .. "&nsvip=" .. os.getenv("nsvip")
    end
    if os.getenv("pool") ~= nil then
        data = data .. "&pool=" .. os.getenv("pool")
    end
    if os.getenv("BACKEDBY_ENV_NAME") ~= nil then
        data = data .. "&BACKEDBY_ENV_NAME=" .. os.getenv("BACKEDBY_ENV_NAME")
    end
    if os.getenv("HERA_TO_MOCK") ~= nil then
        data = data .. "&HERA_TO_MOCK=" .. os.getenv("HERA_TO_MOCK")
    end
    if os.getenv("OCC_TO_MOCK") ~= nil then
        data = data .. "&OCC_TO_MOCK=" .. os.getenv("OCC_TO_MOCK")
    end
    ngx.log(ngx.INFO, "HERAMock: " .. data)
    return data
end

local redis = require "resty.redis"
local red = redis:new()
red:set_timeouts(1000, 1000, 1000)
local ok, err = red:connect("127.0.0.1", 6379)
if not ok then
    log_to_file(ngx.DEBUG, "failed to connect to redis: " .. sock_id + " " + err)
end
local data = load()

red:set(tostring(ngx.var.msec)..":NA:NA:Command", data)
ngx.say('ok')
ngx.eof()


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
    local event_name =  trim(capture("/sbin/ip route|awk '/src/ { print $9 }'"))
    local mocking_hera = "Nil"
    local ns = os.getenv("namespace")
    if ns ~= nil then
        event_name = ns
    end
    local parent_host = os.getenv("HOST_HOSTNAME")
    if parent_host ~= nil then
        event_name = parent_host
    end
    local data = ""
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
        mocking_hera = os.getenv("HERA_TO_MOCK")
    end

end

load()
ngx.say('ok')
ngx.eof()
